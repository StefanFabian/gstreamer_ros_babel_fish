/*
 *  gstreamer_ros_babel_fish - GStreamer ROS interface.
 *  Copyright (C) 2026  Stefan Fabian
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published
 *  by the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
#include "gstreamer_ros_babel_fish/rbfimagesink.hpp"
#include "gstreamer_ros_babel_fish/format_conversion.hpp"
#include "gstreamer_ros_babel_fish/ros_node_interface.hpp"

#include <ament_index_cpp/get_package_share_directory.hpp>
#include <camera_calibration_parsers/parse.hpp>
#include <rcl/validate_topic_name.h>
#include <rclcpp/rclcpp.hpp>
#include <rmw/validate_node_name.h>
#include <sensor_msgs/msg/camera_info.hpp>
#include <sensor_msgs/msg/compressed_image.hpp>
#include <sensor_msgs/msg/image.hpp>
#include <sensor_msgs/srv/set_camera_info.hpp>

#include <chrono>
#include <memory>
#include <mutex>
#include <string>

using namespace gstreamer_ros_babel_fish;

GST_DEBUG_CATEGORY_STATIC( rbf_image_sink_debug );
#define GST_CAT_DEFAULT rbf_image_sink_debug

// Property IDs
enum {
  PROP_0,
  PROP_TOPIC,
  PROP_NODE,
  PROP_NODE_NAME,
  PROP_FRAME_ID,
  PROP_CAMERA_INFO_URL,
  PROP_PREFER_COMPRESSED,
  PROP_SUBSCRIPTION_COUNT,
  PROP_ENABLE_NV_FORMATS,
};

// Private data structure
struct _RbfImageSinkPrivate {
  // Properties
  gchar *topic;
  gchar *node_name;
  gchar *frame_id;
  gchar *camera_info_url;
  gpointer external_node; // rclcpp::Node*
  gboolean prefer_compressed;
  gboolean enable_nv_formats;

  // ROS interface
  std::unique_ptr<RosNodeInterface> ros_interface;
  rclcpp::Publisher<sensor_msgs::msg::Image>::SharedPtr image_pub;
  rclcpp::Publisher<sensor_msgs::msg::CompressedImage>::SharedPtr compressed_pub;
  rclcpp::Publisher<sensor_msgs::msg::CameraInfo>::SharedPtr camera_info_pub;
  rclcpp::Service<sensor_msgs::srv::SetCameraInfo>::SharedPtr set_camera_info_service;

  // State
  gboolean is_compressed;
  std::string compression_format;
  GstVideoInfo video_info;
  gboolean video_info_valid;
  uint32_t compressed_width;
  uint32_t compressed_height;
  gboolean camera_info_loaded;
  sensor_msgs::msg::CameraInfo camera_info;
  std::string camera_info_path;
  std::string camera_name;
  gboolean camera_info_resolution_mismatch;

  std::mutex mutex;
};

#define rbf_image_sink_parent_class parent_class
G_DEFINE_TYPE_WITH_PRIVATE( RbfImageSink, rbf_image_sink, GST_TYPE_BASE_SINK )

// Forward declarations
static void rbf_image_sink_set_property( GObject *object, guint prop_id, const GValue *value,
                                         GParamSpec *pspec );
static void rbf_image_sink_get_property( GObject *object, guint prop_id, GValue *value,
                                         GParamSpec *pspec );
static void rbf_image_sink_finalize( GObject *object );
static gboolean rbf_image_sink_start( GstBaseSink *sink );
static gboolean rbf_image_sink_stop( GstBaseSink *sink );
static gboolean rbf_image_sink_set_caps( GstBaseSink *sink, GstCaps *caps );
static GstFlowReturn rbf_image_sink_render( GstBaseSink *sink, GstBuffer *buffer );
static GstCaps *rbf_image_sink_get_caps( GstBaseSink *sink, GstCaps *filter );
static rclcpp::QoS make_default_qos();
static void rbf_image_sink_setup_camera_info( RbfImageSink *sink, rclcpp::Node::SharedPtr node );

static void rbf_image_sink_class_init( RbfImageSinkClass *klass )
{
  GObjectClass *gobject_class = G_OBJECT_CLASS( klass );
  GstElementClass *element_class = GST_ELEMENT_CLASS( klass );
  GstBaseSinkClass *basesink_class = GST_BASE_SINK_CLASS( klass );

  gobject_class->set_property = rbf_image_sink_set_property;
  gobject_class->get_property = rbf_image_sink_get_property;
  gobject_class->finalize = rbf_image_sink_finalize;

  // Install properties
  g_object_class_install_property(
      gobject_class, PROP_TOPIC,
      g_param_spec_string(
          "topic", "Topic",
          "Base ROS topic name (publishes to topic for raw, topic/compressed for compressed)",
          "/image", (GParamFlags)( G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS ) ) );

  g_object_class_install_property(
      gobject_class, PROP_NODE,
      g_param_spec_pointer(
          "node",
          "Node", "External ROS node (rclcpp::Node*). Needs to be a SharedPtr as shared_from_this will be used.",
          (GParamFlags)( G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS ) ) );

  g_object_class_install_property(
      gobject_class, PROP_NODE_NAME,
      g_param_spec_string(
          "node-name", "Node Name", "Name for internal ROS node (if no external node provided)",
          "rbfimagesink", (GParamFlags)( G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS ) ) );

  g_object_class_install_property(
      gobject_class, PROP_FRAME_ID,
      g_param_spec_string( "frame-id", "Frame ID", "frame_id for ROS header", "",
                           (GParamFlags)( G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS ) ) );

  g_object_class_install_property(
      gobject_class, PROP_CAMERA_INFO_URL,
      g_param_spec_string(
          "camera-info-url", "Camera Info URL",
          "file:// or package:// URL for camera calibration data. The camera info can be updated "
          "using set_camera_info service. Changes are written back to the given file if possible.",
          "", (GParamFlags)( G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS ) ) );

  g_object_class_install_property(
      gobject_class, PROP_PREFER_COMPRESSED,
      g_param_spec_boolean( "prefer-compressed", "Prefer Compressed",
                            "Prefer compressed formats during caps negotiation", TRUE,
                            (GParamFlags)( G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS ) ) );

  g_object_class_install_property(
      gobject_class, PROP_SUBSCRIPTION_COUNT,
      g_param_spec_int( "subscription-count", "Subscription Count",
                        "Number of subscribers to the image topic", 0, G_MAXINT, 0,
                        (GParamFlags)( G_PARAM_READABLE | G_PARAM_STATIC_STRINGS ) ) );

  g_object_class_install_property(
      gobject_class, PROP_ENABLE_NV_FORMATS,
      g_param_spec_boolean( "enable-nv-formats", "Enable NV formats", "Enable NV formats (NV21, NV24)",
                            FALSE, (GParamFlags)( G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS ) ) );

  // Set element metadata
  gst_element_class_set_static_metadata(
      element_class, "ROS Babel Fish Image Sink", "Sink/Video",
      "Publishes video frames to ROS 2 image topics",
      "Stefan Fabian <gstreamer_ros_babel_fish@stefanfabian.com>" );

  // Add sink pad template
  GstCaps *caps = get_all_supported_caps();
  GstPadTemplate *sink_template = gst_pad_template_new( "sink", GST_PAD_SINK, GST_PAD_ALWAYS, caps );
  gst_element_class_add_pad_template( element_class, sink_template );
  gst_caps_unref( caps );

  // Set base sink virtual methods
  basesink_class->start = GST_DEBUG_FUNCPTR( rbf_image_sink_start );
  basesink_class->stop = GST_DEBUG_FUNCPTR( rbf_image_sink_stop );
  basesink_class->set_caps = GST_DEBUG_FUNCPTR( rbf_image_sink_set_caps );
  basesink_class->render = GST_DEBUG_FUNCPTR( rbf_image_sink_render );
  basesink_class->get_caps = GST_DEBUG_FUNCPTR( rbf_image_sink_get_caps );

  GST_DEBUG_CATEGORY_INIT( rbf_image_sink_debug, "rbfimagesink", 0, "ROS Babel Fish Image Sink" );
}

static void rbf_image_sink_init( RbfImageSink *sink )
{
  sink->priv = (RbfImageSinkPrivate *)rbf_image_sink_get_instance_private( sink );
  new ( sink->priv ) RbfImageSinkPrivate();

  sink->priv->topic = g_strdup( "/image" );
  sink->priv->node_name = g_strdup( "rbfimagesink" );
  sink->priv->frame_id = g_strdup( "" );
  sink->priv->camera_info_url = g_strdup( "" );
  sink->priv->external_node = nullptr;
  sink->priv->prefer_compressed = TRUE;
  sink->priv->enable_nv_formats = FALSE;
  sink->priv->is_compressed = FALSE;
  sink->priv->video_info_valid = FALSE;
  sink->priv->compressed_width = 0;
  sink->priv->compressed_height = 0;
  sink->priv->camera_info_loaded = FALSE;
  sink->priv->camera_info_resolution_mismatch = FALSE;

  // Disable synchronization by default for real-time streaming
  gst_base_sink_set_sync( GST_BASE_SINK( sink ), FALSE );
}

static void rbf_image_sink_finalize( GObject *object )
{
  RbfImageSink *sink = RBF_IMAGE_SINK( object );

  g_free( sink->priv->topic );
  g_free( sink->priv->node_name );
  g_free( sink->priv->frame_id );
  g_free( sink->priv->camera_info_url );

  sink->priv->~RbfImageSinkPrivate();

  G_OBJECT_CLASS( parent_class )->finalize( object );
}

static void rbf_image_sink_set_property( GObject *object, guint prop_id, const GValue *value,
                                         GParamSpec *pspec )
{
  RbfImageSink *sink = RBF_IMAGE_SINK( object );
  std::lock_guard<std::mutex> lock( sink->priv->mutex );

  switch ( prop_id ) {
  case PROP_TOPIC: {
    const char *new_topic = g_value_get_string( value );
    int validation_result;
    size_t invalid_index;

    if ( rcl_validate_topic_name( new_topic, &validation_result, &invalid_index ) != RMW_RET_OK ) {
      GST_ERROR_OBJECT( sink, "Internal error validating topic: %s", new_topic );
      break;
    }

    if ( validation_result != RCL_TOPIC_NAME_VALID ) {
      const char *validation_message = rcl_topic_name_validation_result_string( validation_result );
      GST_ERROR_OBJECT( sink, "Invalid topic name '%s': %s (at index %zu)", new_topic,
                        validation_message, invalid_index );
      break;
    }

    g_free( sink->priv->topic );
    sink->priv->topic = g_strdup( new_topic );
    break;
  }
  case PROP_NODE:
    sink->priv->external_node = g_value_get_pointer( value );
    break;
  case PROP_NODE_NAME: {
    const char *new_node_name = g_value_get_string( value );
    int validation_result;
    size_t invalid_index;

    if ( rmw_validate_node_name( new_node_name, &validation_result, &invalid_index ) != RMW_RET_OK ) {
      GST_ERROR_OBJECT( sink, "Internal error validating node name: %s", new_node_name );
      break;
    }

    if ( validation_result != RMW_NODE_NAME_VALID ) {
      const char *validation_message = rmw_node_name_validation_result_string( validation_result );
      GST_ERROR_OBJECT( sink, "Invalid node name '%s': %s (at index %zu)", new_node_name,
                        validation_message, invalid_index );
      break;
    }

    g_free( sink->priv->node_name );
    sink->priv->node_name = g_strdup( new_node_name );
    break;
  }
  case PROP_FRAME_ID:
    g_free( sink->priv->frame_id );
    sink->priv->frame_id = g_value_dup_string( value );
    break;
  case PROP_CAMERA_INFO_URL:
    g_free( sink->priv->camera_info_url );
    sink->priv->camera_info_url = g_value_dup_string( value );
    break;
  case PROP_PREFER_COMPRESSED:
    sink->priv->prefer_compressed = g_value_get_boolean( value );
    break;
  case PROP_ENABLE_NV_FORMATS:
    sink->priv->enable_nv_formats = g_value_get_boolean( value );
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID( object, prop_id, pspec );
    break;
  }
}

static void rbf_image_sink_get_property( GObject *object, guint prop_id, GValue *value,
                                         GParamSpec *pspec )
{
  RbfImageSink *sink = RBF_IMAGE_SINK( object );
  std::lock_guard<std::mutex> lock( sink->priv->mutex );

  switch ( prop_id ) {
  case PROP_TOPIC:
    g_value_set_string( value, sink->priv->topic );
    break;
  case PROP_NODE:
    g_value_set_pointer( value, sink->priv->external_node );
    break;
  case PROP_NODE_NAME:
    g_value_set_string( value, sink->priv->node_name );
    break;
  case PROP_FRAME_ID:
    g_value_set_string( value, sink->priv->frame_id );
    break;
  case PROP_CAMERA_INFO_URL:
    g_value_set_string( value, sink->priv->camera_info_url );
    break;
  case PROP_PREFER_COMPRESSED:
    g_value_set_boolean( value, sink->priv->prefer_compressed );
    break;
  case PROP_ENABLE_NV_FORMATS:
    g_value_set_boolean( value, sink->priv->enable_nv_formats );
    break;
  case PROP_SUBSCRIPTION_COUNT: {
    int count = 0;
    if ( sink->priv->is_compressed && sink->priv->compressed_pub ) {
      count = sink->priv->compressed_pub->get_subscription_count();
    } else if ( sink->priv->image_pub ) {
      count = sink->priv->image_pub->get_subscription_count();
    }
    g_value_set_int( value, count );
    break;
  }
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID( object, prop_id, pspec );
    break;
  }
}

static gboolean rbf_image_sink_start( GstBaseSink *basesink )
{
  RbfImageSink *sink = RBF_IMAGE_SINK( basesink );
  std::lock_guard<std::mutex> lock( sink->priv->mutex );

  try {
    GST_DEBUG_OBJECT( sink, "Starting ROS interface" );

    // Create ROS interface
    sink->priv->ros_interface = std::make_unique<RosNodeInterface>();

    rclcpp::Node::SharedPtr external_node = nullptr;
    if ( sink->priv->external_node ) {
      auto *node_ptr = static_cast<rclcpp::Node *>( sink->priv->external_node );
      external_node = node_ptr->shared_from_this();
    }

    if ( !sink->priv->ros_interface->initialize( external_node, sink->priv->node_name ) ) {
      GST_ERROR_OBJECT( sink, "Failed to initialize ROS interface" );
      return FALSE;
    }

    auto node = sink->priv->ros_interface->get_node();
    if ( !node ) {
      GST_ERROR_OBJECT( sink, "No ROS node available" );
      return FALSE;
    }

    rbf_image_sink_setup_camera_info( sink, node );
    GST_DEBUG_OBJECT( sink, "ROS interface started successfully" );
    return TRUE;
  } catch ( const std::exception &e ) {
    GST_ERROR_OBJECT( sink, "Exception in start: %s", e.what() );
    return FALSE;
  } catch ( ... ) {
    GST_ERROR_OBJECT( sink, "Unknown exception in start" );
    return FALSE;
  }
}

static gboolean rbf_image_sink_stop( GstBaseSink *basesink )
{
  RbfImageSink *sink = RBF_IMAGE_SINK( basesink );
  std::lock_guard<std::mutex> lock( sink->priv->mutex );

  try {
    GST_DEBUG_OBJECT( sink, "Stopping ROS interface" );

    sink->priv->image_pub.reset();
    sink->priv->compressed_pub.reset();
    sink->priv->camera_info_pub.reset();
    sink->priv->set_camera_info_service.reset();
    sink->priv->camera_info_loaded = FALSE;
    sink->priv->camera_info_path.clear();
    sink->priv->camera_name.clear();

    if ( sink->priv->ros_interface ) {
      sink->priv->ros_interface->shutdown();
      sink->priv->ros_interface.reset();
    }

    sink->priv->video_info_valid = FALSE;
    sink->priv->is_compressed = FALSE;

    return TRUE;
  } catch ( const std::exception &e ) {
    GST_ERROR_OBJECT( sink, "Exception in stop: %s", e.what() );
    return FALSE;
  } catch ( ... ) {
    GST_ERROR_OBJECT( sink, "Unknown exception in stop" );
    return FALSE;
  }
}

static GstCaps *rbf_image_sink_get_caps( GstBaseSink *basesink, GstCaps *filter )
{
  RbfImageSink *sink = RBF_IMAGE_SINK( basesink );
  GstCaps *all_caps = get_all_supported_caps();

  // Reorder caps based on prefer_compressed preference
  GstCaps *caps = gst_caps_new_empty();
  guint n_caps = gst_caps_get_size( all_caps );

  if ( sink->priv->prefer_compressed ) {
    // Add compressed formats first, then raw
    for ( guint i = 0; i < n_caps; i++ ) {
      GstStructure *s = gst_caps_get_structure( all_caps, i );
      const gchar *name = gst_structure_get_name( s );
      if ( g_str_has_prefix( name, "image/" ) ) {
        gst_caps_append_structure( caps, gst_structure_copy( s ) );
      }
    }
    for ( guint i = 0; i < n_caps; i++ ) {
      GstStructure *s = gst_caps_get_structure( all_caps, i );
      const gchar *name = gst_structure_get_name( s );
      if ( g_str_has_prefix( name, "video/" ) ) {
        gst_caps_append_structure( caps, gst_structure_copy( s ) );
      }
    }
  } else {
    // Add raw formats first, then compressed
    for ( guint i = 0; i < n_caps; i++ ) {
      GstStructure *s = gst_caps_get_structure( all_caps, i );
      const gchar *name = gst_structure_get_name( s );
      if ( g_str_has_prefix( name, "video/" ) ) {
        gst_caps_append_structure( caps, gst_structure_copy( s ) );
      }
    }
    for ( guint i = 0; i < n_caps; i++ ) {
      GstStructure *s = gst_caps_get_structure( all_caps, i );
      const gchar *name = gst_structure_get_name( s );
      if ( g_str_has_prefix( name, "image/" ) ) {
        gst_caps_append_structure( caps, gst_structure_copy( s ) );
      }
    }
  }
  gst_caps_unref( all_caps );

  if ( filter ) {
    GstCaps *intersection = gst_caps_intersect_full( filter, caps, GST_CAPS_INTERSECT_FIRST );
    gst_caps_unref( caps );
    caps = intersection;
  }

  // Filter out NV formats if disabled
  if ( !sink->priv->enable_nv_formats ) {
    guint i = 0;
    while ( i < gst_caps_get_size( caps ) ) {
      GstStructure *s = gst_caps_get_structure( caps, i );
      const gchar *format = gst_structure_get_string( s, "format" );
      if ( format && ( g_strcmp0( format, "NV21" ) == 0 || g_strcmp0( format, "NV24" ) == 0 ) ) {
        gst_caps_remove_structure( caps, i );
      } else {
        i++;
      }
    }
  }

  return caps;
}

static std::string get_camera_info_topic( const gchar *image_topic )
{
  std::string topic = image_topic ? image_topic : "";
  size_t last_slash = topic.find_last_of( '/' );
  if ( last_slash == std::string::npos || last_slash == 0 ) {
    return last_slash == 0 ? "/camera_info" : "camera_info";
  }
  return topic.substr( 0, last_slash ) + "/camera_info";
}

static std::string get_camera_namespace( const gchar *image_topic )
{
  std::string topic = image_topic ? image_topic : "";
  size_t last_slash = topic.find_last_of( '/' );
  if ( last_slash == std::string::npos ) {
    return "";
  }
  if ( last_slash == 0 ) {
    return "/";
  }
  return topic.substr( 0, last_slash );
}

static std::string get_set_camera_info_service_topic( const gchar *image_topic )
{
  std::string camera_namespace = get_camera_namespace( image_topic );
  if ( camera_namespace.empty() ) {
    return "set_camera_info";
  }
  if ( camera_namespace == "/" ) {
    return "/set_camera_info";
  }
  return camera_namespace + "/set_camera_info";
}

static std::string get_default_camera_name( const gchar *image_topic )
{
  std::string camera_namespace = get_camera_namespace( image_topic );
  while ( !camera_namespace.empty() && camera_namespace.front() == '/' ) {
    camera_namespace.erase( camera_namespace.begin() );
  }
  while ( !camera_namespace.empty() && camera_namespace.back() == '/' ) {
    camera_namespace.pop_back();
  }
  for ( char &c : camera_namespace ) {
    if ( c == '/' ) {
      c = '_';
    }
  }
  return camera_namespace.empty() ? "camera" : camera_namespace;
}

static bool resolve_camera_info_url( const std::string &url, std::string &path,
                                     std::string &error_message )
{
  static const std::string file_prefix = "file://";
  static const std::string package_prefix = "package://";

  if ( url.rfind( file_prefix, 0 ) == 0 ) {
    path = url.substr( file_prefix.size() );
    if ( path.empty() || path.front() != '/' ) {
      error_message = "file:// camera-info-url must contain an absolute path";
      return false;
    }
    return true;
  }

  if ( url.rfind( package_prefix, 0 ) == 0 ) {
    std::string package_path = url.substr( package_prefix.size() );
    size_t slash = package_path.find( '/' );
    if ( slash == std::string::npos || slash == 0 || slash + 1 >= package_path.size() ) {
      error_message = "package:// camera-info-url must be package://package_name/path";
      return false;
    }

    std::string package_name = package_path.substr( 0, slash );
    std::string relative_path = package_path.substr( slash + 1 );
    try {
      path = ament_index_cpp::get_package_share_directory( package_name ) + "/" + relative_path;
      return true;
    } catch ( const std::exception &e ) {
      error_message = e.what();
      return false;
    }
  }

  error_message = "camera-info-url must use file:// or package://";
  return false;
}

static bool get_caps_dimensions( const GstCaps *caps, uint32_t &width, uint32_t &height )
{
  GstVideoInfo video_info;
  if ( gst_video_info_from_caps( &video_info, caps ) ) {
    width = GST_VIDEO_INFO_WIDTH( &video_info );
    height = GST_VIDEO_INFO_HEIGHT( &video_info );
    return width > 0 && height > 0;
  }

  gint caps_width = 0;
  gint caps_height = 0;
  GstStructure *structure = gst_caps_get_structure( caps, 0 );
  bool success = gst_structure_get_int( structure, "width", &caps_width ) &&
                 gst_structure_get_int( structure, "height", &caps_height ) && caps_width > 0 &&
                 caps_height > 0;
  if ( success ) {
    width = static_cast<uint32_t>( caps_width );
    height = static_cast<uint32_t>( caps_height );
  }
  return success;
}

static rclcpp::QoS make_default_qos()
{
  rclcpp::QoS qos( 1 );
  qos.reliable();
  qos.durability_volatile();
  return qos;
}

static void rbf_image_sink_setup_camera_info( RbfImageSink *sink, rclcpp::Node::SharedPtr node )
{
  sink->priv->camera_info_pub.reset();
  sink->priv->set_camera_info_service.reset();
  sink->priv->camera_info_resolution_mismatch = FALSE;

  if ( !sink->priv->camera_info_url || sink->priv->camera_info_url[0] == '\0' ) {
    sink->priv->camera_info_loaded = FALSE;
    sink->priv->camera_info_path.clear();
    sink->priv->camera_name.clear();
    return;
  }

  std::string error_message;
  std::string resolved_path;
  if ( !resolve_camera_info_url( sink->priv->camera_info_url, resolved_path, error_message ) ) {
    GST_ERROR_OBJECT( sink, "Invalid camera info URL '%s': %s; camera info publishing disabled",
                      sink->priv->camera_info_url, error_message.c_str() );
    sink->priv->camera_info_loaded = FALSE;
    sink->priv->camera_info_path.clear();
    sink->priv->camera_name.clear();
    return;
  }
  sink->priv->camera_info_path = resolved_path;

  std::string service_topic = get_set_camera_info_service_topic( sink->priv->topic );
  sink->priv->set_camera_info_service = node->create_service<sensor_msgs::srv::SetCameraInfo>(
      service_topic, [sink]( const std::shared_ptr<sensor_msgs::srv::SetCameraInfo::Request> request,
                             std::shared_ptr<sensor_msgs::srv::SetCameraInfo::Response> response ) {
        std::lock_guard<std::mutex> lock( sink->priv->mutex );
        const auto &new_info = request->camera_info;
        if ( new_info.width == 0 || new_info.height == 0 ) {
          response->success = false;
          response->status_message = "Camera info width and height must be nonzero";
          return;
        }

        sink->priv->camera_info = new_info;
        sink->priv->camera_info_loaded = TRUE;
        if ( sink->priv->camera_name.empty() ) {
          sink->priv->camera_name = get_default_camera_name( sink->priv->topic );
        }

        if ( camera_calibration_parsers::writeCalibration(
                 sink->priv->camera_info_path, sink->priv->camera_name, sink->priv->camera_info ) ) {
          response->success = true;
          response->status_message = "Camera info updated";
        } else {
          response->success = true;
          response->status_message =
              "Camera info updated in memory, but saving calibration file failed";
        }
        sink->priv->camera_info_resolution_mismatch = FALSE;
      } );
  GST_INFO_OBJECT( sink, "Created set_camera_info service on %s", service_topic.c_str() );

  if ( sink->priv->camera_info_loaded ) {
    std::string camera_info_topic = get_camera_info_topic( sink->priv->topic );
    sink->priv->camera_info_pub =
        node->create_publisher<sensor_msgs::msg::CameraInfo>( camera_info_topic, make_default_qos() );
    GST_INFO_OBJECT( sink, "Created camera info publisher on %s", camera_info_topic.c_str() );
    return;
  }

  if ( !camera_calibration_parsers::readCalibration(
           sink->priv->camera_info_path, sink->priv->camera_name, sink->priv->camera_info ) ) {
    sink->priv->camera_info_loaded = FALSE;
    GST_ERROR_OBJECT(
        sink, "Failed to load camera info from '%s'; camera info publishing is disabled until valid camera info is set via service.",
        sink->priv->camera_info_url );
    sink->priv->camera_name.clear();
    return;
  }
  GST_INFO_OBJECT( sink, "Loaded camera info from %s.", sink->priv->camera_info_url );
  sink->priv->camera_info_loaded = TRUE;

  std::string camera_info_topic = get_camera_info_topic( sink->priv->topic );
  sink->priv->camera_info_pub =
      node->create_publisher<sensor_msgs::msg::CameraInfo>( camera_info_topic, make_default_qos() );
  GST_INFO_OBJECT( sink, "Created camera info publisher on %s", camera_info_topic.c_str() );
}

static gboolean rbf_image_sink_set_caps( GstBaseSink *basesink, GstCaps *caps )
{
  RbfImageSink *sink = RBF_IMAGE_SINK( basesink );
  std::lock_guard<std::mutex> lock( sink->priv->mutex );

  try {
    gchar *caps_str = gst_caps_to_string( caps );
    GST_DEBUG_OBJECT( sink, "Setting caps: %s", caps_str );
    g_free( caps_str );

    if ( !sink->priv->ros_interface || !sink->priv->ros_interface->is_initialized() ) {
      GST_ERROR_OBJECT( sink, "ROS interface not initialized" );
      return FALSE;
    }

    auto node = sink->priv->ros_interface->get_node();
    if ( !node ) {
      GST_ERROR_OBJECT( sink, "No ROS node available" );
      return FALSE;
    }

    // Check if caps are for compressed image
    auto compression_format = caps_to_compression_format( caps );

    // QoS settings (RELIABLE as this leaves decision to subscribers)
    // Use queue depth of 1 to minimize latency
    rclcpp::QoS qos = make_default_qos();

    // Reset publishers
    sink->priv->image_pub.reset();
    sink->priv->compressed_pub.reset();
    sink->priv->compressed_width = 0;
    sink->priv->compressed_height = 0;

    if ( compression_format ) {
      // Compressed image - publish to topic/compressed
      sink->priv->is_compressed = TRUE;
      sink->priv->compression_format = *compression_format;
      sink->priv->video_info_valid = FALSE;
      if ( !get_caps_dimensions( caps, sink->priv->compressed_width, sink->priv->compressed_height ) ) {
        GST_WARNING_OBJECT( sink, "Compressed caps do not include width and height; camera info "
                                  "resolution will not be verifiable" );
      }

      std::string compressed_topic = std::string( sink->priv->topic ) + "/compressed";
      GST_INFO_OBJECT( sink, "Creating compressed publisher on %s (format: %s)",
                       compressed_topic.c_str(), compression_format->c_str() );

      sink->priv->compressed_pub =
          node->create_publisher<sensor_msgs::msg::CompressedImage>( compressed_topic, qos );
    } else {
      // Raw image
      sink->priv->is_compressed = FALSE;

      if ( !gst_video_info_from_caps( &sink->priv->video_info, caps ) ) {
        GST_ERROR_OBJECT( sink, "Failed to parse video info from caps" );
        return FALSE;
      }
      sink->priv->video_info_valid = TRUE;

      GST_INFO_OBJECT( sink, "Creating raw image publisher on %s", sink->priv->topic );

      sink->priv->image_pub =
          node->create_publisher<sensor_msgs::msg::Image>( sink->priv->topic, qos );
    }

    return TRUE;
  } catch ( const std::exception &e ) {
    GST_ERROR_OBJECT( sink, "Exception in set_caps: %s", e.what() );
    return FALSE;
  } catch ( ... ) {
    GST_ERROR_OBJECT( sink, "Unknown exception in set_caps" );
    return FALSE;
  }
}

static bool fill_image_msg( sensor_msgs::msg::Image &msg, RbfImageSink *sink,
                            const rclcpp::Time &stamp )
{
  msg.header.stamp = stamp;
  msg.header.frame_id = sink->priv->frame_id;
  msg.width = GST_VIDEO_INFO_WIDTH( &sink->priv->video_info );
  msg.height = GST_VIDEO_INFO_HEIGHT( &sink->priv->video_info );

  auto encoding = gst_format_to_ros_encoding( GST_VIDEO_INFO_FORMAT( &sink->priv->video_info ) );
  if ( !encoding ) {
    GST_ERROR_OBJECT( sink, "Unsupported video format" );
    return false;
  }
  msg.encoding = *encoding;

  msg.step = GST_VIDEO_INFO_PLANE_STRIDE( &sink->priv->video_info, 0 );
  msg.is_bigendian = ( G_BYTE_ORDER == G_BIG_ENDIAN );

  return true;
}

static void publish_camera_info( RbfImageSink *sink, rclcpp::Node::SharedPtr node,
                                 const rclcpp::Time &stamp, uint32_t stream_width,
                                 uint32_t stream_height )
{
  if ( !sink->priv->camera_info_loaded ) {
    return;
  }

  auto camera_info = sink->priv->camera_info;
  if ( camera_info.width != stream_width || camera_info.height != stream_height ) {
    sink->priv->camera_info_pub.reset();
    if ( !sink->priv->camera_info_resolution_mismatch ) {
      GST_ERROR_OBJECT( sink,
                        "Camera info resolution %ux%u from '%s' does not match stream "
                        "resolution %ux%u; camera info publishing disabled",
                        camera_info.width, camera_info.height, sink->priv->camera_info_url,
                        stream_width, stream_height );
    }
    sink->priv->camera_info_resolution_mismatch = TRUE;
    return;
  }

  sink->priv->camera_info_resolution_mismatch = FALSE;
  if ( !sink->priv->camera_info_pub ) {
    std::string camera_info_topic = get_camera_info_topic( sink->priv->topic );
    GST_INFO_OBJECT( sink, "Creating camera info publisher on %s", camera_info_topic.c_str() );
    sink->priv->camera_info_pub =
        node->create_publisher<sensor_msgs::msg::CameraInfo>( camera_info_topic, make_default_qos() );
  }

  camera_info.header.stamp = stamp;
  camera_info.header.frame_id = sink->priv->frame_id;
  sink->priv->camera_info_pub->publish( camera_info );
}

static void handle_missing_camera_info_dimensions( RbfImageSink *sink )
{
  if ( !sink->priv->camera_info_loaded ) {
    return;
  }

  sink->priv->camera_info_pub.reset();
  if ( !sink->priv->camera_info_resolution_mismatch ) {
    GST_ERROR_OBJECT( sink,
                      "Unable to verify camera info resolution for '%s': current caps do not "
                      "include width and height; camera info publishing disabled",
                      sink->priv->camera_info_url );
  }
  sink->priv->camera_info_resolution_mismatch = TRUE;
}

static GstFlowReturn rbf_image_sink_render( GstBaseSink *basesink, GstBuffer *buffer )
{
  RbfImageSink *sink = RBF_IMAGE_SINK( basesink );
  std::lock_guard<std::mutex> lock( sink->priv->mutex );

  if ( !sink->priv->ros_interface || !sink->priv->ros_interface->is_initialized() ) {
    GST_ERROR_OBJECT( sink, "ROS interface not initialized" );
    return GST_FLOW_ERROR;
  }

  auto node = sink->priv->ros_interface->get_node();
  if ( !node || !node->get_node_options().context()->is_valid() ) {
    GST_ELEMENT_ERROR( sink, RESOURCE, NOT_FOUND, ( "ROS shutdown detected" ), ( nullptr ) );
    return GST_FLOW_ERROR;
  }

  auto node_now = node->now();
  rclcpp::Time stamp;
  GstClockTime absolute_time = GST_CLOCK_TIME_NONE;

  // Check for unix reference timestamp meta first
  GstCaps *ts_caps = gst_caps_new_empty_simple( "timestamp/x-unix" );
  GstReferenceTimestampMeta *ts_meta = gst_buffer_get_reference_timestamp_meta( buffer, ts_caps );
  gst_caps_unref( ts_caps );

  if ( ts_meta && ts_meta->timestamp > 0 ) {
    absolute_time = ts_meta->timestamp;
    GST_DEBUG_OBJECT( sink, "Using unix reference timestamp meta: %" GST_TIME_FORMAT,
                      GST_TIME_ARGS( absolute_time ) );
  } else if ( GST_BUFFER_PTS_IS_VALID( buffer ) ) {
    GstClockTime base_time = gst_element_get_base_time( GST_ELEMENT( sink ) );
    GstClock *clock = gst_element_get_clock( GST_ELEMENT( sink ) );
    if ( clock ) {
      auto ros_base_time = node->get_clock()->now().nanoseconds();
      auto gst_now = gst_clock_get_time( clock );
      gst_object_unref( clock );

      if ( GST_CLOCK_TIME_IS_VALID( base_time ) ) {
        GstClockTime running_time = GST_BUFFER_PTS( buffer );
        GstClockTime monotonic_time = base_time + running_time;
        absolute_time = ros_base_time + ( monotonic_time - gst_now );

        GST_DEBUG_OBJECT( sink, "Reconstructed unix timestamp from PTS." );
      }
    }
  }

  try {
    if ( GST_CLOCK_TIME_IS_VALID( absolute_time ) ) {
      stamp = rclcpp::Time( static_cast<int64_t>( absolute_time ), RCL_SYSTEM_TIME );
    } else {
      // No timestamp available: use current wall clock time
      stamp = node_now;
      GST_DEBUG_OBJECT( sink, "No valid timestamp available, using current ROS time" );
    }

    if ( sink->priv->is_compressed && sink->priv->compressed_pub ) {
      auto msg = std::make_unique<sensor_msgs::msg::CompressedImage>();
      msg->header.stamp = stamp;
      msg->header.frame_id = sink->priv->frame_id;
      msg->format = sink->priv->compression_format;

      // Map buffer and copy data
      GstMapInfo map;
      if ( !gst_buffer_map( buffer, &map, GST_MAP_READ ) ) {
        GST_ERROR_OBJECT( sink, "Failed to map buffer" );
        return GST_FLOW_ERROR;
      }

      msg->data.assign( map.data, map.data + map.size );
      gst_buffer_unmap( buffer, &map );

      sink->priv->compressed_pub->publish( std::move( msg ) );
      if ( sink->priv->compressed_width > 0 && sink->priv->compressed_height > 0 ) {
        publish_camera_info( sink, node, stamp, sink->priv->compressed_width,
                             sink->priv->compressed_height );
      } else {
        handle_missing_camera_info_dimensions( sink );
      }

    } else if ( !sink->priv->is_compressed && sink->priv->image_pub && sink->priv->video_info_valid ) {
      if ( sink->priv->image_pub->can_loan_messages() ) {
        auto loaned_msg = sink->priv->image_pub->borrow_loaned_message();
        if ( !fill_image_msg( loaned_msg.get(), sink, stamp ) ) {
          return GST_FLOW_ERROR;
        }

        sensor_msgs::msg::Image &msg = loaned_msg.get();

        // Map buffer and copy data
        GstMapInfo map;
        if ( !gst_buffer_map( buffer, &map, GST_MAP_READ ) ) {
          GST_ERROR_OBJECT( sink, "Failed to map buffer" );
          return GST_FLOW_ERROR;
        }

        msg.data.resize( map.size );
        memcpy( msg.data.data(), map.data, map.size );
        gst_buffer_unmap( buffer, &map );

        uint32_t image_width = msg.width;
        uint32_t image_height = msg.height;
        sink->priv->image_pub->publish( std::move( loaned_msg ) );
        publish_camera_info( sink, node, stamp, image_width, image_height );
      } else {
        auto msg = std::make_unique<sensor_msgs::msg::Image>();
        if ( !fill_image_msg( *msg, sink, stamp ) ) {
          return GST_FLOW_ERROR;
        }

        // Map buffer and copy data
        GstMapInfo map;
        if ( !gst_buffer_map( buffer, &map, GST_MAP_READ ) ) {
          GST_ERROR_OBJECT( sink, "Failed to map buffer" );
          return GST_FLOW_ERROR;
        }

        msg->data.assign( map.data, map.data + map.size );
        gst_buffer_unmap( buffer, &map );

        uint32_t image_width = msg->width;
        uint32_t image_height = msg->height;
        sink->priv->image_pub->publish( std::move( msg ) );
        publish_camera_info( sink, node, stamp, image_width, image_height );
      }
    } else {
      GST_WARNING_OBJECT( sink, "No publisher available" );
      return GST_FLOW_ERROR;
    }
  } catch ( const std::exception &e ) {
    GST_ELEMENT_ERROR( sink, RESOURCE, WRITE, ( "Failed to publish ROS message" ),
                       ( "Exception: %s", e.what() ) );
    return GST_FLOW_ERROR;
  } catch ( ... ) {
    GST_ELEMENT_ERROR( sink, RESOURCE, WRITE, ( "Failed to publish ROS message" ),
                       ( "Unknown exception" ) );
    return GST_FLOW_ERROR;
  }

  return GST_FLOW_OK;
}

gboolean rbf_image_sink_plugin_init( GstPlugin *plugin )
{
  return gst_element_register( plugin, "rbfimagesink", GST_RANK_NONE, RBF_TYPE_IMAGE_SINK );
}
