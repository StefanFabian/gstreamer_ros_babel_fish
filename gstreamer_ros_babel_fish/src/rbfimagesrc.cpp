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
#include "gstreamer_ros_babel_fish/rbfimagesrc.hpp"
#include "gstreamer_ros_babel_fish/format_conversion.hpp"
#include "gstreamer_ros_babel_fish/ros_node_interface.hpp"

#include <rcl/validate_topic_name.h>
#include <rclcpp/rclcpp.hpp>
#include <rmw/validate_node_name.h>
#include <sensor_msgs/msg/compressed_image.hpp>
#include <sensor_msgs/msg/image.hpp>

#include <cmath>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <variant>

using namespace gstreamer_ros_babel_fish;

GST_DEBUG_CATEGORY_STATIC( rbf_image_src_debug );
#define GST_CAT_DEFAULT rbf_image_src_debug

// Property IDs
enum {
  PROP_0,
  PROP_TOPIC,
  PROP_NODE,
  PROP_NODE_NAME,
  PROP_QOS_RELIABILITY,
  PROP_QOS_DURABILITY,
  PROP_QOS_HISTORY_DEPTH,
  PROP_DETERMINE_FRAMERATE,
  PROP_WAIT_FRAME_COUNT,
  PROP_FRAMERATE,
};

// QoS reliability enum
enum RbfQosReliability {
  RBF_QOS_RELIABILITY_BEST_EFFORT = 0,
  RBF_QOS_RELIABILITY_RELIABLE = 1,
};

// QoS durability enum
enum RbfQosDurability {
  RBF_QOS_DURABILITY_VOLATILE = 0,
  RBF_QOS_DURABILITY_TRANSIENT_LOCAL = 1,
};

#define RBF_TYPE_QOS_RELIABILITY ( rbf_qos_reliability_get_type() )
static GType rbf_qos_reliability_get_type()
{
  static GType type = 0;
  if ( !type ) {
    static const GEnumValue values[] = {
        { RBF_QOS_RELIABILITY_BEST_EFFORT, "Best Effort", "best-effort" },
        { RBF_QOS_RELIABILITY_RELIABLE, "Reliable", "reliable" },
        { 0, nullptr, nullptr } };
    type = g_enum_register_static( "RbfQosReliability", values );
  }
  return type;
}

#define RBF_TYPE_QOS_DURABILITY ( rbf_qos_durability_get_type() )
static GType rbf_qos_durability_get_type()
{
  static GType type = 0;
  if ( !type ) {
    static const GEnumValue values[] = {
        { RBF_QOS_DURABILITY_VOLATILE, "Volatile", "volatile" },
        { RBF_QOS_DURABILITY_TRANSIENT_LOCAL, "Transient Local", "transient-local" },
        { 0, nullptr, nullptr } };
    type = g_enum_register_static( "RbfQosDurability", values );
  }
  return type;
}

// Message variant type
using RosMessage =
    std::variant<sensor_msgs::msg::Image::SharedPtr, sensor_msgs::msg::CompressedImage::SharedPtr>;

// Private data structure
struct _RbfImageSrcPrivate {
  // Properties
  gchar *topic;
  gchar *node_name;
  gpointer external_node; // rclcpp::Node*
  RbfQosReliability qos_reliability;
  RbfQosDurability qos_durability;
  gint qos_history_depth;

  // ROS interface
  std::unique_ptr<RosNodeInterface> ros_interface;
  rclcpp::Subscription<sensor_msgs::msg::Image>::SharedPtr image_sub;
  rclcpp::Subscription<sensor_msgs::msg::CompressedImage>::SharedPtr compressed_sub;

  // State
  gboolean is_compressed; // Detected from topic type
  GstCaps *current_caps;
  GstClockTime first_timestamp;

  // Message queue
  std::queue<RosMessage> message_queue;
  std::mutex queue_mutex;
  std::condition_variable queue_cond;
  gboolean flushing;

  std::mutex mutex;

  // Framerate configuration
  gboolean determine_framerate;
  gint wait_frame_count;
  gchar *framerate_property;

  // Runtime framerate state
  gint framerate_num;
  gint framerate_den;
  gboolean framerate_determined;
  std::vector<GstClockTime> frame_timestamps;
};

#define rbf_image_src_parent_class parent_class
G_DEFINE_TYPE_WITH_PRIVATE( RbfImageSrc, rbf_image_src, GST_TYPE_PUSH_SRC )

// Forward declarations
static void rbf_image_src_set_property( GObject *object, guint prop_id, const GValue *value,
                                        GParamSpec *pspec );
static void rbf_image_src_get_property( GObject *object, guint prop_id, GValue *value,
                                        GParamSpec *pspec );
static void rbf_image_src_finalize( GObject *object );
static gboolean rbf_image_src_start( GstBaseSrc *src );
static gboolean rbf_image_src_stop( GstBaseSrc *src );
static gboolean rbf_image_src_unlock( GstBaseSrc *src );
static gboolean rbf_image_src_unlock_stop( GstBaseSrc *src );
static GstFlowReturn rbf_image_src_create( GstPushSrc *src, GstBuffer **buffer );
static GstCaps *rbf_image_src_get_caps( GstBaseSrc *src, GstCaps *filter );
static gboolean rbf_image_src_query( GstBaseSrc *src, GstQuery *query );

static void rbf_image_src_class_init( RbfImageSrcClass *klass )
{
  GObjectClass *gobject_class = G_OBJECT_CLASS( klass );
  GstElementClass *element_class = GST_ELEMENT_CLASS( klass );
  GstBaseSrcClass *basesrc_class = GST_BASE_SRC_CLASS( klass );
  GstPushSrcClass *pushsrc_class = GST_PUSH_SRC_CLASS( klass );

  gobject_class->set_property = rbf_image_src_set_property;
  gobject_class->get_property = rbf_image_src_get_property;
  gobject_class->finalize = rbf_image_src_finalize;

  // Install properties
  g_object_class_install_property(
      gobject_class, PROP_TOPIC,
      g_param_spec_string( "topic", "Topic", "ROS topic name to subscribe to", "/image",
                           (GParamFlags)( G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS ) ) );

  g_object_class_install_property(
      gobject_class, PROP_NODE,
      g_param_spec_pointer(
          "node",
          "Node", "External ROS node (rclcpp::Node*). Needs to be a SharedPtr as shared_from_this will be used.",
          (GParamFlags)( G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS ) ) );

  g_object_class_install_property(
      gobject_class, PROP_NODE_NAME,
      g_param_spec_string( "node-name", "Node Name",
                           "Name for internal ROS node (if no external node provided)", "rbfimagesrc",
                           (GParamFlags)( G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS ) ) );

  g_object_class_install_property(
      gobject_class, PROP_QOS_RELIABILITY,
      g_param_spec_enum( "qos-reliability", "QoS Reliability", "QoS reliability setting",
                         RBF_TYPE_QOS_RELIABILITY, RBF_QOS_RELIABILITY_BEST_EFFORT,
                         (GParamFlags)( G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS ) ) );

  g_object_class_install_property(
      gobject_class, PROP_QOS_DURABILITY,
      g_param_spec_enum( "qos-durability", "QoS Durability", "QoS durability setting",
                         RBF_TYPE_QOS_DURABILITY, RBF_QOS_DURABILITY_VOLATILE,
                         (GParamFlags)( G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS ) ) );

  g_object_class_install_property(
      gobject_class, PROP_QOS_HISTORY_DEPTH,
      g_param_spec_int( "qos-history-depth", "QoS History Depth", "QoS history depth", 1, 1000, 1,
                        (GParamFlags)( G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS ) ) );

  g_object_class_install_property(
      gobject_class, PROP_DETERMINE_FRAMERATE,
      g_param_spec_boolean( "determine-framerate", "Determine Framerate",
                            "Whether to determine framerate from input topics", FALSE,
                            (GParamFlags)( G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS ) ) );

  g_object_class_install_property(
      gobject_class, PROP_WAIT_FRAME_COUNT,
      g_param_spec_int( "wait-frame-count", "Wait Frame Count",
                        "Number of frames to wait to determine framerate", 2, 1000, 5,
                        (GParamFlags)( G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS ) ) );

  g_object_class_install_property(
      gobject_class, PROP_FRAMERATE,
      g_param_spec_string( "framerate", "Framerate", "Manual framerate (e.g. 30/1)", nullptr,
                           (GParamFlags)( G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS ) ) );

  // Set element metadata
  gst_element_class_set_static_metadata(
      element_class, "ROS Babel Fish Image Source", "Source/Video",
      "Subscribes to ROS 2 image topics and outputs video frames",
      "Stefan Fabian <gstreamer_ros_babel_fish@stefanfabian.com>" );

  // Add src pad template
  GstCaps *caps = get_all_supported_caps();
  GstPadTemplate *src_template = gst_pad_template_new( "src", GST_PAD_SRC, GST_PAD_ALWAYS, caps );
  gst_element_class_add_pad_template( element_class, src_template );
  gst_caps_unref( caps );

  // Set base src virtual methods
  basesrc_class->start = GST_DEBUG_FUNCPTR( rbf_image_src_start );
  basesrc_class->stop = GST_DEBUG_FUNCPTR( rbf_image_src_stop );
  basesrc_class->unlock = GST_DEBUG_FUNCPTR( rbf_image_src_unlock );
  basesrc_class->unlock_stop = GST_DEBUG_FUNCPTR( rbf_image_src_unlock_stop );
  basesrc_class->get_caps = GST_DEBUG_FUNCPTR( rbf_image_src_get_caps );
  basesrc_class->query = GST_DEBUG_FUNCPTR( rbf_image_src_query );

  // Set push src virtual methods
  pushsrc_class->create = GST_DEBUG_FUNCPTR( rbf_image_src_create );

  GST_DEBUG_CATEGORY_INIT( rbf_image_src_debug, "rbfimagesrc", 0, "ROS Babel Fish Image Source" );
}

static void rbf_image_src_init( RbfImageSrc *src )
{
  src->priv = (RbfImageSrcPrivate *)rbf_image_src_get_instance_private( src );
  new ( src->priv ) RbfImageSrcPrivate();

  src->priv->topic = g_strdup( "/image" );
  src->priv->node_name = g_strdup( "rbfimagesrc" );
  src->priv->external_node = nullptr;
  src->priv->qos_reliability = RBF_QOS_RELIABILITY_BEST_EFFORT;
  src->priv->qos_durability = RBF_QOS_DURABILITY_VOLATILE;
  src->priv->qos_history_depth = 1;
  src->priv->is_compressed = FALSE;
  src->priv->current_caps = nullptr;
  src->priv->first_timestamp = GST_CLOCK_TIME_NONE;
  src->priv->flushing = FALSE;

  src->priv->determine_framerate = FALSE;
  src->priv->wait_frame_count = 5;
  src->priv->framerate_property = nullptr;
  src->priv->framerate_num = 0;
  src->priv->framerate_den = 1;
  src->priv->framerate_determined = FALSE;

  // Set as live source
  gst_base_src_set_live( GST_BASE_SRC( src ), TRUE );
  gst_base_src_set_format( GST_BASE_SRC( src ), GST_FORMAT_TIME );
}

static void rbf_image_src_finalize( GObject *object )
{
  RbfImageSrc *src = RBF_IMAGE_SRC( object );

  g_free( src->priv->topic );
  g_free( src->priv->node_name );
  g_free( src->priv->framerate_property );

  if ( src->priv->current_caps ) {
    gst_caps_unref( src->priv->current_caps );
  }

  src->priv->~RbfImageSrcPrivate();

  G_OBJECT_CLASS( parent_class )->finalize( object );
}

static void rbf_image_src_set_property( GObject *object, guint prop_id, const GValue *value,
                                        GParamSpec *pspec )
{
  RbfImageSrc *src = RBF_IMAGE_SRC( object );
  std::lock_guard<std::mutex> lock( src->priv->mutex );

  switch ( prop_id ) {
  case PROP_TOPIC: {
    const char *new_topic = g_value_get_string( value );
    int validation_result;
    size_t invalid_index;

    if ( rcl_validate_topic_name( new_topic, &validation_result, &invalid_index ) != RMW_RET_OK ) {
      GST_ERROR_OBJECT( src, "Internal error validating topic: %s", new_topic );
      break;
    }

    if ( validation_result != RCL_TOPIC_NAME_VALID ) {
      const char *validation_message = rcl_topic_name_validation_result_string( validation_result );
      GST_ERROR_OBJECT( src, "Invalid topic name '%s': %s (at index %zu)", new_topic,
                        validation_message, invalid_index );
      break;
    }

    g_free( src->priv->topic );
    src->priv->topic = g_strdup( new_topic );
    break;
  }
  case PROP_NODE:
    src->priv->external_node = g_value_get_pointer( value );
    break;
  case PROP_NODE_NAME: {
    const char *new_node_name = g_value_get_string( value );
    int validation_result;
    size_t invalid_index;

    if ( rmw_validate_node_name( new_node_name, &validation_result, &invalid_index ) != RMW_RET_OK ) {
      GST_ERROR_OBJECT( src, "Internal error validating node name: %s", new_node_name );
      break;
    }

    if ( validation_result != RMW_NODE_NAME_VALID ) {
      const char *validation_message = rmw_node_name_validation_result_string( validation_result );
      GST_ERROR_OBJECT( src, "Invalid node name '%s': %s (at index %zu)", new_node_name,
                        validation_message, invalid_index );
      break;
    }

    g_free( src->priv->node_name );
    src->priv->node_name = g_strdup( new_node_name );
    break;
  }
  case PROP_QOS_RELIABILITY:
    src->priv->qos_reliability = (RbfQosReliability)g_value_get_enum( value );
    break;
  case PROP_QOS_DURABILITY:
    src->priv->qos_durability = (RbfQosDurability)g_value_get_enum( value );
    break;
  case PROP_QOS_HISTORY_DEPTH:
    src->priv->qos_history_depth = g_value_get_int( value );
    break;
  case PROP_DETERMINE_FRAMERATE:
    src->priv->determine_framerate = g_value_get_boolean( value );
    break;
  case PROP_WAIT_FRAME_COUNT:
    src->priv->wait_frame_count = g_value_get_int( value );
    break;
  case PROP_FRAMERATE: {
    const gchar *str = g_value_get_string( value );
    g_free( src->priv->framerate_property );
    src->priv->framerate_property = g_strdup( str );
    if ( str ) {
      int n, d;
      if ( sscanf( str, "%d/%d", &n, &d ) == 2 ) {
        src->priv->framerate_num = n;
        src->priv->framerate_den = d;
        src->priv->framerate_determined = TRUE; // Manual override counts as determined
      } else {
        GST_WARNING_OBJECT( src, "Failed to parse framerate string '%s'", str );
      }
    } else {
      // Reset if cleared
      if ( !src->priv->determine_framerate ) {
        src->priv->framerate_num = 0;
        src->priv->framerate_den = 1;
        src->priv->framerate_determined = FALSE;
      }
    }
    break;
  }
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID( object, prop_id, pspec );
    break;
  }
}

static void rbf_image_src_get_property( GObject *object, guint prop_id, GValue *value,
                                        GParamSpec *pspec )
{
  RbfImageSrc *src = RBF_IMAGE_SRC( object );
  std::lock_guard<std::mutex> lock( src->priv->mutex );

  switch ( prop_id ) {
  case PROP_TOPIC:
    g_value_set_string( value, src->priv->topic );
    break;
  case PROP_NODE:
    g_value_set_pointer( value, src->priv->external_node );
    break;
  case PROP_NODE_NAME:
    g_value_set_string( value, src->priv->node_name );
    break;
  case PROP_QOS_RELIABILITY:
    g_value_set_enum( value, src->priv->qos_reliability );
    break;
  case PROP_QOS_DURABILITY:
    g_value_set_enum( value, src->priv->qos_durability );
    break;
  case PROP_QOS_HISTORY_DEPTH:
    g_value_set_int( value, src->priv->qos_history_depth );
    break;
  case PROP_DETERMINE_FRAMERATE:
    g_value_set_boolean( value, src->priv->determine_framerate );
    break;
  case PROP_WAIT_FRAME_COUNT:
    g_value_set_int( value, src->priv->wait_frame_count );
    break;
  case PROP_FRAMERATE:
    g_value_set_string( value, src->priv->framerate_property );
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID( object, prop_id, pspec );
    break;
  }
}

static bool detect_topic_type( RbfImageSrc *src )
{
  auto node = src->priv->ros_interface->get_node();
  if ( !node ) {
    return false;
  }

  std::string topic = src->priv->topic;

  // Query the graph for topic types on the exact topic specified
  auto topic_names_and_types = node->get_topic_names_and_types();

  for ( const auto &topic_info : topic_names_and_types ) {
    if ( topic_info.first == topic ) {
      for ( const auto &type : topic_info.second ) {
        if ( type == "sensor_msgs/msg/Image" ) {
          src->priv->is_compressed = FALSE;
          GST_INFO_OBJECT( src, "Detected raw image topic: %s", topic.c_str() );
          return true;
        } else if ( type == "sensor_msgs/msg/CompressedImage" ) {
          src->priv->is_compressed = TRUE;
          GST_INFO_OBJECT( src, "Detected compressed image topic: %s", topic.c_str() );
          return true;
        }
      }
    }
  }

  return false;
}

static void on_image_received( RbfImageSrc *src, sensor_msgs::msg::Image::SharedPtr msg )
{
  std::lock_guard<std::mutex> lock( src->priv->queue_mutex );

  if ( src->priv->flushing ) {
    return;
  }

  // Limit queue size to avoid memory issues
  while ( src->priv->message_queue.size() >= 5 ) { src->priv->message_queue.pop(); }

  src->priv->message_queue.push( msg );
  src->priv->queue_cond.notify_one();
}

static void on_compressed_received( RbfImageSrc *src,
                                    sensor_msgs::msg::CompressedImage::SharedPtr msg )
{
  std::lock_guard<std::mutex> lock( src->priv->queue_mutex );

  if ( src->priv->flushing ) {
    return;
  }

  // Limit queue size
  while ( src->priv->message_queue.size() >= 5 ) { src->priv->message_queue.pop(); }

  src->priv->message_queue.push( msg );
  src->priv->queue_cond.notify_one();
}

static bool setup_subscription( RbfImageSrc *src )
{
  // Set up QoS
  rclcpp::QoS qos( src->priv->qos_history_depth );
  if ( src->priv->qos_reliability == RBF_QOS_RELIABILITY_RELIABLE ) {
    qos.reliable();
  } else {
    qos.best_effort();
  }
  if ( src->priv->qos_durability == RBF_QOS_DURABILITY_TRANSIENT_LOCAL ) {
    qos.transient_local();
  } else {
    qos.durability_volatile();
  }

  auto node = src->priv->ros_interface->get_node();

  if ( src->priv->is_compressed ) {
    // Subscribe to compressed topic
    GST_INFO_OBJECT( src, "Subscribing to compressed image topic: %s", src->priv->topic );

    src->priv->compressed_sub = node->create_subscription<sensor_msgs::msg::CompressedImage>(
        src->priv->topic, qos, [src]( sensor_msgs::msg::CompressedImage::SharedPtr msg ) {
          on_compressed_received( src, msg );
        } );
  } else {
    // Subscribe to raw topic
    GST_INFO_OBJECT( src, "Subscribing to raw image topic: %s", src->priv->topic );

    src->priv->image_sub = node->create_subscription<sensor_msgs::msg::Image>(
        src->priv->topic, qos,
        [src]( sensor_msgs::msg::Image::SharedPtr msg ) { on_image_received( src, msg ); } );
  }
  return true;
}

static gboolean rbf_image_src_start( GstBaseSrc *basesrc )
{
  RbfImageSrc *src = RBF_IMAGE_SRC( basesrc );
  std::lock_guard<std::mutex> lock( src->priv->mutex );

  try {
    GST_DEBUG_OBJECT( src, "Starting ROS interface" );

    src->priv->flushing = FALSE;
    src->priv->is_compressed = FALSE;
    src->priv->first_timestamp = GST_CLOCK_TIME_NONE;
    src->priv->frame_timestamps.clear();

    if ( src->priv->framerate_property ) {
      // Already parsed in set_property
      src->priv->framerate_determined = TRUE;
    } else {
      src->priv->framerate_num = 0;
      src->priv->framerate_den = 1;
      src->priv->framerate_determined = FALSE;
    }

    // Create ROS interface
    src->priv->ros_interface = std::make_unique<RosNodeInterface>();

    rclcpp::Node::SharedPtr external_node = nullptr;
    if ( src->priv->external_node ) {
      auto *node_ptr = static_cast<rclcpp::Node *>( src->priv->external_node );
      external_node = node_ptr->shared_from_this();
    }

    if ( !src->priv->ros_interface->initialize( external_node, src->priv->node_name ) ) {
      GST_ERROR_OBJECT( src, "Failed to initialize ROS interface" );
      return FALSE;
    }

    // Try to detect topic immediately
    if ( detect_topic_type( src ) ) {
      setup_subscription( src );
    } else {
      GST_INFO_OBJECT( src, "Topic %s not found yet, will wait for it in create loop",
                       src->priv->topic );
    }

    GST_DEBUG_OBJECT( src, "ROS interface started successfully" );
    return TRUE;
  } catch ( const std::exception &e ) {
    GST_ERROR_OBJECT( src, "Exception in start: %s", e.what() );
    return FALSE;
  } catch ( ... ) {
    GST_ERROR_OBJECT( src, "Unknown exception in start" );
    return FALSE;
  }
}

static gboolean rbf_image_src_stop( GstBaseSrc *basesrc )
{
  RbfImageSrc *src = RBF_IMAGE_SRC( basesrc );

  {
    std::lock_guard<std::mutex> lock( src->priv->queue_mutex );
    src->priv->flushing = TRUE;
    src->priv->queue_cond.notify_all();
  }

  std::lock_guard<std::mutex> lock( src->priv->mutex );

  try {
    GST_DEBUG_OBJECT( src, "Stopping ROS interface" );

    src->priv->image_sub.reset();
    src->priv->compressed_sub.reset();

    if ( src->priv->ros_interface ) {
      src->priv->ros_interface->shutdown();
      src->priv->ros_interface.reset();
    }

    // Clear queue
    {
      std::lock_guard<std::mutex> qlock( src->priv->queue_mutex );
      while ( !src->priv->message_queue.empty() ) { src->priv->message_queue.pop(); }
    }

    if ( src->priv->current_caps ) {
      gst_caps_unref( src->priv->current_caps );
      src->priv->current_caps = nullptr;
    }

    return TRUE;
  } catch ( const std::exception &e ) {
    GST_ERROR_OBJECT( src, "Exception in stop: %s", e.what() );
    return FALSE; // But stopping usually shouldn't fail
  } catch ( ... ) {
    GST_ERROR_OBJECT( src, "Unknown exception in stop" );
    return FALSE;
  }
}

static gboolean rbf_image_src_unlock( GstBaseSrc *basesrc )
{
  RbfImageSrc *src = RBF_IMAGE_SRC( basesrc );

  std::lock_guard<std::mutex> lock( src->priv->queue_mutex );
  src->priv->flushing = TRUE;
  src->priv->queue_cond.notify_all();

  return TRUE;
}

static gboolean rbf_image_src_unlock_stop( GstBaseSrc *basesrc )
{
  RbfImageSrc *src = RBF_IMAGE_SRC( basesrc );

  std::lock_guard<std::mutex> lock( src->priv->queue_mutex );
  src->priv->flushing = FALSE;

  return TRUE;
}

static GstCaps *rbf_image_src_get_caps( GstBaseSrc *basesrc, GstCaps *filter )
{
  RbfImageSrc *src = RBF_IMAGE_SRC( basesrc );
  GstCaps *caps;

  {
    std::lock_guard<std::mutex> lock( src->priv->mutex );
    if ( src->priv->current_caps ) {
      caps = gst_caps_ref( src->priv->current_caps );
    } else {
      caps = get_all_supported_caps();
    }
  }

  if ( filter ) {
    GstCaps *intersection = gst_caps_intersect_full( filter, caps, GST_CAPS_INTERSECT_FIRST );
    gst_caps_unref( caps );
    caps = intersection;
  }

  return caps;
}

static gboolean rbf_image_src_query( GstBaseSrc *basesrc, GstQuery *query )
{
  gboolean ret;

  switch ( GST_QUERY_TYPE( query ) ) {
  case GST_QUERY_LATENCY:
    // We're a live source with variable latency
    gst_query_set_latency( query, TRUE, 0, GST_CLOCK_TIME_NONE );
    ret = TRUE;
    break;
  default:
    ret = GST_BASE_SRC_CLASS( parent_class )->query( basesrc, query );
    break;
  }

  return ret;
}

static GstFlowReturn rbf_image_src_wait_for_message( RbfImageSrc *src, RosMessage &msg );

static GstFlowReturn rbf_image_src_determine_framerate_from_input( RbfImageSrc *src, RosMessage &msg )
{
  while ( src->priv->frame_timestamps.size() < (size_t)src->priv->wait_frame_count ) {
    GstFlowReturn ret = rbf_image_src_wait_for_message( src, msg );
    if ( ret != GST_FLOW_OK ) {
      return ret;
    }

    // Extract timestamp
    GstClockTime current_ts = GST_CLOCK_TIME_NONE;
    if ( std::holds_alternative<sensor_msgs::msg::Image::SharedPtr>( msg ) ) {
      auto image = std::get<sensor_msgs::msg::Image::SharedPtr>( msg );
      current_ts = rclcpp::Time( image->header.stamp ).nanoseconds();
    } else if ( std::holds_alternative<sensor_msgs::msg::CompressedImage::SharedPtr>( msg ) ) {
      auto compressed = std::get<sensor_msgs::msg::CompressedImage::SharedPtr>( msg );
      current_ts = rclcpp::Time( compressed->header.stamp ).nanoseconds();
    }

    if ( GST_CLOCK_TIME_IS_VALID( current_ts ) ) {
      src->priv->frame_timestamps.push_back( current_ts );
      GST_DEBUG_OBJECT( src, "Collecting frame for framerate determination (%zu/%d)",
                        src->priv->frame_timestamps.size(), src->priv->wait_frame_count );
    }
  }

  // Calculate framerate
  GstClockTime first = src->priv->frame_timestamps.front();
  GstClockTime last = src->priv->frame_timestamps.back();

  if ( last > first && src->priv->frame_timestamps.size() > 1 ) {
    double duration_ns = (double)( last - first );
    double avg_interval = duration_ns / ( src->priv->frame_timestamps.size() - 1 );
    double fps = 1e9 / avg_interval;

    int num = 1;
    int den = 1;
    if ( fps >= 1.0 ) {
      num = (gint)std::lround( fps );
    } else {
      // For very low framerates, use a fractional representation to avoid rounding to 0 fps
      den = (gint)std::lround( 1.0 / fps );
    }

    src->priv->framerate_num = num;
    src->priv->framerate_den = den;
    src->priv->framerate_determined = TRUE;
    GST_INFO_OBJECT( src, "Determined framerate: %d/%d (measured: %.4f fps)", num, den, fps );
  } else {
    GST_WARNING_OBJECT( src,
                        "Could not determine framerate (invalid timestamps), defaulting to 0/1" );
    src->priv->framerate_num = 0;
    src->priv->framerate_den = 1;
    src->priv->framerate_determined = TRUE; // Stop trying
  }
  // Anchor first_timestamp so that the last collected frame (the one pushed
  // downstream) gets a PTS equal to the current pipeline running time.
  // Without this, PTS=0 while running_time has advanced by the collection
  // duration, causing sync=true sinks to drop every buffer as late.
  //
  // We compute: first_timestamp = ros_ts_last - running_time
  // so that PTS = ros_ts_last - first_timestamp = running_time.
  // Subsequent frames naturally track: PTS_i = running_time + (ros_ts_i - ros_ts_last).
  if ( !GST_CLOCK_TIME_IS_VALID( src->priv->first_timestamp ) &&
       !src->priv->frame_timestamps.empty() ) {
    GstClock *clock = gst_element_get_clock( GST_ELEMENT( src ) );
    if ( clock ) {
      GstClockTime now = gst_clock_get_time( clock );
      GstClockTime base_time = gst_element_get_base_time( GST_ELEMENT( src ) );
      gst_object_unref( clock );
      if ( now >= base_time ) {
        GstClockTime running_time = now - base_time;
        src->priv->first_timestamp = src->priv->frame_timestamps.back() - running_time;
      } else {
        // Fallback: use the first collected timestamp (PTS won't be perfectly aligned
        // but avoids underflow).
        src->priv->first_timestamp = src->priv->frame_timestamps.front();
      }
    } else {
      // No clock available (shouldn't happen for a live source in PLAYING), fall back.
      src->priv->first_timestamp = src->priv->frame_timestamps.front();
    }
  }
  src->priv->frame_timestamps.clear();

  return GST_FLOW_OK;
}

static GstFlowReturn rbf_image_src_wait_for_message( RbfImageSrc *src, RosMessage &msg )
{
  std::unique_lock<std::mutex> lock( src->priv->queue_mutex );

  while ( src->priv->message_queue.empty() && !src->priv->flushing ) {
    auto node = src->priv->ros_interface->get_node();
    if ( !node || !node->get_node_options().context()->is_valid() ) {
      GST_ELEMENT_ERROR( src, RESOURCE, NOT_FOUND, ( "ROS shutdown detected" ), ( nullptr ) );
      return GST_FLOW_ERROR;
    }
    src->priv->queue_cond.wait_for( lock, std::chrono::milliseconds( 50 ) );
  }

  if ( src->priv->flushing ) {
    return GST_FLOW_FLUSHING;
  }

  msg = src->priv->message_queue.front();
  src->priv->message_queue.pop();

  return GST_FLOW_OK;
}

static GstFlowReturn rbf_image_src_create( GstPushSrc *pushsrc, GstBuffer **buffer )
{
  RbfImageSrc *src = RBF_IMAGE_SRC( pushsrc );
  RosMessage msg;

  try {
    // Check if we need to subscribe
    {
      std::unique_lock<std::mutex> lock( src->priv->mutex );

      if ( !src->priv->image_sub && !src->priv->compressed_sub ) {
        // Not subscribed yet, try to detect topic
        while ( !src->priv->image_sub && !src->priv->compressed_sub ) {
          {
            std::lock_guard<std::mutex> qlock( src->priv->queue_mutex );
            if ( src->priv->flushing ) {
              return GST_FLOW_FLUSHING;
            }
          }

          auto node = src->priv->ros_interface->get_node();
          if ( !node || !node->get_node_options().context()->is_valid() ) {
            GST_ELEMENT_ERROR( src, RESOURCE, NOT_FOUND, ( "ROS shutdown detected" ), ( nullptr ) );
            return GST_FLOW_ERROR;
          }

          bool detected = false;
          try {
            detected = detect_topic_type( src );
          } catch ( const std::exception &e ) {
            GST_WARNING_OBJECT( src, "Exception during topic detection: %s", e.what() );
          } catch ( ... ) {
            GST_WARNING_OBJECT( src, "Unknown exception during topic detection" );
          }

          if ( detected ) {
            setup_subscription( src );
            break;
          }

          // Wait a bit before retrying
          lock.unlock();
          {
            std::unique_lock<std::mutex> qlock( src->priv->queue_mutex );
            if ( src->priv->flushing )
              return GST_FLOW_FLUSHING;
            src->priv->queue_cond.wait_for( qlock, std::chrono::milliseconds( 100 ) );
            if ( src->priv->flushing )
              return GST_FLOW_FLUSHING;
          }
          lock.lock();
        }
      }
    }

    // Determine framerate logic
    bool have_msg = false;
    if ( src->priv->determine_framerate && !src->priv->framerate_determined ) {
      GstFlowReturn ret = rbf_image_src_determine_framerate_from_input( src, msg );
      if ( ret != GST_FLOW_OK ) {
        return ret;
      }
      have_msg = true;
    }

    // Wait for a message (normal operation)
    if ( !have_msg ) {
      GstFlowReturn ret = rbf_image_src_wait_for_message( src, msg );
      if ( ret != GST_FLOW_OK ) {
        return ret;
      }
    }

    // Process the message
    if ( std::holds_alternative<sensor_msgs::msg::Image::SharedPtr>( msg ) ) {
      auto image = std::get<sensor_msgs::msg::Image::SharedPtr>( msg );

      // Create caps for this image
      GstCaps *new_caps =
          create_caps_from_ros_image( image->encoding, image->width, image->height, image->step,
                                      src->priv->framerate_num, src->priv->framerate_den );

      if ( !new_caps ) {
        GST_ERROR_OBJECT( src, "Unsupported encoding: %s", image->encoding.c_str() );
        return GST_FLOW_NOT_NEGOTIATED;
      }

      // Check if caps changed
      {
        std::lock_guard<std::mutex> lock( src->priv->mutex );

        bool caps_changed =
            !src->priv->current_caps || !gst_caps_is_equal( src->priv->current_caps, new_caps );

        if ( caps_changed ) {
          GST_INFO_OBJECT( src, "Caps changed, reconfiguring" );

          if ( src->priv->current_caps ) {
            gst_caps_unref( src->priv->current_caps );
          }
          src->priv->current_caps = gst_caps_ref( new_caps );

          // Set caps on pad
          if ( !gst_base_src_set_caps( GST_BASE_SRC( src ), new_caps ) ) {
            GST_ERROR_OBJECT( src, "Failed to set caps" );
            gst_caps_unref( new_caps );
            return GST_FLOW_NOT_NEGOTIATED;
          }
        }
      }
      gst_caps_unref( new_caps );

      // Create buffer wrapping ROS message memory
      // We allocate a shared_ptr on the heap to keep the message alive
      auto *msg_ptr = new sensor_msgs::msg::Image::SharedPtr( image );

      *buffer = gst_buffer_new_wrapped_full(
          (GstMemoryFlags)0, (gpointer)image->data.data(), image->data.size(), 0,
          image->data.size(), msg_ptr,
          []( gpointer data ) { delete static_cast<sensor_msgs::msg::Image::SharedPtr *>( data ); } );

      if ( !*buffer ) {
        GST_ERROR_OBJECT( src, "Failed to allocate buffer" );
        delete msg_ptr;
        return GST_FLOW_ERROR;
      }

      // Set timestamp
      GstClockTime timestamp = rclcpp::Time( image->header.stamp ).nanoseconds();

      // Attach unix timestamp as reference timestamp meta
      GstCaps *ts_caps = gst_caps_new_empty_simple( "timestamp/x-unix" );
      gst_buffer_add_reference_timestamp_meta( *buffer, ts_caps, timestamp, GST_CLOCK_TIME_NONE );
      gst_caps_unref( ts_caps );

      // Calculate PTS starting from 0 based on first received timestamp
      if ( !GST_CLOCK_TIME_IS_VALID( src->priv->first_timestamp ) ) {
        src->priv->first_timestamp = timestamp;
      }

      if ( timestamp >= src->priv->first_timestamp ) {
        GstClockTime offset_time = timestamp - src->priv->first_timestamp;
        GST_BUFFER_PTS( *buffer ) = offset_time;
        GST_BUFFER_DTS( *buffer ) = offset_time;
      } else {
        // Timestamp is older than first timestamp (shouldn't happen with live source unless clock jumps back)
        GST_WARNING_OBJECT(
            src, "Timestamp %" GST_TIME_FORMAT " is older than first timestamp %" GST_TIME_FORMAT,
            GST_TIME_ARGS( timestamp ), GST_TIME_ARGS( src->priv->first_timestamp ) );
        GST_BUFFER_PTS( *buffer ) = 0;
        GST_BUFFER_DTS( *buffer ) = 0;
      }
    } else if ( std::holds_alternative<sensor_msgs::msg::CompressedImage::SharedPtr>( msg ) ) {
      auto compressed = std::get<sensor_msgs::msg::CompressedImage::SharedPtr>( msg );

      // Create caps for this image
      GstCaps *new_caps = create_caps_for_compressed( compressed->format, src->priv->framerate_num,
                                                      src->priv->framerate_den );

      if ( !new_caps ) {
        GST_ERROR_OBJECT( src, "Unsupported compressed format: %s", compressed->format.c_str() );
        return GST_FLOW_NOT_NEGOTIATED;
      }

      // Check if caps changed
      {
        std::lock_guard<std::mutex> lock( src->priv->mutex );

        bool caps_changed =
            !src->priv->current_caps || !gst_caps_is_equal( src->priv->current_caps, new_caps );

        if ( caps_changed ) {
          GST_INFO_OBJECT( src, "Caps changed, reconfiguring" );

          if ( src->priv->current_caps ) {
            gst_caps_unref( src->priv->current_caps );
          }
          src->priv->current_caps = gst_caps_ref( new_caps );

          // Set caps on pad
          if ( !gst_base_src_set_caps( GST_BASE_SRC( src ), new_caps ) ) {
            GST_ERROR_OBJECT( src, "Failed to set caps" );
            gst_caps_unref( new_caps );
            return GST_FLOW_NOT_NEGOTIATED;
          }
        }
      }
      gst_caps_unref( new_caps );

      // Create buffer wrapping ROS message memory
      auto *msg_ptr = new sensor_msgs::msg::CompressedImage::SharedPtr( compressed );

      *buffer = gst_buffer_new_wrapped_full(
          (GstMemoryFlags)0, (gpointer)compressed->data.data(), compressed->data.size(), 0,
          compressed->data.size(), msg_ptr, []( gpointer data ) {
            delete static_cast<sensor_msgs::msg::CompressedImage::SharedPtr *>( data );
          } );

      if ( !*buffer ) {
        GST_ERROR_OBJECT( src, "Failed to allocate buffer" );
        delete msg_ptr;
        return GST_FLOW_ERROR;
      }

      // Set timestamp
      GstClockTime timestamp = rclcpp::Time( compressed->header.stamp ).nanoseconds();

      // Attach unix timestamp as reference timestamp meta
      GstCaps *ts_caps = gst_caps_new_empty_simple( "timestamp/x-unix" );
      gst_buffer_add_reference_timestamp_meta( *buffer, ts_caps, timestamp, GST_CLOCK_TIME_NONE );
      gst_caps_unref( ts_caps );

      // Calculate PTS starting from 0 based on first received timestamp
      if ( !GST_CLOCK_TIME_IS_VALID( src->priv->first_timestamp ) ) {
        src->priv->first_timestamp = timestamp;
      }

      if ( timestamp >= src->priv->first_timestamp ) {
        GstClockTime offset_time = timestamp - src->priv->first_timestamp;
        GST_BUFFER_PTS( *buffer ) = offset_time;
        GST_BUFFER_DTS( *buffer ) = offset_time;
      } else {
        GST_WARNING_OBJECT(
            src, "Timestamp %" GST_TIME_FORMAT " is older than first timestamp %" GST_TIME_FORMAT,
            GST_TIME_ARGS( timestamp ), GST_TIME_ARGS( src->priv->first_timestamp ) );
        GST_BUFFER_PTS( *buffer ) = 0;
        GST_BUFFER_DTS( *buffer ) = 0;
      }
    }

    return GST_FLOW_OK;
  } catch ( const std::exception &e ) {
    GST_ELEMENT_ERROR( src, STREAM, FAILED, ( "Exception in create" ), ( "%s", e.what() ) );
    return GST_FLOW_ERROR;
  } catch ( ... ) {
    GST_ELEMENT_ERROR( src, STREAM, FAILED, ( "Unknown exception in create" ), ( nullptr ) );
    return GST_FLOW_ERROR;
  }
}

gboolean rbf_image_src_plugin_init( GstPlugin *plugin )
{
  return gst_element_register( plugin, "rbfimagesrc", GST_RANK_NONE, RBF_TYPE_IMAGE_SRC );
}
