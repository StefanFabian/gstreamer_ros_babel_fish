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

#include <rcl/validate_topic_name.h>
#include <rclcpp/rclcpp.hpp>
#include <rmw/validate_node_name.h>
#include <sensor_msgs/msg/compressed_image.hpp>
#include <sensor_msgs/msg/image.hpp>

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
  PROP_PREFER_COMPRESSED,
  PROP_SUBSCRIPTION_COUNT,
};

// Private data structure
struct _RbfImageSinkPrivate {
  // Properties
  gchar *topic;
  gchar *node_name;
  gchar *frame_id;
  gpointer external_node; // rclcpp::Node*
  gboolean prefer_compressed;

  // ROS interface
  std::unique_ptr<RosNodeInterface> ros_interface;
  rclcpp::Publisher<sensor_msgs::msg::Image>::SharedPtr image_pub;
  rclcpp::Publisher<sensor_msgs::msg::CompressedImage>::SharedPtr compressed_pub;

  // State
  gboolean is_compressed;
  std::string compression_format;
  GstVideoInfo video_info;
  gboolean video_info_valid;

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
      gobject_class, PROP_PREFER_COMPRESSED,
      g_param_spec_boolean( "prefer-compressed", "Prefer Compressed",
                            "Prefer compressed formats during caps negotiation", TRUE,
                            (GParamFlags)( G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS ) ) );

  g_object_class_install_property(
      gobject_class, PROP_SUBSCRIPTION_COUNT,
      g_param_spec_int( "subscription-count", "Subscription Count",
                        "Number of subscribers to the image topic", 0, G_MAXINT, 0,
                        (GParamFlags)( G_PARAM_READABLE | G_PARAM_STATIC_STRINGS ) ) );

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
  sink->priv->external_node = nullptr;
  sink->priv->prefer_compressed = TRUE;
  sink->priv->is_compressed = FALSE;
  sink->priv->video_info_valid = FALSE;

  // Disable synchronization by default for real-time streaming
  gst_base_sink_set_sync( GST_BASE_SINK( sink ), FALSE );
}

static void rbf_image_sink_finalize( GObject *object )
{
  RbfImageSink *sink = RBF_IMAGE_SINK( object );

  g_free( sink->priv->topic );
  g_free( sink->priv->node_name );
  g_free( sink->priv->frame_id );

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
  case PROP_PREFER_COMPRESSED:
    sink->priv->prefer_compressed = g_value_get_boolean( value );
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
  case PROP_PREFER_COMPRESSED:
    g_value_set_boolean( value, sink->priv->prefer_compressed );
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

  return caps;
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
    rclcpp::QoS qos( 1 );
    qos.reliable();
    qos.durability_volatile();

    // Reset publishers
    sink->priv->image_pub.reset();
    sink->priv->compressed_pub.reset();

    if ( compression_format ) {
      // Compressed image - publish to topic/compressed
      sink->priv->is_compressed = TRUE;
      sink->priv->compression_format = *compression_format;
      sink->priv->video_info_valid = FALSE;

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

        sink->priv->image_pub->publish( std::move( loaned_msg ) );
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

        sink->priv->image_pub->publish( std::move( msg ) );
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
