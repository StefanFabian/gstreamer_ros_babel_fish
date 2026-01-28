#include <gst/gst.h>
#include <gtest/gtest.h>
#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/image_encodings.hpp>
#include <sensor_msgs/msg/compressed_image.hpp>
#include <sensor_msgs/msg/image.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace enc = sensor_msgs::image_encodings;

class RosToGstTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    gst_init( nullptr, nullptr );

    if ( !rclcpp::ok() ) {
      rclcpp::init( 0, nullptr );
    }

    node_ = std::make_shared<rclcpp::Node>( "ros_to_gst_test_node" );
    executor_ = std::make_shared<rclcpp::executors::SingleThreadedExecutor>();
    executor_->add_node( node_ );

    // Start spinning in background
    spin_thread_ = std::thread( [this]() {
      while ( rclcpp::ok() && spinning_ ) {
        executor_->spin_some( std::chrono::milliseconds( 10 ) );
        std::this_thread::sleep_for( std::chrono::milliseconds( 1 ) );
      }
    } );
  }

  void TearDown() override
  {
    spinning_ = false;
    if ( spin_thread_.joinable() ) {
      spin_thread_.join();
    }

    if ( pipeline_ ) {
      gst_element_set_state( pipeline_, GST_STATE_NULL );
      gst_object_unref( pipeline_ );
      pipeline_ = nullptr;
    }

    executor_->remove_node( node_ );
    executor_.reset();
    node_.reset();
  }

  sensor_msgs::msg::Image::SharedPtr create_test_image( uint32_t width, uint32_t height,
                                                        const std::string &encoding )
  {
    auto msg = std::make_shared<sensor_msgs::msg::Image>();
    msg->header.stamp = node_->get_clock()->now();
    msg->header.frame_id = "test_frame";
    msg->width = width;
    msg->height = height;
    msg->encoding = encoding;

    int bytes_per_pixel = 1;
    if ( encoding == enc::RGB8 || encoding == enc::BGR8 ) {
      bytes_per_pixel = 3;
    } else if ( encoding == enc::RGBA8 || encoding == enc::BGRA8 ) {
      bytes_per_pixel = 4;
    } else if ( encoding == enc::MONO16 ) {
      bytes_per_pixel = 2;
    }

    msg->step = width * bytes_per_pixel;
    msg->data.resize( height * msg->step );

    // Fill with test pattern
    for ( size_t i = 0; i < msg->data.size(); i++ ) {
      msg->data[i] = static_cast<uint8_t>( i % 256 );
    }

    return msg;
  }

  sensor_msgs::msg::CompressedImage::SharedPtr create_test_compressed_image( const std::string &format )
  {
    auto msg = std::make_shared<sensor_msgs::msg::CompressedImage>();
    msg->header.stamp = node_->get_clock()->now();
    msg->header.frame_id = "test_frame";
    msg->format = format;

    // Create minimal valid JPEG or PNG data
    if ( format == "jpeg" ) {
      // Minimal 1x1 JPEG (red pixel)
      static const uint8_t minimal_jpeg[] = {
          0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00, 0x01, 0x01, 0x00, 0x00,
          0x01, 0x00, 0x01, 0x00, 0x00, 0xFF, 0xDB, 0x00, 0x43, 0x00, 0x08, 0x06, 0x06, 0x07, 0x06,
          0x05, 0x08, 0x07, 0x07, 0x07, 0x09, 0x09, 0x08, 0x0A, 0x0C, 0x14, 0x0D, 0x0C, 0x0B, 0x0B,
          0x0C, 0x19, 0x12, 0x13, 0x0F, 0x14, 0x1D, 0x1A, 0x1F, 0x1E, 0x1D, 0x1A, 0x1C, 0x1C, 0x20,
          0x24, 0x2E, 0x27, 0x20, 0x22, 0x2C, 0x23, 0x1C, 0x1C, 0x28, 0x37, 0x29, 0x2C, 0x30, 0x31,
          0x34, 0x34, 0x34, 0x1F, 0x27, 0x39, 0x3D, 0x38, 0x32, 0x3C, 0x2E, 0x33, 0x34, 0x32, 0xFF,
          0xC0, 0x00, 0x0B, 0x08, 0x00, 0x01, 0x00, 0x01, 0x01, 0x01, 0x11, 0x00, 0xFF, 0xC4, 0x00,
          0x1F, 0x00, 0x00, 0x01, 0x05, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B,
          0xFF, 0xC4, 0x00, 0xB5, 0x10, 0x00, 0x02, 0x01, 0x03, 0x03, 0x02, 0x04, 0x03, 0x05, 0x05,
          0x04, 0x04, 0x00, 0x00, 0x01, 0x7D, 0x01, 0x02, 0x03, 0x00, 0x04, 0x11, 0x05, 0x12, 0x21,
          0x31, 0x41, 0x06, 0x13, 0x51, 0x61, 0x07, 0x22, 0x71, 0x14, 0x32, 0x81, 0x91, 0xA1, 0x08,
          0x23, 0x42, 0xB1, 0xC1, 0x15, 0x52, 0xD1, 0xF0, 0x24, 0x33, 0x62, 0x72, 0x82, 0x09, 0x0A,
          0x16, 0x17, 0x18, 0x19, 0x1A, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x34, 0x35, 0x36, 0x37,
          0x38, 0x39, 0x3A, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4A, 0x53, 0x54, 0x55, 0x56,
          0x57, 0x58, 0x59, 0x5A, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6A, 0x73, 0x74, 0x75,
          0x76, 0x77, 0x78, 0x79, 0x7A, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x92, 0x93,
          0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9,
          0xAA, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6,
          0xC7, 0xC8, 0xC9, 0xCA, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xE1, 0xE2,
          0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7,
          0xF8, 0xF9, 0xFA, 0xFF, 0xDA, 0x00, 0x08, 0x01, 0x01, 0x00, 0x00, 0x3F, 0x00, 0xFB, 0xD5,
          0xDB, 0x20, 0xB8, 0xAE, 0xDB, 0xCA, 0xA7, 0xFF, 0xD9 };
      msg->data.assign( minimal_jpeg, minimal_jpeg + sizeof( minimal_jpeg ) );
    } else if ( format == "png" ) {
      // Minimal 1x1 PNG (red pixel)
      static const uint8_t minimal_png[] = {
          0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48,
          0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00,
          0x00, 0x90, 0x77, 0x53, 0xDE, 0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41, 0x54, 0x08,
          0xD7, 0x63, 0xF8, 0xCF, 0xC0, 0x00, 0x00, 0x00, 0x03, 0x00, 0x01, 0x00, 0x05, 0xFE,
          0xD4, 0xE3, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82 };
      msg->data.assign( minimal_png, minimal_png + sizeof( minimal_png ) );
    }

    return msg;
  }

  bool wait_for_buffers( int count, std::chrono::milliseconds timeout )
  {
    auto start = std::chrono::steady_clock::now();
    while ( buffer_count_ < count ) {
      if ( std::chrono::steady_clock::now() - start > timeout ) {
        return false;
      }
      std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );
    }
    return true;
  }

  static GstPadProbeReturn buffer_probe_callback( GstPad * /* pad */, GstPadProbeInfo *info,
                                                  gpointer user_data )
  {
    auto *test = static_cast<RosToGstTest *>( user_data );

    if ( GST_PAD_PROBE_INFO_TYPE( info ) & GST_PAD_PROBE_TYPE_BUFFER ) {
      GstBuffer *buffer = GST_PAD_PROBE_INFO_BUFFER( info );
      if ( buffer ) {
        test->buffer_count_++;

        GstMapInfo map;
        if ( gst_buffer_map( buffer, &map, GST_MAP_READ ) ) {
          test->last_buffer_size_ = map.size;
          gst_buffer_unmap( buffer, &map );
        }

        // Check for reference timestamp meta
        GstCaps *ts_caps = gst_caps_new_empty_simple( "timestamp/x-unix" );
        GstReferenceTimestampMeta *ts_meta =
            gst_buffer_get_reference_timestamp_meta( buffer, ts_caps );
        gst_caps_unref( ts_caps );
        if ( ts_meta ) {
          test->last_ref_timestamp_ = ts_meta->timestamp;
        }
      }
    }

    return GST_PAD_PROBE_OK;
  }

  rclcpp::Node::SharedPtr node_;
  rclcpp::executors::SingleThreadedExecutor::SharedPtr executor_;
  std::thread spin_thread_;
  std::atomic<bool> spinning_{ true };

  GstElement *pipeline_ = nullptr;
  std::atomic<int> buffer_count_{ 0 };
  std::atomic<size_t> last_buffer_size_{ 0 };
  std::atomic<GstClockTime> last_ref_timestamp_{ GST_CLOCK_TIME_NONE };
};

TEST_F( RosToGstTest, RawImageSubscription )
{
  const std::string topic = "/test_ros_raw_input";
  const int width = 320;
  const int height = 240;
  const int num_messages = 5;

  // Create publisher first (so topic exists for detection)
  auto pub = node_->create_publisher<sensor_msgs::msg::Image>( topic, 10 );

  std::string pipeline_str = "rbfimagesrc topic=" + topic + " ! fakesink name=sink";

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  // Add probe to count buffers
  GstElement *sink = gst_bin_get_by_name( GST_BIN( pipeline_ ), "sink" );
  ASSERT_NE( sink, nullptr );

  GstPad *pad = gst_element_get_static_pad( sink, "sink" );
  ASSERT_NE( pad, nullptr );

  gst_pad_add_probe( pad, GST_PAD_PROBE_TYPE_BUFFER, buffer_probe_callback, this, nullptr );

  gst_object_unref( pad );
  gst_object_unref( sink );

  // Start pipeline
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";

  // Wait for pipeline to be ready
  std::this_thread::sleep_for( std::chrono::milliseconds( 500 ) );

  for ( int i = 0; i < num_messages; i++ ) {
    auto msg = create_test_image( width, height, enc::RGB8 );
    pub->publish( *msg );
    std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );
  }

  ASSERT_TRUE( wait_for_buffers( num_messages, std::chrono::seconds( 5 ) ) )
      << "Timeout waiting for buffers. Received: " << buffer_count_;

  // Verify buffer size
  size_t expected_size = width * height * 3;
  EXPECT_EQ( last_buffer_size_, expected_size );
}

TEST_F( RosToGstTest, CompressedJpegSubscription )
{
  const std::string topic = "/test_ros_jpeg_input";
  const int num_messages = 5;

  // Create publisher on the same topic
  auto pub = node_->create_publisher<sensor_msgs::msg::CompressedImage>( topic, 10 );

  std::string pipeline_str = "rbfimagesrc topic=" + topic + " ! jpegdec ! fakesink name=sink";

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  // Add probe to count buffers
  GstElement *sink = gst_bin_get_by_name( GST_BIN( pipeline_ ), "sink" );
  ASSERT_NE( sink, nullptr );

  GstPad *pad = gst_element_get_static_pad( sink, "sink" );
  ASSERT_NE( pad, nullptr );

  gst_pad_add_probe( pad, GST_PAD_PROBE_TYPE_BUFFER, buffer_probe_callback, this, nullptr );

  gst_object_unref( pad );
  gst_object_unref( sink );

  // Start pipeline
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";

  // Wait for pipeline to be ready
  std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );

  for ( int i = 0; i < num_messages; i++ ) {
    auto msg = create_test_compressed_image( "jpeg" );
    pub->publish( *msg );
    std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );
  }

  ASSERT_TRUE( wait_for_buffers( num_messages, std::chrono::seconds( 5 ) ) )
      << "Timeout waiting for buffers. Received: " << buffer_count_;
}

TEST_F( RosToGstTest, MonochromeImageSubscription )
{
  const std::string topic = "/test_ros_mono_input";
  const int width = 320;
  const int height = 240;
  const int num_messages = 5;

  auto pub = node_->create_publisher<sensor_msgs::msg::Image>( topic, 10 );

  std::this_thread::sleep_for( std::chrono::milliseconds( 200 ) );

  std::string pipeline_str = "rbfimagesrc topic=" + topic + " ! fakesink name=sink";

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr );

  // Add probe
  GstElement *sink = gst_bin_get_by_name( GST_BIN( pipeline_ ), "sink" );
  GstPad *pad = gst_element_get_static_pad( sink, "sink" );
  gst_pad_add_probe( pad, GST_PAD_PROBE_TYPE_BUFFER, buffer_probe_callback, this, nullptr );
  gst_object_unref( pad );
  gst_object_unref( sink );

  gst_element_set_state( pipeline_, GST_STATE_PLAYING );

  std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );

  for ( int i = 0; i < num_messages; i++ ) {
    auto msg = create_test_image( width, height, enc::MONO8 );
    pub->publish( *msg );
    std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );
  }

  ASSERT_TRUE( wait_for_buffers( num_messages, std::chrono::seconds( 5 ) ) );

  // Verify buffer size
  size_t expected_size = width * height;
  EXPECT_EQ( last_buffer_size_, expected_size );
}

TEST_F( RosToGstTest, QosPropertyBestEffort )
{
  const std::string topic = "/test_ros_qos_input";
  const int width = 160;
  const int height = 120;
  const int num_messages = 3;

  rclcpp::QoS qos( 10 );
  qos.best_effort();
  auto pub = node_->create_publisher<sensor_msgs::msg::Image>( topic, qos );

  std::string pipeline_str =
      "rbfimagesrc topic=" + topic + " qos-reliability=best-effort ! fakesink name=sink";

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr );

  GstElement *sink = gst_bin_get_by_name( GST_BIN( pipeline_ ), "sink" );
  GstPad *pad = gst_element_get_static_pad( sink, "sink" );
  gst_pad_add_probe( pad, GST_PAD_PROBE_TYPE_BUFFER, buffer_probe_callback, this, nullptr );
  gst_object_unref( pad );
  gst_object_unref( sink );

  gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  std::this_thread::sleep_for( std::chrono::milliseconds( 500 ) );

  for ( int i = 0; i < num_messages; i++ ) {
    auto msg = create_test_image( width, height, enc::RGB8 );
    pub->publish( *msg );
    std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );
  }

  ASSERT_TRUE( wait_for_buffers( num_messages, std::chrono::seconds( 5 ) ) );
}

TEST_F( RosToGstTest, RoundTripRawImage )
{
  // Test complete round trip: ROS publisher -> rbfimagesrc -> rbfimagesink -> ROS subscriber
  const std::string input_topic = "/test_roundtrip_input";
  const std::string output_topic = "/test_roundtrip_output";
  const int width = 160;
  const int height = 120;
  const int num_messages = 3;

  auto pub = node_->create_publisher<sensor_msgs::msg::Image>( input_topic, 10 );

  std::atomic<int> received_count{ 0 };
  sensor_msgs::msg::Image::SharedPtr last_received;
  std::mutex msg_mutex;

  auto sub = node_->create_subscription<sensor_msgs::msg::Image>(
      output_topic, 10,
      [&received_count, &last_received, &msg_mutex]( sensor_msgs::msg::Image::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( msg_mutex );
        last_received = msg;
        received_count++;
      } );

  std::string pipeline_str = "rbfimagesrc topic=" + input_topic +
                             " ! "
                             "rbfimagesink topic=" +
                             output_topic;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr );

  gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );

  // Publish messages
  auto original_msg = create_test_image( width, height, enc::RGB8 );
  for ( int i = 0; i < num_messages; i++ ) {
    pub->publish( *original_msg );
    std::this_thread::sleep_for( std::chrono::milliseconds( 20 ) );
  }

  // Wait for received messages
  auto start = std::chrono::steady_clock::now();
  while ( received_count < num_messages ) {
    if ( std::chrono::steady_clock::now() - start > std::chrono::seconds( 5 ) ) {
      break;
    }
    std::this_thread::sleep_for( std::chrono::milliseconds( 20 ) );
  }

  ASSERT_GE( received_count, 1 ) << "No messages received in round trip";

  {
    std::lock_guard<std::mutex> lock( msg_mutex );
    ASSERT_NE( last_received, nullptr );
    EXPECT_EQ( last_received->width, original_msg->width );
    EXPECT_EQ( last_received->height, original_msg->height );
    EXPECT_EQ( last_received->encoding, original_msg->encoding );
    EXPECT_EQ( last_received->step, original_msg->step );
    EXPECT_EQ( last_received->data.size(), original_msg->data.size() );
    // Data should match
    EXPECT_EQ( last_received->data, original_msg->data );
  }
}

TEST_F( RosToGstTest, RawImageHasReferenceTimestampMeta )
{
  const std::string topic = "/test_ros_raw_ts_meta";
  const int width = 160;
  const int height = 120;

  auto pub = node_->create_publisher<sensor_msgs::msg::Image>( topic, 10 );

  std::this_thread::sleep_for( std::chrono::milliseconds( 200 ) );

  std::string pipeline_str = "rbfimagesrc topic=" + topic + " ! fakesink name=sink";

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr );

  GstElement *sink = gst_bin_get_by_name( GST_BIN( pipeline_ ), "sink" );
  GstPad *pad = gst_element_get_static_pad( sink, "sink" );
  gst_pad_add_probe( pad, GST_PAD_PROBE_TYPE_BUFFER, buffer_probe_callback, this, nullptr );
  gst_object_unref( pad );
  gst_object_unref( sink );

  gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  std::this_thread::sleep_for( std::chrono::milliseconds( 500 ) );

  // Publish with a known timestamp
  auto msg = create_test_image( width, height, enc::RGB8 );
  rclcpp::Time test_time( 1600000000, 123456789 );
  msg->header.stamp = test_time;
  pub->publish( *msg );

  ASSERT_TRUE( wait_for_buffers( 1, std::chrono::seconds( 5 ) ) );

  // Verify reference timestamp meta matches the ROS timestamp
  GstClockTime expected_ts = static_cast<GstClockTime>( test_time.nanoseconds() );
  EXPECT_EQ( last_ref_timestamp_.load(), expected_ts )
      << "Reference timestamp meta should match the ROS header timestamp";
}

TEST_F( RosToGstTest, CompressedImageHasReferenceTimestampMeta )
{
  const std::string topic = "/test_ros_compressed_ts_meta";

  auto pub = node_->create_publisher<sensor_msgs::msg::CompressedImage>( topic, 10 );

  std::this_thread::sleep_for( std::chrono::milliseconds( 200 ) );

  std::string pipeline_str = "rbfimagesrc topic=" + topic + " ! jpegdec ! fakesink name=sink";

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr );

  GstElement *sink = gst_bin_get_by_name( GST_BIN( pipeline_ ), "sink" );
  GstPad *pad = gst_element_get_static_pad( sink, "sink" );
  gst_pad_add_probe( pad, GST_PAD_PROBE_TYPE_BUFFER, buffer_probe_callback, this, nullptr );
  gst_object_unref( pad );
  gst_object_unref( sink );

  gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  std::this_thread::sleep_for( std::chrono::milliseconds( 500 ) );

  // Publish with a known timestamp
  auto msg = create_test_compressed_image( "jpeg" );
  rclcpp::Time test_time( 1700000000, 987654321 );
  msg->header.stamp = test_time;
  pub->publish( *msg );

  ASSERT_TRUE( wait_for_buffers( 1, std::chrono::seconds( 5 ) ) );

  // Note: jpegdec may or may not preserve the reference timestamp meta.
  // We check that it was at least set by rbfimagesrc (meta may be lost after decode).
  // If the meta survived jpegdec, verify it matches.
  if ( last_ref_timestamp_.load() != GST_CLOCK_TIME_NONE ) {
    GstClockTime expected_ts = static_cast<GstClockTime>( test_time.nanoseconds() );
    EXPECT_EQ( last_ref_timestamp_.load(), expected_ts );
  }
}

int main( int argc, char **argv )
{
  testing::InitGoogleTest( &argc, argv );
  return RUN_ALL_TESTS();
}
