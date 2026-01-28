#include <gst/gst.h>
#include <gtest/gtest.h>
#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/image_encodings.hpp>
#include <sensor_msgs/msg/compressed_image.hpp>
#include <sensor_msgs/msg/image.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

namespace enc = sensor_msgs::image_encodings;

class GstToRosTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    gst_init( nullptr, nullptr );

    if ( !rclcpp::ok() ) {
      rclcpp::init( 0, nullptr );
    }

    node_ = std::make_shared<rclcpp::Node>( "gst_to_ros_test_node" );
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

  bool wait_for_messages( int count, std::chrono::milliseconds timeout )
  {
    auto start = std::chrono::steady_clock::now();
    while ( received_count_ < count ) {
      if ( std::chrono::steady_clock::now() - start > timeout ) {
        return false;
      }
      std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );
    }
    return true;
  }

  rclcpp::Node::SharedPtr node_;
  rclcpp::executors::SingleThreadedExecutor::SharedPtr executor_;
  std::thread spin_thread_;
  std::atomic<bool> spinning_{ true };

  GstElement *pipeline_ = nullptr;
  std::atomic<int> received_count_{ 0 };

  std::mutex last_msg_mutex_;
  sensor_msgs::msg::Image::SharedPtr last_image_msg_;
  sensor_msgs::msg::CompressedImage::SharedPtr last_compressed_msg_;
};

TEST_F( GstToRosTest, RawImagePublishing )
{
  const std::string topic = "/test_raw_image";
  const int width = 320;
  const int height = 240;
  const int num_frames = 5;

  // Create subscriber
  auto sub = node_->create_subscription<sensor_msgs::msg::Image>(
      topic, 10, [this]( sensor_msgs::msg::Image::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_image_msg_ = msg;
        received_count_++;
      } );

  // Give subscriber time to be discovered
  std::this_thread::sleep_for( std::chrono::milliseconds( 100 ) );

  std::string pipeline_str = "videotestsrc num-buffers=" + std::to_string( num_frames ) +
                             " ! "
                             "video/x-raw,format=RGB,width=" +
                             std::to_string( width ) + ",height=" + std::to_string( height ) +
                             ",framerate=30/1 ! "
                             "rbfimagesink topic=" +
                             topic;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  // Start pipeline
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";

  GstState state;
  ret = gst_element_get_state( pipeline_, &state, nullptr, 5 * GST_SECOND );
  ASSERT_EQ( ret, GST_STATE_CHANGE_SUCCESS );
  ASSERT_EQ( state, GST_STATE_PLAYING );

  // Wait for messages
  ASSERT_TRUE( wait_for_messages( num_frames, std::chrono::seconds( 5 ) ) )
      << "Timeout waiting for messages. Received: " << received_count_;

  // Verify last message
  {
    std::lock_guard<std::mutex> lock( last_msg_mutex_ );
    ASSERT_NE( last_image_msg_, nullptr );
    EXPECT_EQ( last_image_msg_->width, static_cast<uint32_t>( width ) );
    EXPECT_EQ( last_image_msg_->height, static_cast<uint32_t>( height ) );
    EXPECT_EQ( last_image_msg_->encoding, enc::RGB8 );
    EXPECT_EQ( last_image_msg_->step, static_cast<uint32_t>( width * 3 ) );
    EXPECT_EQ( last_image_msg_->data.size(), static_cast<size_t>( width * height * 3 ) );
  }

  // Wait for EOS
  GstBus *bus = gst_element_get_bus( pipeline_ );
  GstMessage *msg = gst_bus_timed_pop_filtered(
      bus, GST_SECOND * 5, static_cast<GstMessageType>( GST_MESSAGE_EOS | GST_MESSAGE_ERROR ) );

  if ( msg ) {
    if ( GST_MESSAGE_TYPE( msg ) == GST_MESSAGE_ERROR ) {
      GError *err;
      gchar *debug;
      gst_message_parse_error( msg, &err, &debug );
      FAIL() << "Pipeline error: " << err->message << " (" << debug << ")";
      g_error_free( err );
      g_free( debug );
    }
    gst_message_unref( msg );
  }
  gst_object_unref( bus );
}

TEST_F( GstToRosTest, CompressedJpegPublishing )
{
  const std::string topic = "/test_compressed_image";
  const int width = 320;
  const int height = 240;
  const int num_frames = 5;

  // Create subscriber on /compressed subtopic
  auto sub = node_->create_subscription<sensor_msgs::msg::CompressedImage>(
      topic + "/compressed", 10, [this]( sensor_msgs::msg::CompressedImage::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_compressed_msg_ = msg;
        received_count_++;
      } );

  // Give subscriber time to be discovered
  std::this_thread::sleep_for( std::chrono::milliseconds( 100 ) );

  std::string pipeline_str = "videotestsrc num-buffers=" + std::to_string( num_frames ) +
                             " ! "
                             "video/x-raw,width=" +
                             std::to_string( width ) + ",height=" + std::to_string( height ) +
                             ",framerate=30/1 ! "
                             "videoconvert ! jpegenc ! "
                             "rbfimagesink topic=" +
                             topic;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  // Start pipeline
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";

  // Wait for messages
  ASSERT_TRUE( wait_for_messages( num_frames, std::chrono::seconds( 5 ) ) )
      << "Timeout waiting for messages. Received: " << received_count_;

  // Verify last message
  {
    std::lock_guard<std::mutex> lock( last_msg_mutex_ );
    ASSERT_NE( last_compressed_msg_, nullptr );
    EXPECT_EQ( last_compressed_msg_->format, "jpeg" );
    EXPECT_FALSE( last_compressed_msg_->data.empty() );

    // Verify JPEG magic bytes
    ASSERT_GE( last_compressed_msg_->data.size(), 2u );
    EXPECT_EQ( last_compressed_msg_->data[0], 0xFF );
    EXPECT_EQ( last_compressed_msg_->data[1], 0xD8 );
  }

  // Wait for EOS
  GstBus *bus = gst_element_get_bus( pipeline_ );
  GstMessage *msg = gst_bus_timed_pop_filtered(
      bus, GST_SECOND * 5, static_cast<GstMessageType>( GST_MESSAGE_EOS | GST_MESSAGE_ERROR ) );

  if ( msg ) {
    if ( GST_MESSAGE_TYPE( msg ) == GST_MESSAGE_ERROR ) {
      GError *err;
      gchar *debug;
      gst_message_parse_error( msg, &err, &debug );
      FAIL() << "Pipeline error: " << err->message << " (" << debug << ")";
      g_error_free( err );
      g_free( debug );
    }
    gst_message_unref( msg );
  }
  gst_object_unref( bus );
}

TEST_F( GstToRosTest, CompressedPngPublishing )
{
  const std::string topic = "/test_png_image";
  const int width = 160;
  const int height = 120;
  const int num_frames = 3; // PNG encoding is slower

  // Create subscriber on /compressed subtopic
  auto sub = node_->create_subscription<sensor_msgs::msg::CompressedImage>(
      topic + "/compressed", 10, [this]( sensor_msgs::msg::CompressedImage::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_compressed_msg_ = msg;
        received_count_++;
      } );

  std::string pipeline_str = "videotestsrc num-buffers=" + std::to_string( num_frames ) +
                             " ! "
                             "video/x-raw,width=" +
                             std::to_string( width ) + ",height=" + std::to_string( height ) +
                             ",framerate=10/1 ! "
                             "videoconvert ! pngenc ! "
                             "rbfimagesink topic=" +
                             topic;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  // Start pipeline
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";

  // Wait for messages (longer timeout for PNG)
  ASSERT_TRUE( wait_for_messages( num_frames, std::chrono::seconds( 10 ) ) )
      << "Timeout waiting for messages. Received: " << received_count_;

  // Verify last message
  {
    std::lock_guard<std::mutex> lock( last_msg_mutex_ );
    ASSERT_NE( last_compressed_msg_, nullptr );
    EXPECT_EQ( last_compressed_msg_->format, "png" );
    EXPECT_FALSE( last_compressed_msg_->data.empty() );

    // Verify PNG magic bytes
    ASSERT_GE( last_compressed_msg_->data.size(), 8u );
    EXPECT_EQ( last_compressed_msg_->data[0], 0x89 );
    EXPECT_EQ( last_compressed_msg_->data[1], 'P' );
    EXPECT_EQ( last_compressed_msg_->data[2], 'N' );
    EXPECT_EQ( last_compressed_msg_->data[3], 'G' );
  }

  // Wait for EOS
  GstBus *bus = gst_element_get_bus( pipeline_ );
  GstMessage *msg = gst_bus_timed_pop_filtered(
      bus, GST_SECOND * 10, static_cast<GstMessageType>( GST_MESSAGE_EOS | GST_MESSAGE_ERROR ) );

  if ( msg ) {
    if ( GST_MESSAGE_TYPE( msg ) == GST_MESSAGE_ERROR ) {
      GError *err;
      gchar *debug;
      gst_message_parse_error( msg, &err, &debug );
      FAIL() << "Pipeline error: " << err->message << " (" << debug << ")";
      g_error_free( err );
      g_free( debug );
    }
    gst_message_unref( msg );
  }
  gst_object_unref( bus );
}

TEST_F( GstToRosTest, MonochromeImagePublishing )
{
  const std::string topic = "/test_mono_image";
  const int width = 320;
  const int height = 240;
  const int num_frames = 5;

  // Create subscriber
  auto sub = node_->create_subscription<sensor_msgs::msg::Image>(
      topic, 10, [this]( sensor_msgs::msg::Image::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_image_msg_ = msg;
        received_count_++;
      } );

  // Give subscriber time to be discovered
  std::this_thread::sleep_for( std::chrono::milliseconds( 100 ) );

  std::string pipeline_str = "videotestsrc num-buffers=" + std::to_string( num_frames ) +
                             " ! "
                             "video/x-raw,format=GRAY8,width=" +
                             std::to_string( width ) + ",height=" + std::to_string( height ) +
                             ",framerate=30/1 ! "
                             "rbfimagesink topic=" +
                             topic;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  // Start pipeline
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to start pipeline";

  // Wait for messages
  ASSERT_TRUE( wait_for_messages( num_frames, std::chrono::seconds( 5 ) ) )
      << "Timeout waiting for messages. Received: " << received_count_;

  // Verify last message
  {
    std::lock_guard<std::mutex> lock( last_msg_mutex_ );
    ASSERT_NE( last_image_msg_, nullptr );
    EXPECT_EQ( last_image_msg_->width, static_cast<uint32_t>( width ) );
    EXPECT_EQ( last_image_msg_->height, static_cast<uint32_t>( height ) );
    EXPECT_EQ( last_image_msg_->encoding, enc::MONO8 );
    EXPECT_EQ( last_image_msg_->step, static_cast<uint32_t>( width ) );
    EXPECT_EQ( last_image_msg_->data.size(), static_cast<size_t>( width * height ) );
  }

  // Wait for EOS
  GstBus *bus = gst_element_get_bus( pipeline_ );
  GstMessage *msg = gst_bus_timed_pop_filtered(
      bus, GST_SECOND * 5, static_cast<GstMessageType>( GST_MESSAGE_EOS | GST_MESSAGE_ERROR ) );

  if ( msg ) {
    gst_message_unref( msg );
  }
  gst_object_unref( bus );
}

TEST_F( GstToRosTest, FrameIdProperty )
{
  const std::string topic = "/test_frame_id";
  const std::string frame_id = "camera_optical_frame";
  const int num_frames = 2;

  // Create subscriber
  auto sub = node_->create_subscription<sensor_msgs::msg::Image>(
      topic, 10, [this]( sensor_msgs::msg::Image::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( last_msg_mutex_ );
        last_image_msg_ = msg;
        received_count_++;
      } );

  std::this_thread::sleep_for( std::chrono::milliseconds( 100 ) );

  // Create pipeline with frame-id property
  std::string pipeline_str = "videotestsrc num-buffers=" + std::to_string( num_frames ) +
                             " ! "
                             "video/x-raw,format=RGB,width=320,height=240,framerate=30/1 ! "
                             "rbfimagesink topic=" +
                             topic + " frame-id=" + frame_id;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr );

  gst_element_set_state( pipeline_, GST_STATE_PLAYING );

  GstState state;
  gst_element_get_state( pipeline_, &state, nullptr, 5 * GST_SECOND );
  ASSERT_EQ( state, GST_STATE_PLAYING );

  ASSERT_TRUE( wait_for_messages( num_frames, std::chrono::seconds( 5 ) ) );

  {
    std::lock_guard<std::mutex> lock( last_msg_mutex_ );
    ASSERT_NE( last_image_msg_, nullptr );
    EXPECT_EQ( last_image_msg_->header.frame_id, frame_id );
  }

  GstBus *bus = gst_element_get_bus( pipeline_ );
  GstMessage *msg = gst_bus_timed_pop_filtered(
      bus, GST_SECOND * 5, static_cast<GstMessageType>( GST_MESSAGE_EOS | GST_MESSAGE_ERROR ) );
  if ( msg ) {
    gst_message_unref( msg );
  }
  gst_object_unref( bus );
}

int main( int argc, char **argv )
{
  testing::InitGoogleTest( &argc, argv );
  return RUN_ALL_TESTS();
}
