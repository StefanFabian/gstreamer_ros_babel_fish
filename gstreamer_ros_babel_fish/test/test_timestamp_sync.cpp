#include <gst/gst.h>
#include <gtest/gtest.h>
#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/image_encodings.hpp>
#include <sensor_msgs/msg/compressed_image.hpp>
#include <sensor_msgs/msg/image.hpp>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace enc = sensor_msgs::image_encodings;

class TimestampSyncTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    gst_init( nullptr, nullptr );

    if ( !rclcpp::ok() ) {
      rclcpp::init( 0, nullptr );
    }

    node_ = std::make_shared<rclcpp::Node>( "timestamp_sync_test_node" );
    // Use a multi-threaded executor to allow proper callback processing
    executor_ = std::make_shared<rclcpp::executors::MultiThreadedExecutor>();
    executor_->add_node( node_ );

    // Start spinning in background
    scanning_ = true;
    spin_thread_ = std::thread( [this]() { executor_->spin(); } );
  }

  void TearDown() override
  {
    if ( executor_ ) {
      executor_->cancel();
    }
    if ( spin_thread_.joinable() ) {
      spin_thread_.join();
    }

    if ( pipeline_ ) {
      gst_element_set_state( pipeline_, GST_STATE_NULL );
      gst_object_unref( pipeline_ );
      pipeline_ = nullptr;
    }

    executor_.reset();
    node_.reset();
  }

  sensor_msgs::msg::Image::SharedPtr create_test_image( const rclcpp::Time &timestamp )
  {
    auto msg = std::make_shared<sensor_msgs::msg::Image>();
    msg->header.stamp = timestamp;
    msg->header.frame_id = "test_frame";
    msg->width = 320;
    msg->height = 240;
    msg->encoding = enc::RGB8;
    msg->step = 320 * 3;
    msg->data.resize( msg->height * msg->step, 0 );
    return msg;
  }

  rclcpp::Node::SharedPtr node_;
  rclcpp::executors::MultiThreadedExecutor::SharedPtr executor_;
  std::thread spin_thread_;
  std::atomic<bool> scanning_{ false };

  GstElement *pipeline_ = nullptr;
};

TEST_F( TimestampSyncTest, PassThroughTimestampPreservation )
{
  const std::string input_topic = "/test_timestamp_in";
  const std::string output_topic = "/test_timestamp_out";

  auto pub = node_->create_publisher<sensor_msgs::msg::Image>( input_topic, 10 );

  std::mutex msg_mutex;
  sensor_msgs::msg::Image::SharedPtr received_msg;
  std::atomic<bool> received{ false };

  auto sub = node_->create_subscription<sensor_msgs::msg::Image>(
      output_topic, 10, [&]( sensor_msgs::msg::Image::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( msg_mutex );
        received_msg = msg;
        received = true;
      } );

  // Identity pipeline
  std::string pipeline_str =
      "rbfimagesrc topic=" + input_topic + " ! identity ! rbfimagesink topic=" + output_topic;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << ( error ? error->message : "unknown error" );

  gst_element_set_state( pipeline_, GST_STATE_PLAYING );

  // Choose a specific timestamp (not necessarily now)
  rclcpp::Time test_time( 1600000000, 123456789 );

  // Wait for pipeline to be ready
  std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );

  pub->publish( *create_test_image( test_time ) );

  // Wait for message
  auto start = std::chrono::steady_clock::now();
  while ( !received ) {
    if ( std::chrono::steady_clock::now() - start > std::chrono::seconds( 5 ) ) {
      break;
    }
    std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );
  }

  if ( received ) {
    std::lock_guard<std::mutex> lock( msg_mutex );
    // Verify timestamp match
    EXPECT_EQ( received_msg->header.stamp, test_time );

    if ( received_msg->header.stamp != test_time ) {
      int64_t diff =
          rclcpp::Time( received_msg->header.stamp ).nanoseconds() - test_time.nanoseconds();
      std::cout << "Timestamp mismatch! Diff: " << diff << " ns (" << (double)diff / 1e9 << " s)"
                << std::endl;
      std::cout << "Expected: " << test_time.nanoseconds()
                << " Received: " << rclcpp::Time( received_msg->header.stamp ).nanoseconds()
                << std::endl;
    }
  } else {
    FAIL() << "Did not receive message";
  }
}

TEST_F( TimestampSyncTest, FallbackTimestampGeneration )
{
  const std::string topic = "/test_fallback_timestamp";

  std::mutex msg_mutex;
  sensor_msgs::msg::Image::SharedPtr received_msg;
  std::atomic<bool> received{ false };

  auto sub = node_->create_subscription<sensor_msgs::msg::Image>(
      topic, 10, [&]( sensor_msgs::msg::Image::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( msg_mutex );
        received_msg = msg;
        received = true;
      } );

  // Use appsrc to manually control PTS
  std::string pipeline_str =
      "appsrc name=src format=3 is-live=true ! "
      "video/x-raw,format=RGB,width=320,height=240,framerate=30/1 ! rbfimagesink topic=" +
      topic;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << ( error ? error->message : "unknown error" );

  GstElement *src = gst_bin_get_by_name( GST_BIN( pipeline_ ), "src" );
  ASSERT_NE( src, nullptr );

  gst_element_set_state( pipeline_, GST_STATE_PLAYING );

  // Wait for pipeline to start
  std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );

  // Get base time to calculate correct PTS
  GstClockTime base_time = gst_element_get_base_time( pipeline_ );
  ASSERT_TRUE( GST_CLOCK_TIME_IS_VALID( base_time ) );

  std::this_thread::sleep_for( std::chrono::milliseconds( 500 ) );

  // Get current running time
  GstClock *clock = gst_element_get_clock( pipeline_ );
  GstClockTime now_cnt = gst_clock_get_time( clock );
  GstClockTime running_time = now_cnt - base_time;
  gst_object_unref( clock );

  // Create a buffer that was "captured" 500ms ago
  GstClockTime latency = 500 * GST_MSECOND;
  if ( running_time < latency )
    running_time = latency + 1; // Safety
  GstClockTime pts = running_time - latency;

  GstBuffer *buffer = gst_buffer_new_allocate( nullptr, 320 * 240 * 3, nullptr );
  GST_BUFFER_PTS( buffer ) = pts;
  GST_BUFFER_DTS( buffer ) = pts;

  // Push buffer
  GstFlowReturn ret;
  g_signal_emit_by_name( src, "push-buffer", buffer, &ret );
  ASSERT_EQ( ret, GST_FLOW_OK );
  gst_object_unref( src );

  // Wait for message
  auto start = std::chrono::steady_clock::now();
  while ( !received ) {
    if ( std::chrono::steady_clock::now() - start > std::chrono::seconds( 5 ) ) {
      break;
    }
    std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );
  }

  if ( received ) {
    std::lock_guard<std::mutex> lock( msg_mutex );
    rclcpp::Time msg_time = received_msg->header.stamp;
    rclcpp::Time current_time = node_->get_clock()->now();

    // The message timestamp should be approx creation_time (500ms ago), NOT current_time
    // Allow generous tolerance for clock skew/scheduling and processing, but 500ms diff is distinct.

    int64_t diff_from_now = current_time.nanoseconds() - msg_time.nanoseconds();

    // Check that diff is at least ~450ms (proving we didn't use now())
    EXPECT_GT( diff_from_now, 450 * 1000000 );

    // Check that it's not TOO old (e.g. 0 based PTS error)
    // 500ms + some processing time. Should be < 800ms
    EXPECT_LT( diff_from_now, 800 * 1000000 );

  } else {
    FAIL() << "Did not receive message from appsrc";
  }
}

TEST_F( TimestampSyncTest, JpegEncodedTimestampPreservation )
{
  const std::string input_topic = "/test_timestamp_jpeg_in";
  const std::string output_topic = "/test_timestamp_jpeg_out";

  auto pub = node_->create_publisher<sensor_msgs::msg::Image>( input_topic, 10 );

  std::mutex msg_mutex;
  sensor_msgs::msg::CompressedImage::SharedPtr received_msg;
  std::atomic<bool> received{ false };

  // Subscribe to compressed output
  auto sub = node_->create_subscription<sensor_msgs::msg::CompressedImage>(
      output_topic + "/compressed", 10, [&]( sensor_msgs::msg::CompressedImage::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( msg_mutex );
        received_msg = msg;
        received = true;
      } );

  std::string pipeline_str = "rbfimagesrc topic=" + input_topic +
                             " ! videoconvert ! jpegenc ! rbfimagesink topic=" + output_topic;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << ( error ? error->message : "unknown error" );

  gst_element_set_state( pipeline_, GST_STATE_PLAYING );

  rclcpp::Time test_time( 1700000000, 987654321 );

  std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );

  pub->publish( *create_test_image( test_time ) );

  auto start = std::chrono::steady_clock::now();
  while ( !received ) {
    if ( std::chrono::steady_clock::now() - start > std::chrono::seconds( 5 ) ) {
      break;
    }
    std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );
  }

  if ( received ) {
    std::lock_guard<std::mutex> lock( msg_mutex );
    EXPECT_EQ( received_msg->header.stamp, test_time );
    if ( received_msg->header.stamp != test_time ) {
      int64_t diff =
          rclcpp::Time( received_msg->header.stamp ).nanoseconds() - test_time.nanoseconds();
      std::cout << "Timestamp mismatch! Diff: " << diff << " ns (" << (double)diff / 1e9 << " s)"
                << std::endl;
      std::cout << "Expected: " << test_time.nanoseconds()
                << " Received: " << rclcpp::Time( received_msg->header.stamp ).nanoseconds()
                << std::endl;
    }
  } else {
    FAIL() << "Did not receive message";
  }
}

TEST_F( TimestampSyncTest, FutureTimestampPreservation )
{
  const std::string input_topic = "/test_timestamp_future_in";
  const std::string output_topic = "/test_timestamp_future_out";

  auto pub = node_->create_publisher<sensor_msgs::msg::Image>( input_topic, 10 );

  std::mutex msg_mutex;
  sensor_msgs::msg::Image::SharedPtr received_msg;
  std::atomic<bool> received{ false };

  auto sub = node_->create_subscription<sensor_msgs::msg::Image>(
      output_topic, 10, [&]( sensor_msgs::msg::Image::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( msg_mutex );
        received_msg = msg;
        received = true;
      } );

  std::string pipeline_str =
      "rbfimagesrc topic=" + input_topic + " ! identity ! rbfimagesink topic=" + output_topic;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << ( error ? error->message : "unknown error" );

  gst_element_set_state( pipeline_, GST_STATE_PLAYING );

  // Future timestamp (Year 2030)
  // 1900000000 seconds
  int64_t future_ns = 1900000000LL * 1000000000LL;
  rclcpp::Time test_time( future_ns );

  std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );

  pub->publish( *create_test_image( test_time ) );

  auto start = std::chrono::steady_clock::now();
  while ( !received ) {
    if ( std::chrono::steady_clock::now() - start > std::chrono::seconds( 5 ) ) {
      break;
    }
    std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );
  }

  if ( received ) {
    std::lock_guard<std::mutex> lock( msg_mutex );
    EXPECT_EQ( received_msg->header.stamp, test_time );
    if ( received_msg->header.stamp != test_time ) {
      int64_t diff =
          rclcpp::Time( received_msg->header.stamp ).nanoseconds() - test_time.nanoseconds();
      std::cout << "Timestamp mismatch! Diff: " << diff << " ns (" << (double)diff / 1e9 << " s)"
                << std::endl;
      std::cout << "Expected: " << test_time.nanoseconds()
                << " Received: " << rclcpp::Time( received_msg->header.stamp ).nanoseconds()
                << std::endl;
    }
  } else {
    FAIL() << "Did not receive message";
  }
}

TEST_F( TimestampSyncTest, PastTimestampPreservation )
{
  const std::string input_topic = "/test_timestamp_past_in";
  const std::string output_topic = "/test_timestamp_past_out";

  auto pub = node_->create_publisher<sensor_msgs::msg::Image>( input_topic, 10 );

  std::mutex msg_mutex;
  sensor_msgs::msg::Image::SharedPtr received_msg;
  std::atomic<bool> received{ false };

  auto sub = node_->create_subscription<sensor_msgs::msg::Image>(
      output_topic, 10, [&]( sensor_msgs::msg::Image::SharedPtr msg ) {
        std::lock_guard<std::mutex> lock( msg_mutex );
        received_msg = msg;
        received = true;
      } );

  std::string pipeline_str =
      "rbfimagesrc topic=" + input_topic + " ! identity ! rbfimagesink topic=" + output_topic;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << ( error ? error->message : "unknown error" );

  gst_element_set_state( pipeline_, GST_STATE_PLAYING );

  // Past timestamp (Year 2000)
  rclcpp::Time test_time( 946684800, 0 );

  std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );

  pub->publish( *create_test_image( test_time ) );

  auto start = std::chrono::steady_clock::now();
  while ( !received ) {
    if ( std::chrono::steady_clock::now() - start > std::chrono::seconds( 5 ) ) {
      break;
    }
    std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );
  }

  if ( received ) {
    std::lock_guard<std::mutex> lock( msg_mutex );
    EXPECT_EQ( received_msg->header.stamp, test_time );
    if ( received_msg->header.stamp != test_time ) {
      int64_t diff =
          rclcpp::Time( received_msg->header.stamp ).nanoseconds() - test_time.nanoseconds();
      std::cout << "Timestamp mismatch! Diff: " << diff << " ns (" << (double)diff / 1e9 << " s)"
                << std::endl;
      std::cout << "Expected: " << test_time.nanoseconds()
                << " Received: " << rclcpp::Time( received_msg->header.stamp ).nanoseconds()
                << std::endl;
    }
  } else {
    FAIL() << "Did not receive message";
  }
}

int main( int argc, char **argv )
{
  testing::InitGoogleTest( &argc, argv );
  rclcpp::init( argc, argv );
  return RUN_ALL_TESTS();
}
