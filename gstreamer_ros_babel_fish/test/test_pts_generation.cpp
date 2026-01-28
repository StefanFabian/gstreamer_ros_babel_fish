
#include <gst/app/gstappsink.h>
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

class PtsGenerationTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    gst_init( nullptr, nullptr );

    if ( !rclcpp::ok() ) {
      rclcpp::init( 0, nullptr );
    }

    node_ = std::make_shared<rclcpp::Node>( "pts_generation_test_node" );
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

TEST_F( PtsGenerationTest, ZeroBasedPtsLogic )
{
  const std::string topic = "/test_pts_generation";

  auto pub = node_->create_publisher<sensor_msgs::msg::Image>( topic, 10 );

  std::string pipeline_str =
      "rbfimagesrc topic=" + topic + " ! appsink name=sink emit-signals=true sync=false";

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << ( error ? error->message : "unknown error" );

  GstElement *sink = gst_bin_get_by_name( GST_BIN( pipeline_ ), "sink" );
  ASSERT_NE( sink, nullptr );

  gst_element_set_state( pipeline_, GST_STATE_PLAYING );

  // Wait for pipeline to be ready
  std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );

  // 1. Publish FIRST image
  // Use current time to simulate real usage
  rclcpp::Time t1 = node_->get_clock()->now();
  pub->publish( *create_test_image( t1 ) );

  // Pull first sample
  GstSample *sample1;
  g_signal_emit_by_name( sink, "pull-sample", &sample1 );

  // Retry a few times if not immediately available (ros propagation delay)
  int retries = 0;
  while ( !sample1 && retries < 20 ) {
    std::this_thread::sleep_for( std::chrono::milliseconds( 20 ) );
    g_signal_emit_by_name( sink, "pull-sample", &sample1 );
    retries++;
  }

  ASSERT_NE( sample1, nullptr ) << "Failed to receive first sample";
  GstBuffer *buf1 = gst_sample_get_buffer( sample1 );

  // Verify PTS is 0 (or close, but logic should make it exactly 0)
  GstClockTime pts1 = GST_BUFFER_PTS( buf1 );
  std::cout << "First PTS: " << pts1 << std::endl;

  // Check for Reference Timestamp Meta
  GstCaps *ts_caps = gst_caps_new_empty_simple( "timestamp/x-unix" );
  GstReferenceTimestampMeta *meta1 = gst_buffer_get_reference_timestamp_meta( buf1, ts_caps );
  gst_caps_unref( ts_caps );

  EXPECT_EQ( pts1, 0 ) << "First frame PTS should be 0";
  ASSERT_NE( meta1, nullptr ) << "First frame should have Reference Timestamp Meta";
  EXPECT_EQ( meta1->timestamp, t1.nanoseconds() ) << "Meta timestamp should match ROS time";

  gst_sample_unref( sample1 );

  // 2. Publish SECOND image 100ms later
  rclcpp::Time t2 = t1 + rclcpp::Duration( 0, 100000000 );
  pub->publish( *create_test_image( t2 ) );

  GstSample *sample2;
  g_signal_emit_by_name( sink, "pull-sample", &sample2 );

  retries = 0;
  while ( !sample2 && retries < 20 ) {
    std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );
    g_signal_emit_by_name( sink, "pull-sample", &sample2 );
    retries++;
  }

  ASSERT_NE( sample2, nullptr ) << "Failed to receive second sample";
  GstBuffer *buf2 = gst_sample_get_buffer( sample2 );

  GstClockTime pts2 = GST_BUFFER_PTS( buf2 );
  std::cout << "Second PTS: " << pts2 << std::endl;

  // PTS should be 100ms (100 * 10^6 ns)
  EXPECT_EQ( pts2, 100 * GST_MSECOND ) << "Second frame PTS should be 100ms";

  // Check Meta again
  ts_caps = gst_caps_new_empty_simple( "timestamp/x-unix" );
  GstReferenceTimestampMeta *meta2 = gst_buffer_get_reference_timestamp_meta( buf2, ts_caps );
  gst_caps_unref( ts_caps );

  ASSERT_NE( meta2, nullptr );
  EXPECT_EQ( meta2->timestamp, t2.nanoseconds() );

  gst_sample_unref( sample2 );

  gst_object_unref( sink );
}

TEST_F( PtsGenerationTest, CompressedZeroBasedPtsLogic )
{
  const std::string topic = "/test_pts_generation_compressed";

  // Publish compressed image to the topic. rbfimagesrc detects the type based on topic type.

  auto pub = node_->create_publisher<sensor_msgs::msg::CompressedImage>( topic, 10 );

  std::string pipeline_str =
      "rbfimagesrc topic=" + topic + " ! appsink name=sink emit-signals=true sync=false";

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << ( error ? error->message : "unknown error" );

  GstElement *sink = gst_bin_get_by_name( GST_BIN( pipeline_ ), "sink" );
  ASSERT_NE( sink, nullptr );

  gst_element_set_state( pipeline_, GST_STATE_PLAYING );

  std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );

  // 1. Publish FIRST compressed image
  rclcpp::Time t1 = node_->get_clock()->now();
  auto msg1 = std::make_shared<sensor_msgs::msg::CompressedImage>();
  msg1->header.stamp = t1;
  msg1->header.frame_id = "test_frame";
  msg1->format = "jpeg";
  msg1->data = { 0xFF, 0xD8, 0xFF, 0xE0 }; // minimal jpeg header
  pub->publish( *msg1 );

  GstSample *sample1;
  g_signal_emit_by_name( sink, "pull-sample", &sample1 );

  int retries = 0;
  while ( !sample1 && retries < 20 ) {
    std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );
    g_signal_emit_by_name( sink, "pull-sample", &sample1 );
    retries++;
  }

  ASSERT_NE( sample1, nullptr ) << "Failed to receive first compressed sample";
  GstBuffer *buf1 = gst_sample_get_buffer( sample1 );

  GstClockTime pts1 = GST_BUFFER_PTS( buf1 );
  EXPECT_EQ( pts1, 0 ) << "First compressed frame PTS should be 0";

  GstCaps *ts_caps = gst_caps_new_empty_simple( "timestamp/x-unix" );
  GstReferenceTimestampMeta *meta1 = gst_buffer_get_reference_timestamp_meta( buf1, ts_caps );
  gst_caps_unref( ts_caps );
  ASSERT_NE( meta1, nullptr );
  EXPECT_EQ( meta1->timestamp, t1.nanoseconds() );

  gst_sample_unref( sample1 );

  // 2. Publish SECOND compressed image 100ms later
  rclcpp::Time t2 = t1 + rclcpp::Duration( 0, 100000000 );
  auto msg2 = std::make_shared<sensor_msgs::msg::CompressedImage>();
  msg2->header.stamp = t2;
  msg2->header.frame_id = "test_frame";
  msg2->format = "jpeg";
  msg2->data = { 0xFF, 0xD8, 0xFF, 0xE0 };
  pub->publish( *msg2 );

  GstSample *sample2;
  g_signal_emit_by_name( sink, "pull-sample", &sample2 );

  retries = 0;
  while ( !sample2 && retries < 20 ) {
    std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );
    g_signal_emit_by_name( sink, "pull-sample", &sample2 );
    retries++;
  }

  ASSERT_NE( sample2, nullptr ) << "Failed to receive second compressed sample";
  GstBuffer *buf2 = gst_sample_get_buffer( sample2 );

  GstClockTime pts2 = GST_BUFFER_PTS( buf2 );
  EXPECT_EQ( pts2, 100 * GST_MSECOND ) << "Second compressed frame PTS should be 100ms";

  gst_sample_unref( sample2 );
  gst_object_unref( sink );
}

int main( int argc, char **argv )
{
  testing::InitGoogleTest( &argc, argv );
  rclcpp::init( argc, argv );
  return RUN_ALL_TESTS();
}
