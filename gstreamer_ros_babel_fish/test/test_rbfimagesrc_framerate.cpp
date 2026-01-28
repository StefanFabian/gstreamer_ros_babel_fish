
#include <gst/app/gstappsink.h>
#include <gst/gst.h>
#include <gtest/gtest.h>
#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/image_encodings.hpp>
#include <sensor_msgs/msg/image.hpp>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

namespace enc = sensor_msgs::image_encodings;

class RbfImageSrcFramerateTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    gst_init( nullptr, nullptr );
    if ( !rclcpp::ok() ) {
      rclcpp::init( 0, nullptr );
    }

    node_ = std::make_shared<rclcpp::Node>( "framerate_test_node" );
    executor_ = std::make_shared<rclcpp::executors::MultiThreadedExecutor>();
    executor_->add_node( node_ );
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

TEST_F( RbfImageSrcFramerateTest, FramerateProperty )
{
  const std::string topic = "/test_framerate_prop";
  auto pub = node_->create_publisher<sensor_msgs::msg::Image>( topic, 10 );

  // Set fixed framerate 30/1
  std::string pipeline_str = "rbfimagesrc topic=" + topic +
                             " framerate=30/1 ! appsink name=sink emit-signals=true sync=false";

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << ( error ? error->message : "unknown error" );

  GstElement *sink = gst_bin_get_by_name( GST_BIN( pipeline_ ), "sink" );
  gst_element_set_state( pipeline_, GST_STATE_PLAYING );

  // Wait for pipeline to start
  std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );

  // Publish image
  pub->publish( *create_test_image( node_->get_clock()->now() ) );

  GstSample *sample;
  g_signal_emit_by_name( sink, "pull-sample", &sample );

  int retries = 0;
  while ( !sample && retries < 20 ) {
    std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );
    g_signal_emit_by_name( sink, "pull-sample", &sample );
    retries++;
  }

  ASSERT_NE( sample, nullptr ) << "Failed to receive sample";
  GstCaps *caps = gst_sample_get_caps( sample );
  ASSERT_NE( caps, nullptr );

  GstStructure *s = gst_caps_get_structure( caps, 0 );
  int num, den;
  gst_structure_get_fraction( s, "framerate", &num, &den );

  // Expect 30/1
  EXPECT_EQ( num, 30 );
  EXPECT_EQ( den, 1 );

  gst_sample_unref( sample );
  gst_object_unref( sink );
}

TEST_F( RbfImageSrcFramerateTest, DetermineFramerate )
{
  const std::string topic = "/test_determine_framerate";
  auto pub = node_->create_publisher<sensor_msgs::msg::Image>( topic, 10 );

  // determine-framerate=true, wait-frame-count=5
  // We expect it to drop the first 5 frames while determining.
  // We publish at 10Hz.
  std::string pipeline_str = "rbfimagesrc topic=" + topic +
                             " determine-framerate=true wait-frame-count=5 ! appsink name=sink "
                             "emit-signals=true sync=false";

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << ( error ? error->message : "unknown error" );

  GstElement *sink = gst_bin_get_by_name( GST_BIN( pipeline_ ), "sink" );
  gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );

  // Publish 10 images at 10Hz (100ms interval)
  rclcpp::Time start_time = node_->get_clock()->now();
  for ( int i = 0; i < 10; ++i ) {
    pub->publish( *create_test_image( start_time + rclcpp::Duration( 0, i * 100000000 ) ) );
    std::this_thread::sleep_for( std::chrono::milliseconds( 100 ) );
  }

  // We expect to receive 6 frames (frames 5, 6, 7, 8, 9, 10)
  // The first 4 are consumed for calculation (dropped), the 5th triggers determination and is pushed.
  for ( int i = 0; i < 6; ++i ) {
    GstSample *sample = nullptr;
    // Try pulling
    int retries = 0;
    while ( !sample && retries < 10 ) {
      g_signal_emit_by_name( sink, "pull-sample", &sample );
      if ( !sample )
        std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );
      retries++;
    }
    ASSERT_NE( sample, nullptr ) << "Failed to receive sample " << i + 1 << " (after determination)";

    // Check caps on the first received frame
    if ( i == 0 ) {
      GstCaps *caps = gst_sample_get_caps( sample );
      GstStructure *s = gst_caps_get_structure( caps, 0 );
      int num, den;
      gst_structure_get_fraction( s, "framerate", &num, &den );
      // Expect 10/1
      EXPECT_EQ( num, 10 );
      EXPECT_EQ( den, 1 );
    }
    gst_sample_unref( sample );
  }

  // Ensure no more samples (consumer should be drained)
  GstSample *extra_sample = nullptr;
  // Use try-pull-sample with a small timeout (50ms) to avoid blocking indefinitely
  g_signal_emit_by_name( sink, "try-pull-sample", (guint64)50 * GST_MSECOND, &extra_sample );

  EXPECT_EQ( extra_sample, nullptr ) << "Received more samples than expected";
  if ( extra_sample )
    gst_sample_unref( extra_sample );

  gst_object_unref( sink );
}
