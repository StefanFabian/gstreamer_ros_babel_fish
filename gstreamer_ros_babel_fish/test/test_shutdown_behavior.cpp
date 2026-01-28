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

class ShutdownBehaviorTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    gst_init( nullptr, nullptr );

    if ( !rclcpp::ok() ) {
      rclcpp::init( 0, nullptr );
    }

    node_ = std::make_shared<rclcpp::Node>( "shutdown_behavior_test_node" );
    // Use a separate executor for the test node to avoid interference if we want to run async
    executor_ = std::make_shared<rclcpp::executors::SingleThreadedExecutor>();
    executor_->add_node( node_ );

    // Start spinning in background
    spinning_ = true;
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

    // Ensure rclcpp is valid for next test if we shut it down
    if ( !rclcpp::ok() ) {
      rclcpp::init( 0, nullptr );
    }
  }

  sensor_msgs::msg::Image::SharedPtr create_test_image( uint32_t width, uint32_t height )
  {
    auto msg = std::make_shared<sensor_msgs::msg::Image>();
    msg->header.stamp = node_->get_clock()->now();
    msg->header.frame_id = "test_frame";
    msg->width = width;
    msg->height = height;
    msg->encoding = enc::RGB8;
    msg->step = width * 3;
    msg->data.resize( height * msg->step, 0 );
    return msg;
  }

  rclcpp::Node::SharedPtr node_;
  rclcpp::executors::SingleThreadedExecutor::SharedPtr executor_;
  std::thread spin_thread_;
  std::atomic<bool> spinning_{ true };

  GstElement *pipeline_ = nullptr;
};

TEST_F( ShutdownBehaviorTest, SrcNoInput )
{
  const std::string topic = "/non_existent_topic";

  std::string pipeline_str = "rbfimagesrc topic=" + topic + " ! fakesink";

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr );

  // It should be able to go to paused but might stay in paused while waiting for data
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE );

  // Wait a bit. It likely won't finish transition to PLAYING because there is no topic.
  GstState state;
  gst_element_get_state( pipeline_, &state, nullptr, 100 * GST_MSECOND );
  ASSERT_EQ( state, GST_STATE_PAUSED );

  // Now verify we can stop it.
  ret = gst_element_set_state( pipeline_, GST_STATE_NULL );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE );

  // It must reach NULL state eventually
  ret = gst_element_get_state( pipeline_, &state, nullptr, 5 * GST_SECOND );
  EXPECT_EQ( ret, GST_STATE_CHANGE_SUCCESS );
  EXPECT_EQ( state, GST_STATE_NULL );
}

TEST_F( ShutdownBehaviorTest, SrcTopicDisappears )
{
  const std::string topic = "/disappearing_topic";

  {
    auto pub = node_->create_publisher<sensor_msgs::msg::Image>( topic, 10 );

    std::string pipeline_str = "rbfimagesrc topic=" + topic + " ! fakesink";
    GError *error = nullptr;
    pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
    ASSERT_NE( pipeline_, nullptr );

    GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
    ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE );

    auto msg = create_test_image( 640, 480 );
    for ( int i = 0; i < 3; ++i ) {
      pub->publish( *msg );
      std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );
    }
  }

  // Wait for it to reach playing
  GstState state;
  GstStateChangeReturn ret = gst_element_get_state( pipeline_, &state, nullptr, 5 * GST_SECOND );
  ASSERT_EQ( ret, GST_STATE_CHANGE_SUCCESS );
  ASSERT_EQ( state, GST_STATE_PLAYING );

  std::this_thread::sleep_for( std::chrono::seconds( 1 ) );

  ret = gst_element_set_state( pipeline_, GST_STATE_NULL );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE );

  ret = gst_element_get_state( pipeline_, &state, nullptr, 5 * GST_SECOND );
  EXPECT_EQ( ret, GST_STATE_CHANGE_SUCCESS );
  EXPECT_EQ( state, GST_STATE_NULL );
}

TEST_F( ShutdownBehaviorTest, SinkPipelineStop )
{
  const std::string topic = "/sink_stop_test";

  std::string pipeline_str = "videotestsrc num-buffers=100 ! "
                             "video/x-raw,format=RGB,width=320,height=240 ! rbfimagesink topic=" +
                             topic;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr );

  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE );

  GstState state;
  ret = gst_element_get_state( pipeline_, &state, nullptr, 5 * GST_SECOND );
  ASSERT_EQ( ret, GST_STATE_CHANGE_SUCCESS );
  ASSERT_EQ( state, GST_STATE_PLAYING );

  std::this_thread::sleep_for( std::chrono::seconds( 1 ) );

  // Stop pipeline while it is running
  ret = gst_element_set_state( pipeline_, GST_STATE_NULL );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE );

  ret = gst_element_get_state( pipeline_, &state, nullptr, 5 * GST_SECOND );
  EXPECT_EQ( ret, GST_STATE_CHANGE_SUCCESS );
  EXPECT_EQ( state, GST_STATE_NULL );
}

TEST_F( ShutdownBehaviorTest, RosShutdown )
{
  const std::string topic = "/shutdown_test";

  auto pub = node_->create_publisher<sensor_msgs::msg::Image>( topic, 10 );

  std::string pipeline_str = "rbfimagesrc topic=" + topic + " ! fakesink";
  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr );

  gst_element_set_state( pipeline_, GST_STATE_PLAYING );

  auto msg = create_test_image( 320, 240 );
  for ( int i = 0; i < 5; ++i ) {
    pub->publish( *msg );
    std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );
  }

  // Wait for playing
  GstState state;
  gst_element_get_state( pipeline_, &state, nullptr, 5 * GST_SECOND );
  ASSERT_EQ( state, GST_STATE_PLAYING );

  pub->publish( *msg );

  // Shut down ROS
  rclcpp::shutdown();

  std::this_thread::sleep_for( std::chrono::milliseconds( 500 ) );

  // Stop pipeline - this should not crash and should reach NULL
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_NULL );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE );

  ret = gst_element_get_state( pipeline_, &state, nullptr, 5 * GST_SECOND );
  EXPECT_EQ( ret, GST_STATE_CHANGE_SUCCESS );
  EXPECT_EQ( state, GST_STATE_NULL );

  gst_object_unref( pipeline_ );
  pipeline_ = nullptr;
}

TEST_F( ShutdownBehaviorTest, SinkRosShutdown )
{
  const std::string topic = "/sink_shutdown_test";

  std::string pipeline_str = "videotestsrc num-buffers=100 ! "
                             "video/x-raw,format=RGB,width=320,height=240 ! rbfimagesink topic=" +
                             topic;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr );

  gst_element_set_state( pipeline_, GST_STATE_PLAYING );

  // Wait to ensure flow
  GstState state;
  gst_element_get_state( pipeline_, &state, nullptr, 5 * GST_SECOND );
  ASSERT_EQ( state, GST_STATE_PLAYING );

  std::this_thread::sleep_for( std::chrono::milliseconds( 500 ) );

  rclcpp::shutdown();

  // Wait a bit - pipeline is still pushing buffers
  std::this_thread::sleep_for( std::chrono::milliseconds( 500 ) );

  // Stop pipeline
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_NULL );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE );

  ret = gst_element_get_state( pipeline_, &state, nullptr, 5 * GST_SECOND );
  EXPECT_EQ( ret, GST_STATE_CHANGE_SUCCESS );
  EXPECT_EQ( state, GST_STATE_NULL );
}
