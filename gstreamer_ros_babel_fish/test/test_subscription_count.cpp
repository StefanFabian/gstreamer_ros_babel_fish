#include <gst/gst.h>
#include <gtest/gtest.h>
#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/msg/image.hpp>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

class SubscriptionCountTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    gst_init( nullptr, nullptr );

    if ( !rclcpp::ok() ) {
      rclcpp::init( 0, nullptr );
    }

    node_ = std::make_shared<rclcpp::Node>( "subscription_count_test_node" );
    pipeline_ = nullptr;
  }

  void TearDown() override
  {
    if ( pipeline_ ) {
      gst_element_set_state( pipeline_, GST_STATE_NULL );
      gst_object_unref( pipeline_ );
      pipeline_ = nullptr;
    }
  }

  rclcpp::Node::SharedPtr node_;
  GstElement *pipeline_;
};

TEST_F( SubscriptionCountTest, CountSubscribers )
{
  const std::string topic = "/test_subscription_count";

  // Create pipeline: videotestsrc ! rbfimagesink
  std::string pipeline_str =
      "videotestsrc is-live=true ! video/x-raw,format=RGB,width=320,height=240,framerate=10/1 ! "
      "rbfimagesink name=sink topic=" +
      topic;

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to create pipeline: "
                                  << ( error ? error->message : "unknown error" );

  GstElement *sink = gst_bin_get_by_name( GST_BIN( pipeline_ ), "sink" );
  ASSERT_NE( sink, nullptr );

  // Start pipeline
  gst_element_set_state( pipeline_, GST_STATE_PLAYING );

  // Wait for pipeline to start up
  std::this_thread::sleep_for( std::chrono::milliseconds( 500 ) );

  // Check initial count (should be 0)
  int count = 0;
  g_object_get( sink, "subscription-count", &count, nullptr );
  EXPECT_EQ( count, 0 );

  // Create subscriber
  auto sub = node_->create_subscription<sensor_msgs::msg::Image>(
      topic, 10, []( sensor_msgs::msg::Image::SharedPtr ) { } );

  // Wait for discovery (discovery can take some time)
  // We poll for a bit
  bool found = false;
  for ( int i = 0; i < 20; ++i ) {
    g_object_get( sink, "subscription-count", &count, nullptr );
    if ( count == 1 ) {
      found = true;
      break;
    }
    std::this_thread::sleep_for( std::chrono::milliseconds( 100 ) );
  }
  EXPECT_TRUE( found ) << "Failed to detect subscriber. Count: " << count;

  // Destroy subscriber and check count goes back to 0
  sub.reset();

  found = false;
  for ( int i = 0; i < 20; ++i ) {
    g_object_get( sink, "subscription-count", &count, nullptr );
    if ( count == 0 ) {
      found = true;
      break;
    }
    std::this_thread::sleep_for( std::chrono::milliseconds( 100 ) );
  }
  EXPECT_TRUE( found ) << "Failed to detect detached subscriber. Count: " << count;

  gst_object_unref( sink );
}
