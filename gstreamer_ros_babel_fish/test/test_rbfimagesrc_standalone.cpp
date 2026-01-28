
#include <gst/gst.h>
#include <gtest/gtest.h>
#include <rclcpp/rclcpp.hpp>

#include <chrono>
#include <thread>

class RbfImageSrcStandaloneTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    gst_init( nullptr, nullptr );
    // INTENTIONALLY do NOT initialize rclcpp global context here.
    // This simulates gst-launch behavior where the plugin acts alone.
  }

  void TearDown() override
  {
    if ( pipeline_ ) {
      gst_element_set_state( pipeline_, GST_STATE_NULL );
      gst_object_unref( pipeline_ );
      pipeline_ = nullptr;
    }

    // Clean up if the plugin initialized something (it shouldn't affect global state, but good practice)
    if ( rclcpp::ok() ) {
      rclcpp::shutdown();
    }
  }

  GstElement *pipeline_ = nullptr;
};

TEST_F( RbfImageSrcStandaloneTest, PipelineStartup )
{
  // We use fakesink to avoid needing a display or real consumer
  // We use a non-existent topic, so it should just wait/search, without erroring out immediately.
  const std::string pipeline_str = "rbfimagesrc topic=/standalone_test_topic ! fakesink";

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to parse pipeline: "
                                  << ( error ? error->message : "unknown" );

  // Start the pipeline
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to set pipeline to PLAYING";

  // Wait a moment.
  // If the bug exists, rbfimagesrc will detect "ROS shutdown" (because !rclcpp::ok())
  // and post an error message on the bus.

  // We monitor the bus for errors for a short duration.
  GstBus *bus = gst_element_get_bus( pipeline_ );
  GstMessage *msg = gst_bus_timed_pop_filtered( bus, 1 * GST_SECOND, GST_MESSAGE_ERROR );

  if ( msg ) {
    GError *err = nullptr;
    gchar *debug_info = nullptr;
    gst_message_parse_error( msg, &err, &debug_info );

    // If the pipeline receives an error, fail the test.
    // Previously, this would fail with "ROS shutdown detected" when rclcpp::ok() was false.

    std::string err_msg( err->message );
    g_error_free( err );
    g_free( debug_info );
    gst_message_unref( msg );

    FAIL() << "Pipeline received error: " << err_msg;
  }

  gst_object_unref( bus );

  // If we got here, no error occurred in the first second.
  // Success!
}
