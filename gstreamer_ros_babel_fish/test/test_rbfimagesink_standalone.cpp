
#include <gst/gst.h>
#include <gtest/gtest.h>
#include <rclcpp/rclcpp.hpp>

#include <chrono>
#include <thread>

class RbfImageSinkStandaloneTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    gst_init( nullptr, nullptr );
    // INTENTIONALLY do NOT initialize rclcpp global context here.
  }

  void TearDown() override
  {
    if ( pipeline_ ) {
      gst_element_set_state( pipeline_, GST_STATE_NULL );
      gst_object_unref( pipeline_ );
      pipeline_ = nullptr;
    }

    if ( rclcpp::ok() ) {
      rclcpp::shutdown();
    }
  }

  GstElement *pipeline_ = nullptr;
};

TEST_F( RbfImageSinkStandaloneTest, PipelineStartup )
{
  // videotestsrc -> rbfimagesink
  // We expect rbfimagesink to initialize its own node.
  // If it incorrectly checks !rclcpp::ok(), it will fail because we didn't init global rclcpp.
  const std::string pipeline_str =
      "videotestsrc num-buffers=10 ! video/x-raw,format=RGB,width=320,height=240 ! "
      "rbfimagesink topic=/standalone_sink_test";

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_NE( pipeline_, nullptr ) << "Failed to parse pipeline: "
                                  << ( error ? error->message : "unknown" );

  // Start the pipeline
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to set pipeline to PLAYING";

  // Monitor bus for errors
  GstBus *bus = gst_element_get_bus( pipeline_ );
  GstMessage *msg = gst_bus_timed_pop_filtered( bus, 2 * GST_SECOND, GST_MESSAGE_ERROR );

  if ( msg ) {
    GError *err = nullptr;
    gchar *debug_info = nullptr;
    gst_message_parse_error( msg, &err, &debug_info );

    std::string err_msg( err->message );
    g_error_free( err );
    g_free( debug_info );
    gst_message_unref( msg );

    // Fail if an error is received on the bus.
    FAIL() << "Pipeline received error: " << err_msg;
  }

  gst_object_unref( bus );
}
