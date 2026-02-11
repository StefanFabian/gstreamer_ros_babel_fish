
#include <gst/gst.h>
#include <gtest/gtest.h>
#include <rclcpp/rclcpp.hpp>

class RbfImageSinkNVTest : public ::testing::Test
{
protected:
  void SetUp() override { gst_init( nullptr, nullptr ); }

  void TearDown() override
  {
    if ( pipeline_ ) {
      gst_element_set_state( pipeline_, GST_STATE_NULL );
      gst_object_unref( pipeline_ );
      pipeline_ = nullptr;
    }
  }

  GstElement *pipeline_ = nullptr;
};

TEST_F( RbfImageSinkNVTest, DefaultRejectsNV21 )
{
  // Pipeline: videotestsrc ! video/x-raw,format=NV21 ! rbfimagesink
  const std::string pipeline_str = "videotestsrc num-buffers=1 ! video/x-raw,format=NV21 ! "
                                   "rbfimagesink name=sink topic=/nv_test";

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );

  // With enable-nv-formats=false (default), negotiation should fail.
  // gst_parse_launch might succeed but setting state to PAUSED/PLAYING should fail or error out.

  if ( error ) {
    // If it fails to parse/link, that constitutes rejection
    GST_INFO( "Pipeline parsing failed as expected: %s", error->message );
    g_error_free( error );
    return;
  }

  // If parsing succeeded, try starting it.
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );

  if ( ret == GST_STATE_CHANGE_FAILURE ) {
    // Expected failure
    GST_INFO( "State change failed as expected." );
  } else {
    // Wait for error/eos
    GstBus *bus = gst_element_get_bus( pipeline_ );
    GstMessage *msg = gst_bus_timed_pop_filtered( bus, 1 * GST_SECOND, GST_MESSAGE_ERROR );
    if ( msg ) {
      GError *err = nullptr;
      gchar *debug_info = nullptr;
      gst_message_parse_error( msg, &err, &debug_info );
      GST_INFO( "Received expected error: %s", err->message );
      g_error_free( err );
      g_free( debug_info );
      gst_message_unref( msg );
    } else {
      FAIL() << "Pipeline should have failed with default settings (NV21 disabled)";
    }
    gst_object_unref( bus );
  }
}

TEST_F( RbfImageSinkNVTest, EnableNVFormatsAcceptsNV21 )
{
  // Pipeline: videotestsrc ! video/x-raw,format=NV21 ! rbfimagesink enable-nv-formats=true
  const std::string pipeline_str =
      "videotestsrc num-buffers=1 ! video/x-raw,format=NV21 ! "
      "rbfimagesink name=sink topic=/nv_test_enabled enable-nv-formats=true";

  GError *error = nullptr;
  pipeline_ = gst_parse_launch( pipeline_str.c_str(), &error );
  ASSERT_FALSE( error ) << "Failed to parse pipeline: " << ( error ? error->message : "" );

  // Start the pipeline
  GstStateChangeReturn ret = gst_element_set_state( pipeline_, GST_STATE_PLAYING );
  ASSERT_NE( ret, GST_STATE_CHANGE_FAILURE ) << "Failed to set pipeline to PLAYING";

  // Wait for completion (EOS or error)
  GstBus *bus = gst_element_get_bus( pipeline_ );
  GstMessage *msg = gst_bus_timed_pop_filtered(
      bus, 2 * GST_SECOND, (GstMessageType)( GST_MESSAGE_ERROR | GST_MESSAGE_EOS ) );

  if ( msg ) {
    if ( GST_MESSAGE_TYPE( msg ) == GST_MESSAGE_ERROR ) {
      GError *err = nullptr;
      gchar *debug_info = nullptr;
      gst_message_parse_error( msg, &err, &debug_info );
      FAIL() << "Pipeline failed: " << err->message;
      g_error_free( err );
      g_free( debug_info );
    }
    ASSERT_EQ( GST_MESSAGE_TYPE( msg ), GST_MESSAGE_EOS ) << "Expected EOS message.";
    gst_message_unref( msg );
  } else {
    FAIL() << "Expected EOS message.";
  }

  gst_object_unref( bus );
}
