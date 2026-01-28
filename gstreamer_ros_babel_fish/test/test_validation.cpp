#include <gst/gst.h>
#include <gtest/gtest.h>
#include <rclcpp/rclcpp.hpp>

class ValidationTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    gst_init( nullptr, nullptr );
    if ( !rclcpp::ok() ) {
      rclcpp::init( 0, nullptr );
    }
  }
};

TEST_F( ValidationTest, ImageSrcTopicValidation )
{
  GstElement *element = gst_element_factory_make( "rbfimagesrc", "src" );
  ASSERT_NE( element, nullptr );

  // Default is "/image"
  gchar *topic = nullptr;
  g_object_get( element, "topic", &topic, nullptr );
  EXPECT_STREQ( topic, "/image" );
  g_free( topic );

  // Set valid topic
  g_object_set( element, "topic", "/valid/topic", nullptr );
  g_object_get( element, "topic", &topic, nullptr );
  EXPECT_STREQ( topic, "/valid/topic" );
  g_free( topic );

  // Set invalid topic (spaces not allowed)
  // This should trigger the validation error and NOT update the property
  g_object_set( element, "topic", "invalid topic", nullptr );
  g_object_get( element, "topic", &topic, nullptr );
  // Should still be the previous valid topic
  EXPECT_STREQ( topic, "/valid/topic" );
  g_free( topic );

  // Set invalid topic (start with number)
  g_object_set( element, "topic", "1invalid", nullptr );
  g_object_get( element, "topic", &topic, nullptr );
  EXPECT_STREQ( topic, "/valid/topic" );
  g_free( topic );

  gst_object_unref( element );
}

TEST_F( ValidationTest, ImageSrcNodeNameValidation )
{
  GstElement *element = gst_element_factory_make( "rbfimagesrc", "src" );
  ASSERT_NE( element, nullptr );

  // Default
  gchar *node_name = nullptr;
  g_object_get( element, "node-name", &node_name, nullptr );
  EXPECT_STREQ( node_name, "rbfimagesrc" );
  g_free( node_name );

  // Valid
  g_object_set( element, "node-name", "valid_node_name", nullptr );
  g_object_get( element, "node-name", &node_name, nullptr );
  EXPECT_STREQ( node_name, "valid_node_name" );
  g_free( node_name );

  // Invalid (contains /)
  g_object_set( element, "node-name", "invalid/node/name", nullptr );
  g_object_get( element, "node-name", &node_name, nullptr );
  EXPECT_STREQ( node_name, "valid_node_name" );
  g_free( node_name );

  // Invalid (starts with number)
  g_object_set( element, "node-name", "1invalid", nullptr );
  g_object_get( element, "node-name", &node_name, nullptr );
  EXPECT_STREQ( node_name, "valid_node_name" );
  g_free( node_name );

  gst_object_unref( element );
}

TEST_F( ValidationTest, ImageSinkTopicValidation )
{
  GstElement *element = gst_element_factory_make( "rbfimagesink", "sink" );
  ASSERT_NE( element, nullptr );

  // Default is "/image"
  gchar *topic = nullptr;
  g_object_get( element, "topic", &topic, nullptr );
  EXPECT_STREQ( topic, "/image" );
  g_free( topic );

  // Set valid topic
  g_object_set( element, "topic", "/valid/topic", nullptr );
  g_object_get( element, "topic", &topic, nullptr );
  EXPECT_STREQ( topic, "/valid/topic" );
  g_free( topic );

  // Set invalid topic
  g_object_set( element, "topic", "invalid topic", nullptr );
  g_object_get( element, "topic", &topic, nullptr );
  EXPECT_STREQ( topic, "/valid/topic" );
  g_free( topic );

  gst_object_unref( element );
}

TEST_F( ValidationTest, ImageSinkNodeNameValidation )
{
  GstElement *element = gst_element_factory_make( "rbfimagesink", "sink" );
  ASSERT_NE( element, nullptr );

  // Default
  gchar *node_name = nullptr;
  g_object_get( element, "node-name", &node_name, nullptr );
  EXPECT_STREQ( node_name, "rbfimagesink" );
  g_free( node_name );

  // Valid
  g_object_set( element, "node-name", "valid_node_name", nullptr );
  g_object_get( element, "node-name", &node_name, nullptr );
  EXPECT_STREQ( node_name, "valid_node_name" );
  g_free( node_name );

  // Invalid
  g_object_set( element, "node-name", "invalid/node/name", nullptr );
  g_object_get( element, "node-name", &node_name, nullptr );
  EXPECT_STREQ( node_name, "valid_node_name" );
  g_free( node_name );

  gst_object_unref( element );
}

int main( int argc, char **argv )
{
  testing::InitGoogleTest( &argc, argv );
  return RUN_ALL_TESTS();
}
