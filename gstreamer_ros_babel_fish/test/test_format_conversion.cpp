#include "gstreamer_ros_babel_fish/format_conversion.hpp"
#include <gst/gst.h>
#include <gtest/gtest.h>
#include <sensor_msgs/image_encodings.hpp>

using namespace gstreamer_ros_babel_fish;
namespace enc = sensor_msgs::image_encodings;

class FormatConversionTest : public ::testing::Test
{
protected:
  void SetUp() override { gst_init( nullptr, nullptr ); }
};

TEST_F( FormatConversionTest, GstToRosRgb8 )
{
  auto encoding = gst_format_to_ros_encoding( GST_VIDEO_FORMAT_RGB );
  ASSERT_TRUE( encoding.has_value() );
  EXPECT_EQ( *encoding, enc::RGB8 );
}

TEST_F( FormatConversionTest, GstToRosBgr8 )
{
  auto encoding = gst_format_to_ros_encoding( GST_VIDEO_FORMAT_BGR );
  ASSERT_TRUE( encoding.has_value() );
  EXPECT_EQ( *encoding, enc::BGR8 );
}

TEST_F( FormatConversionTest, GstToRosMono8 )
{
  auto encoding = gst_format_to_ros_encoding( GST_VIDEO_FORMAT_GRAY8 );
  ASSERT_TRUE( encoding.has_value() );
  EXPECT_EQ( *encoding, enc::MONO8 );
}

TEST_F( FormatConversionTest, GstToRosMono16 )
{
  auto encoding = gst_format_to_ros_encoding( GST_VIDEO_FORMAT_GRAY16_LE );
  ASSERT_TRUE( encoding.has_value() );
  EXPECT_EQ( *encoding, enc::MONO16 );
}

TEST_F( FormatConversionTest, GstToRosRgba8 )
{
  auto encoding = gst_format_to_ros_encoding( GST_VIDEO_FORMAT_RGBA );
  ASSERT_TRUE( encoding.has_value() );
  EXPECT_EQ( *encoding, enc::RGBA8 );
}

TEST_F( FormatConversionTest, GstToRosBgra8 )
{
  auto encoding = gst_format_to_ros_encoding( GST_VIDEO_FORMAT_BGRA );
  ASSERT_TRUE( encoding.has_value() );
  EXPECT_EQ( *encoding, enc::BGRA8 );
}

TEST_F( FormatConversionTest, GstToRosUyvy )
{
  auto encoding = gst_format_to_ros_encoding( GST_VIDEO_FORMAT_UYVY );
  ASSERT_TRUE( encoding.has_value() );
  EXPECT_EQ( *encoding, enc::UYVY );
}

TEST_F( FormatConversionTest, GstToRosYuyv )
{
  auto encoding = gst_format_to_ros_encoding( GST_VIDEO_FORMAT_YUY2 );
  ASSERT_TRUE( encoding.has_value() );
  EXPECT_EQ( *encoding, enc::YUYV );
}

TEST_F( FormatConversionTest, GstToRosUnknown )
{
  auto encoding = gst_format_to_ros_encoding( GST_VIDEO_FORMAT_UNKNOWN );
  EXPECT_FALSE( encoding.has_value() );
}

TEST_F( FormatConversionTest, RosToGstRgb8 )
{
  auto format = ros_encoding_to_gst_format( enc::RGB8 );
  ASSERT_TRUE( format.has_value() );
  EXPECT_EQ( *format, GST_VIDEO_FORMAT_RGB );
}

TEST_F( FormatConversionTest, RosToGstBgr8 )
{
  auto format = ros_encoding_to_gst_format( enc::BGR8 );
  ASSERT_TRUE( format.has_value() );
  EXPECT_EQ( *format, GST_VIDEO_FORMAT_BGR );
}

TEST_F( FormatConversionTest, RosToGstMono8 )
{
  auto format = ros_encoding_to_gst_format( enc::MONO8 );
  ASSERT_TRUE( format.has_value() );
  EXPECT_EQ( *format, GST_VIDEO_FORMAT_GRAY8 );
}

TEST_F( FormatConversionTest, RosToGstMono16 )
{
  auto format = ros_encoding_to_gst_format( enc::MONO16 );
  ASSERT_TRUE( format.has_value() );
  EXPECT_EQ( *format, GST_VIDEO_FORMAT_GRAY16_LE );
}

TEST_F( FormatConversionTest, RosToGstYuv422Deprecated )
{
  // Test deprecated alias
  auto format = ros_encoding_to_gst_format( enc::YUV422 );
  ASSERT_TRUE( format.has_value() );
  EXPECT_EQ( *format, GST_VIDEO_FORMAT_UYVY );
}

TEST_F( FormatConversionTest, RosToGstUnknown )
{
  auto format = ros_encoding_to_gst_format( "unknown_encoding" );
  EXPECT_FALSE( format.has_value() );
}

TEST_F( FormatConversionTest, BitsPerPixelRgb8 )
{
  EXPECT_EQ( ros_encoding_bits_per_pixel( enc::RGB8 ), 24 );
}

TEST_F( FormatConversionTest, BitsPerPixelMono8 )
{
  EXPECT_EQ( ros_encoding_bits_per_pixel( enc::MONO8 ), 8 );
}

TEST_F( FormatConversionTest, BitsPerPixelMono16 )
{
  EXPECT_EQ( ros_encoding_bits_per_pixel( enc::MONO16 ), 16 );
}

TEST_F( FormatConversionTest, BitsPerPixelRgba8 )
{
  EXPECT_EQ( ros_encoding_bits_per_pixel( enc::RGBA8 ), 32 );
}

TEST_F( FormatConversionTest, CalculateStepRgb8 )
{
  EXPECT_EQ( calculate_ros_step( enc::RGB8, 640 ), 640 * 3 );
}

TEST_F( FormatConversionTest, CalculateStepMono8 )
{
  EXPECT_EQ( calculate_ros_step( enc::MONO8, 640 ), 640 );
}

TEST_F( FormatConversionTest, CalculateStepMono16 )
{
  EXPECT_EQ( calculate_ros_step( enc::MONO16, 640 ), 640 * 2 );
}

TEST_F( FormatConversionTest, IsCompressedJpeg )
{
  EXPECT_TRUE( is_compressed_encoding( "jpeg" ) );
  EXPECT_TRUE( is_compressed_encoding( "jpg" ) );
  // Extended ROS CompressedImage format strings
  EXPECT_TRUE( is_compressed_encoding( "rgb8; jpeg compressed bgr8" ) );
  EXPECT_TRUE( is_compressed_encoding( "bgr8; jpeg compressed bgr8" ) );
}

TEST_F( FormatConversionTest, IsCompressedPng )
{
  EXPECT_TRUE( is_compressed_encoding( "png" ) );
  EXPECT_TRUE( is_compressed_encoding( "rgb8; png compressed bgr8" ) );
}

TEST_F( FormatConversionTest, IsCompressedRaw )
{
  EXPECT_FALSE( is_compressed_encoding( enc::RGB8 ) );
  EXPECT_FALSE( is_compressed_encoding( enc::MONO8 ) );
}

TEST_F( FormatConversionTest, CapsToCompressionFormatJpeg )
{
  GstCaps *caps = gst_caps_new_empty_simple( "image/jpeg" );
  auto format = caps_to_compression_format( caps );
  ASSERT_TRUE( format.has_value() );
  EXPECT_EQ( *format, "jpeg" );
  gst_caps_unref( caps );
}

TEST_F( FormatConversionTest, CapsToCompressionFormatPng )
{
  GstCaps *caps = gst_caps_new_empty_simple( "image/png" );
  auto format = caps_to_compression_format( caps );
  ASSERT_TRUE( format.has_value() );
  EXPECT_EQ( *format, "png" );
  gst_caps_unref( caps );
}

TEST_F( FormatConversionTest, CapsToCompressionFormatRaw )
{
  GstCaps *caps = gst_caps_new_simple( "video/x-raw", "format", G_TYPE_STRING, "RGB", nullptr );
  auto format = caps_to_compression_format( caps );
  EXPECT_FALSE( format.has_value() );
  gst_caps_unref( caps );
}

TEST_F( FormatConversionTest, CreateCapsFromRosImage )
{
  GstCaps *caps = create_caps_from_ros_image( enc::RGB8, 640, 480, 640 * 3 );
  ASSERT_NE( caps, nullptr );

  GstStructure *structure = gst_caps_get_structure( caps, 0 );
  EXPECT_STREQ( gst_structure_get_name( structure ), "video/x-raw" );

  const gchar *format = gst_structure_get_string( structure, "format" );
  EXPECT_STREQ( format, "RGB" );

  gint width, height;
  gst_structure_get_int( structure, "width", &width );
  gst_structure_get_int( structure, "height", &height );
  EXPECT_EQ( width, 640 );
  EXPECT_EQ( height, 480 );

  gst_caps_unref( caps );
}

TEST_F( FormatConversionTest, CreateCapsForCompressedJpeg )
{
  GstCaps *caps = create_caps_for_compressed( "jpeg" );
  ASSERT_NE( caps, nullptr );

  GstStructure *structure = gst_caps_get_structure( caps, 0 );
  EXPECT_STREQ( gst_structure_get_name( structure ), "image/jpeg" );

  gst_caps_unref( caps );
}

TEST_F( FormatConversionTest, CreateCapsForCompressedPng )
{
  GstCaps *caps = create_caps_for_compressed( "png" );
  ASSERT_NE( caps, nullptr );

  GstStructure *structure = gst_caps_get_structure( caps, 0 );
  EXPECT_STREQ( gst_structure_get_name( structure ), "image/png" );

  gst_caps_unref( caps );
}

TEST_F( FormatConversionTest, GetSupportedRawCaps )
{
  GstCaps *caps = get_supported_raw_caps();
  ASSERT_NE( caps, nullptr );
  EXPECT_FALSE( gst_caps_is_empty( caps ) );

  // Check that we have multiple formats
  guint n_structures = gst_caps_get_size( caps );
  EXPECT_GT( n_structures, 1u );

  gst_caps_unref( caps );
}

TEST_F( FormatConversionTest, GetSupportedCompressedCaps )
{
  GstCaps *caps = get_supported_compressed_caps();
  ASSERT_NE( caps, nullptr );
  EXPECT_FALSE( gst_caps_is_empty( caps ) );

  // Should have JPEG and PNG
  guint n_structures = gst_caps_get_size( caps );
  EXPECT_GE( n_structures, 2u );

  gst_caps_unref( caps );
}

TEST_F( FormatConversionTest, GetAllSupportedCaps )
{
  GstCaps *caps = get_all_supported_caps();
  ASSERT_NE( caps, nullptr );
  EXPECT_FALSE( gst_caps_is_empty( caps ) );

  // Should include both raw and compressed formats
  GstCaps *raw_caps = get_supported_raw_caps();
  GstCaps *compressed_caps = get_supported_compressed_caps();

  guint expected_count = gst_caps_get_size( raw_caps ) + gst_caps_get_size( compressed_caps );
  EXPECT_EQ( gst_caps_get_size( caps ), expected_count );

  gst_caps_unref( caps );
  gst_caps_unref( raw_caps );
  gst_caps_unref( compressed_caps );
}

int main( int argc, char **argv )
{
  testing::InitGoogleTest( &argc, argv );
  return RUN_ALL_TESTS();
}
