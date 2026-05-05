#include "gstreamer_ros_babel_fish/format_conversion.hpp"
#include "jpeg_fixtures.hpp"
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

// JPEG header parsing - populated caps must satisfy strict decoders. Three
// positive cases cover the YCbCr / grayscale / fallback branches; the RGB
// branch is exercised manually elsewhere because gst-launch can't natively
// produce Adobe-APP14-RGB JPEGs.
TEST_F( FormatConversionTest, JpegBaseline420EmitsFullCaps )
{
  using namespace gstreamer_ros_babel_fish_test;
  GstCaps *caps =
      create_caps_for_compressed( "jpeg", kJpegBaseline420, sizeof( kJpegBaseline420 ), 30, 1 );
  ASSERT_NE( caps, nullptr );

  GstStructure *s = gst_caps_get_structure( caps, 0 );
  EXPECT_STREQ( gst_structure_get_name( s ), "image/jpeg" );

  gint sof = -1, width = 0, height = 0;
  EXPECT_TRUE( gst_structure_get_int( s, "sof-marker", &sof ) );
  EXPECT_EQ( sof, 0 );
  EXPECT_STREQ( gst_structure_get_string( s, "colorspace" ), "sYUV" );
  EXPECT_STREQ( gst_structure_get_string( s, "sampling" ), "YCbCr-4:2:0" );
  EXPECT_TRUE( gst_structure_get_int( s, "width", &width ) );
  EXPECT_EQ( width, 16 );
  EXPECT_TRUE( gst_structure_get_int( s, "height", &height ) );
  EXPECT_EQ( height, 16 );
  EXPECT_STREQ( gst_structure_get_string( s, "interlace-mode" ), "progressive" );

  gst_caps_unref( caps );
}

TEST_F( FormatConversionTest, JpegGrayscaleEmitsGrayCaps )
{
  using namespace gstreamer_ros_babel_fish_test;
  GstCaps *caps =
      create_caps_for_compressed( "jpeg", kJpegGrayscale, sizeof( kJpegGrayscale ), 30, 1 );
  ASSERT_NE( caps, nullptr );

  GstStructure *s = gst_caps_get_structure( caps, 0 );
  EXPECT_STREQ( gst_structure_get_string( s, "colorspace" ), "GRAY" );
  EXPECT_STREQ( gst_structure_get_string( s, "sampling" ), "GRAYSCALE" );

  gst_caps_unref( caps );
}

TEST_F( FormatConversionTest, JpegProgressiveEmitsFullCapsWithSofMarker2 )
{
  using namespace gstreamer_ros_babel_fish_test;
  GstCaps *caps =
      create_caps_for_compressed( "jpeg", kJpegProgressive, sizeof( kJpegProgressive ), 30, 1 );
  ASSERT_NE( caps, nullptr );

  GstStructure *s = gst_caps_get_structure( caps, 0 );
  EXPECT_STREQ( gst_structure_get_name( s ), "image/jpeg" );
  // Non-baseline still gets full caps; sof-marker=2 lets strict decoders
  // self-exclude during negotiation, leaving the non-strict path to handle
  // progressive input.
  gint sof = -1, width = 0, height = 0;
  EXPECT_TRUE( gst_structure_get_int( s, "sof-marker", &sof ) );
  EXPECT_EQ( sof, 2 );
  EXPECT_STREQ( gst_structure_get_string( s, "colorspace" ), "sYUV" );
  EXPECT_STREQ( gst_structure_get_string( s, "sampling" ), "YCbCr-4:2:0" );
  EXPECT_TRUE( gst_structure_get_int( s, "width", &width ) );
  EXPECT_EQ( width, 16 );
  EXPECT_TRUE( gst_structure_get_int( s, "height", &height ) );
  EXPECT_EQ( height, 16 );

  gst_caps_unref( caps );
}

// Pins the APP14 parsing branch. Without the +2 length-byte skip, payload[11]
// reads flags1[0] (=0) instead of the transform byte (=1), so the parser
// takes the transform=0 branch with numeric component IDs (which neither
// match RGB nor BGR) and would emit bare caps. With the fix, transform=1 is
// recognised and the Adobe branch emits sYUV / YCbCr-4:4:4.
TEST_F( FormatConversionTest, JpegAdobeApp14Transform1IsDetectedAsYCbCr )
{
  static constexpr unsigned char kJpegAdobeT1[] = {
      0xFF, 0xD8,                  // SOI
      0xFF, 0xEE,                  // APP14
      0x00, 0x0E,                  // length = 14
      'A',  'd',  'o',  'b',  'e', // identifier
      0x00, 0x65,                  // version
      0x00, 0x00,                  // flags0
      0x00, 0x00,                  // flags1
      0x01,                        // transform = 1 (YCbCr)
      0xFF, 0xC0,                  // SOF0
      0x00, 0x11,                  // length = 17
      0x08,                        // sample precision
      0x00, 0x10, 0x00, 0x10,      // height=16, width=16
      0x03,                        // num_components
      0x01, 0x11, 0x00,            // C0: id=1, sampling=1:1, tq=0
      0x02, 0x11, 0x00,            // C1: id=2, sampling=1:1, tq=0
      0x03, 0x11, 0x00,            // C2: id=3, sampling=1:1, tq=0
  };
  GstCaps *caps = create_caps_for_compressed( "jpeg", kJpegAdobeT1, sizeof( kJpegAdobeT1 ), 30, 1 );
  ASSERT_NE( caps, nullptr );

  GstStructure *s = gst_caps_get_structure( caps, 0 );
  EXPECT_STREQ( gst_structure_get_string( s, "colorspace" ), "sYUV" );
  EXPECT_STREQ( gst_structure_get_string( s, "sampling" ), "YCbCr-4:4:4" );
  gint sof = -1;
  EXPECT_TRUE( gst_structure_get_int( s, "sof-marker", &sof ) );
  EXPECT_EQ( sof, 0 );

  gst_caps_unref( caps );
}

// Truncated JPEG with an APP0 length field that extends past the buffer must
// fall back to bare caps without over-reading. gst_jpeg_parse trusts the
// on-wire length, so without the bounds check the APP0 identifier read and
// SOF parser would access bytes past the ROS message buffer.
TEST_F( FormatConversionTest, JpegMalformedAppLengthFallsBackToBareCaps )
{
  static constexpr unsigned char kJpegTruncatedApp0[] = {
      0xFF, 0xD8, // SOI
      0xFF, 0xE0, // APP0
      0xFF, 0xFF, // length = 65535, far past buffer end
  };
  GstCaps *caps =
      create_caps_for_compressed( "jpeg", kJpegTruncatedApp0, sizeof( kJpegTruncatedApp0 ), 30, 1 );
  ASSERT_NE( caps, nullptr );

  GstStructure *s = gst_caps_get_structure( caps, 0 );
  EXPECT_STREQ( gst_structure_get_name( s ), "image/jpeg" );
  EXPECT_FALSE( gst_structure_has_field( s, "sof-marker" ) );
  EXPECT_FALSE( gst_structure_has_field( s, "width" ) );
  EXPECT_FALSE( gst_structure_has_field( s, "height" ) );

  gst_caps_unref( caps );
}

// SOF segment whose declared length extends past the buffer: without the
// bounds check, gst_jpeg_segment_parse_frame_header would read past `size`.
TEST_F( FormatConversionTest, JpegTruncatedSofFallsBackToBareCaps )
{
  static constexpr unsigned char kJpegTruncatedSof[] = {
      0xFF, 0xD8, // SOI
      0xFF, 0xC0, // SOF0
      0x00, 0x11, // length = 17, but buffer ends right here
  };
  GstCaps *caps =
      create_caps_for_compressed( "jpeg", kJpegTruncatedSof, sizeof( kJpegTruncatedSof ), 30, 1 );
  ASSERT_NE( caps, nullptr );

  GstStructure *s = gst_caps_get_structure( caps, 0 );
  EXPECT_STREQ( gst_structure_get_name( s ), "image/jpeg" );
  EXPECT_FALSE( gst_structure_has_field( s, "sof-marker" ) );
  EXPECT_FALSE( gst_structure_has_field( s, "width" ) );
  EXPECT_FALSE( gst_structure_has_field( s, "height" ) );

  gst_caps_unref( caps );
}

TEST_F( FormatConversionTest, PngEmitsWidthHeight )
{
  using namespace gstreamer_ros_babel_fish_test;
  GstCaps *caps = create_caps_for_compressed( "png", kPngRgb, sizeof( kPngRgb ), 30, 1 );
  ASSERT_NE( caps, nullptr );

  GstStructure *s = gst_caps_get_structure( caps, 0 );
  EXPECT_STREQ( gst_structure_get_name( s ), "image/png" );
  gint width = 0, height = 0;
  EXPECT_TRUE( gst_structure_get_int( s, "width", &width ) );
  EXPECT_TRUE( gst_structure_get_int( s, "height", &height ) );
  EXPECT_EQ( width, 8 );
  EXPECT_EQ( height, 8 );

  gst_caps_unref( caps );
}

TEST_F( FormatConversionTest, CompressedNoBufferStillEmitsBareCaps )
{
  // Compatibility with callers that don't have buffer bytes available: the
  // two-arg overload must still produce valid caps.
  GstCaps *caps = create_caps_for_compressed( "jpeg" );
  ASSERT_NE( caps, nullptr );
  GstStructure *s = gst_caps_get_structure( caps, 0 );
  EXPECT_FALSE( gst_structure_has_field( s, "sof-marker" ) );
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
