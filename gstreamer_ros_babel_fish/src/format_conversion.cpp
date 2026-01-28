/*
 *  gstreamer_ros_babel_fish - GStreamer ROS interface.
 *  Copyright (C) 2026  Stefan Fabian
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published
 *  by the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
#include "gstreamer_ros_babel_fish/format_conversion.hpp"
#include <sensor_msgs/image_encodings.hpp>
#include <unordered_map>

namespace gstreamer_ros_babel_fish
{

namespace enc = sensor_msgs::image_encodings;

// Mapping from GStreamer format to ROS encoding
static const std::unordered_map<GstVideoFormat, std::string> gst_to_ros_map = {
    { GST_VIDEO_FORMAT_RGB, enc::RGB8 },    { GST_VIDEO_FORMAT_BGR, enc::BGR8 },
    { GST_VIDEO_FORMAT_RGBA, enc::RGBA8 },  { GST_VIDEO_FORMAT_BGRA, enc::BGRA8 },
    { GST_VIDEO_FORMAT_GRAY8, enc::MONO8 }, { GST_VIDEO_FORMAT_GRAY16_LE, enc::MONO16 },
    { GST_VIDEO_FORMAT_UYVY, enc::UYVY },   { GST_VIDEO_FORMAT_YUY2, enc::YUYV },
    { GST_VIDEO_FORMAT_NV21, enc::NV21 },   { GST_VIDEO_FORMAT_NV24, enc::NV24 } };

// Mapping from ROS encoding to GStreamer format
static const std::unordered_map<std::string, GstVideoFormat> ros_to_gst_map = {
    { enc::RGB8, GST_VIDEO_FORMAT_RGB },
    { enc::BGR8, GST_VIDEO_FORMAT_BGR },
    { enc::RGBA8, GST_VIDEO_FORMAT_RGBA },
    { enc::BGRA8, GST_VIDEO_FORMAT_BGRA },
    { enc::MONO8, GST_VIDEO_FORMAT_GRAY8 },
    { enc::MONO16, GST_VIDEO_FORMAT_GRAY16_LE },
    { enc::UYVY, GST_VIDEO_FORMAT_UYVY },
    { enc::YUV422, GST_VIDEO_FORMAT_UYVY }, // deprecated alias
    { enc::YUYV, GST_VIDEO_FORMAT_YUY2 },
    { enc::YUV422_YUY2, GST_VIDEO_FORMAT_YUY2 }, // deprecated alias
    { enc::NV21, GST_VIDEO_FORMAT_NV21 },
    { enc::NV24, GST_VIDEO_FORMAT_NV24 },
    // OpenCV type aliases
    { enc::TYPE_8UC1, GST_VIDEO_FORMAT_GRAY8 },
    { enc::TYPE_8UC3, GST_VIDEO_FORMAT_RGB },  // Assume RGB for 3-channel
    { enc::TYPE_8UC4, GST_VIDEO_FORMAT_RGBA }, // Assume RGBA for 4-channel
    { enc::TYPE_16UC1, GST_VIDEO_FORMAT_GRAY16_LE },
};

std::optional<std::string> gst_format_to_ros_encoding( GstVideoFormat format )
{
  auto it = gst_to_ros_map.find( format );
  if ( it != gst_to_ros_map.end() ) {
    return it->second;
  }
  return std::nullopt;
}

std::optional<GstVideoFormat> ros_encoding_to_gst_format( const std::string &encoding )
{
  auto it = ros_to_gst_map.find( encoding );
  if ( it != ros_to_gst_map.end() ) {
    return it->second;
  }
  return std::nullopt;
}

int ros_encoding_bits_per_pixel( const std::string &encoding )
{
  try {
    int channels = enc::numChannels( encoding );
    int depth = enc::bitDepth( encoding );
    return channels * depth;
  } catch ( const std::runtime_error & ) {
    return 0;
  }
}

uint32_t calculate_ros_step( const std::string &encoding, uint32_t width )
{
  int bpp = ros_encoding_bits_per_pixel( encoding );
  return ( width * bpp + 7 ) / 8; // Round up to nearest byte
}

bool is_compressed_encoding( const std::string &encoding )
{
  // ROS CompressedImage format can be:
  // - Simple: "jpeg", "png"
  // - Extended: "<source_encoding>; <type> compressed <target_encoding>"
  //   e.g., "rgb8; jpeg compressed bgr8"
  return encoding.find( "jpeg" ) != std::string::npos ||
         encoding.find( "jpg" ) != std::string::npos ||
         encoding.find( "png" ) != std::string::npos || encoding == "compressed";
}

std::optional<std::string> caps_to_compression_format( const GstCaps *caps )
{
  if ( !caps || gst_caps_is_empty( caps ) ) {
    return std::nullopt;
  }

  GstStructure *structure = gst_caps_get_structure( caps, 0 );
  const gchar *media_type = gst_structure_get_name( structure );

  if ( g_strcmp0( media_type, "image/jpeg" ) == 0 ) {
    return "jpeg";
  } else if ( g_strcmp0( media_type, "image/png" ) == 0 ) {
    return "png";
  }

  return std::nullopt;
}

GstCaps *create_caps_from_ros_image( const std::string &encoding, uint32_t width, uint32_t height,
                                     uint32_t /* step */, int framerate_num, int framerate_den )
{
  auto gst_format = ros_encoding_to_gst_format( encoding );
  if ( !gst_format ) {
    return nullptr;
  }

  return gst_caps_new_simple(
      "video/x-raw", "format", G_TYPE_STRING, gst_video_format_to_string( *gst_format ), "width",
      G_TYPE_INT, static_cast<gint>( width ), "height", G_TYPE_INT, static_cast<gint>( height ),
      "framerate", GST_TYPE_FRACTION, framerate_num, framerate_den, nullptr );
}

GstCaps *create_caps_for_compressed( const std::string &format, int framerate_num, int framerate_den )
{
  // ROS CompressedImage format can be:
  // - Simple: "jpeg", "png"
  // - Extended: "<source_encoding>; <type> compressed <target_encoding>"
  //   e.g., "rgb8; jpeg compressed bgr8"
  GstCaps *caps = nullptr;
  if ( format.find( "jpeg" ) != std::string::npos || format.find( "jpg" ) != std::string::npos ) {
    caps = gst_caps_new_empty_simple( "image/jpeg" );
  } else if ( format.find( "png" ) != std::string::npos ) {
    caps = gst_caps_new_empty_simple( "image/png" );
  }

  if ( caps ) {
    gst_caps_set_simple( caps, "framerate", GST_TYPE_FRACTION, framerate_num, framerate_den, nullptr );
  }
  return caps;
}

GstCaps *get_supported_raw_caps()
{
  GstCaps *caps = gst_caps_new_empty();

  // Add all supported raw formats
  for ( const auto &pair : gst_to_ros_map ) {
    GstCaps *format_caps = gst_caps_new_simple(
        "video/x-raw", "format", G_TYPE_STRING, gst_video_format_to_string( pair.first ), "width",
        GST_TYPE_INT_RANGE, 1, G_MAXINT, "height", GST_TYPE_INT_RANGE, 1, G_MAXINT, "framerate",
        GST_TYPE_FRACTION_RANGE, 0, 1, G_MAXINT, 1, nullptr );
    gst_caps_append( caps, format_caps );
  }

  return caps;
}

GstCaps *get_supported_compressed_caps()
{
  GstCaps *caps = gst_caps_new_empty();

  // JPEG - no width/height constraints, decoder will parse from stream
  GstCaps *jpeg_caps = gst_caps_new_empty_simple( "image/jpeg" );
  gst_caps_append( caps, jpeg_caps );

  // PNG - no width/height constraints, decoder will parse from stream
  GstCaps *png_caps = gst_caps_new_empty_simple( "image/png" );
  gst_caps_append( caps, png_caps );

  return caps;
}

GstCaps *get_all_supported_caps()
{
  GstCaps *caps = get_supported_raw_caps();
  GstCaps *compressed = get_supported_compressed_caps();
  gst_caps_append( caps, compressed );
  return caps;
}

} // namespace gstreamer_ros_babel_fish
