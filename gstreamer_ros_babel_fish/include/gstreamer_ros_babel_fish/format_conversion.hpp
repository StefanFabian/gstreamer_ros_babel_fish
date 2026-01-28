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
#ifndef GSTREAMER_ROS_BABEL_FISH__FORMAT_CONVERSION_HPP_
#define GSTREAMER_ROS_BABEL_FISH__FORMAT_CONVERSION_HPP_

#include <cstdint>
#include <gst/gst.h>
#include <gst/video/video.h>
#include <optional>
#include <string>

namespace gstreamer_ros_babel_fish
{

struct FormatInfo {
  std::string ros_encoding;
  GstVideoFormat gst_format;
  int bits_per_pixel;
  int num_channels;
};

// Convert GStreamer video format to ROS encoding string
std::optional<std::string> gst_format_to_ros_encoding( GstVideoFormat format );

// Convert ROS encoding string to GStreamer video format
std::optional<GstVideoFormat> ros_encoding_to_gst_format( const std::string &encoding );

// Get bits per pixel for a ROS encoding
int ros_encoding_bits_per_pixel( const std::string &encoding );

// Get step (row stride) for a ROS image
uint32_t calculate_ros_step( const std::string &encoding, uint32_t width );

// Check if encoding is a compressed format
bool is_compressed_encoding( const std::string &encoding );

// Get compression format string from caps media type
std::optional<std::string> caps_to_compression_format( const GstCaps *caps );

// Create GStreamer caps from ROS image info
GstCaps *create_caps_from_ros_image( const std::string &encoding, uint32_t width, uint32_t height,
                                     uint32_t step, int framerate_num = 0, int framerate_den = 1 );

// Create GStreamer caps for compressed images
GstCaps *create_caps_for_compressed( const std::string &format, int framerate_num = 0,
                                     int framerate_den = 1 );

// Get supported raw video caps (for sink pad template)
GstCaps *get_supported_raw_caps();

// Get supported compressed caps (for sink pad template)
GstCaps *get_supported_compressed_caps();

// Get all supported caps (raw + compressed)
GstCaps *get_all_supported_caps();

} // namespace gstreamer_ros_babel_fish

#endif // GSTREAMER_ROS_BABEL_FISH__FORMAT_CONVERSION_HPP_
