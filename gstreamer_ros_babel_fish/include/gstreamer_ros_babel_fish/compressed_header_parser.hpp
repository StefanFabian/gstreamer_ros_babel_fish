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
#ifndef GSTREAMER_ROS_BABEL_FISH_COMPRESSED_HEADER_PARSER_HPP_
#define GSTREAMER_ROS_BABEL_FISH_COMPRESSED_HEADER_PARSER_HPP_

#include <cstddef>
#include <cstdint>

namespace gstreamer_ros_babel_fish
{

// Result of parsing a JPEG header.
//   dims_valid is true once a SOF marker was found and width/height were read.
//   valid is true once a colorspace/sampling combination was identified from the
//   SOF + APP markers. Fields are still emitted for non-baseline JPEGs so
//   downstream caps negotiation can route to the appropriate decoder via
//   sof_marker.
struct JpegHeaderInfo {
  bool dims_valid = false;
  bool valid = false;
  int width = 0;
  int height = 0;
  int sof_marker = 0;          // marker - 0xC0
  const char *colorspace = ""; // "sYUV" | "GRAY" | "sRGB"
  const char *sampling = "";   // "YCbCr-4:2:0" | ... | "GRAYSCALE" | "RGB" | "BGR"
};

struct PngHeaderInfo {
  bool valid = false;
  int width = 0;
  int height = 0;
};

//! Walks JPEG segments looking for SOF0 + (optional) APP0/JFIF and APP14/Adobe
//! markers. Tolerant of malformed or non-baseline input - returns an
//! info struct with valid=false in that case.
JpegHeaderInfo parse_jpeg_header( const uint8_t *data, size_t size );

//! Reads a PNG IHDR chunk for width/height. Validates the 8-byte signature
//! before reading.
PngHeaderInfo parse_png_header( const uint8_t *data, size_t size );

} // namespace gstreamer_ros_babel_fish

#endif // GSTREAMER_ROS_BABEL_FISH_COMPRESSED_HEADER_PARSER_HPP_
