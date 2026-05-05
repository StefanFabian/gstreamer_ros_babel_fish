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
#include "gstreamer_ros_babel_fish/compressed_header_parser.hpp"

// gst_jpeg_parser.h is marked unstable, but the marker-walk + SOF parsing
// surface used here is stable across the gst-codecparsers versions this
// package supports.
#define GST_USE_UNSTABLE_API
#include <gst/codecparsers/gstjpegparser.h>

namespace gstreamer_ros_babel_fish
{

namespace
{

// Sampling-factor pattern for 3 YCbCr components (Y subsampled vs Cb/Cr 1:1).
const char *ycbcr_sampling_string( const GstJpegFrameComponent &y, const GstJpegFrameComponent &cb,
                                   const GstJpegFrameComponent &cr )
{
  if ( cb.horizontal_factor != 1 || cb.vertical_factor != 1 || cr.horizontal_factor != 1 ||
       cr.vertical_factor != 1 ) {
    return nullptr; // unusual; let fallback handle
  }
  if ( y.horizontal_factor == 2 && y.vertical_factor == 2 )
    return "YCbCr-4:2:0";
  if ( y.horizontal_factor == 2 && y.vertical_factor == 1 )
    return "YCbCr-4:2:2";
  if ( y.horizontal_factor == 1 && y.vertical_factor == 1 )
    return "YCbCr-4:4:4";
  return nullptr;
}

} // namespace

JpegHeaderInfo parse_jpeg_header( const uint8_t *data, size_t size )
{
  JpegHeaderInfo info;
  if ( !data || size < 4 )
    return info;

  bool have_jfif = false;
  int adobe_transform = -1; // -1 = no APP14, 0/1/2 = transform values
  GstJpegFrameHdr frame{};
  bool have_sof = false;
  GstJpegMarker sof_marker = GST_JPEG_MARKER_SOF0;

  GstJpegSegment seg{};
  guint offset = 0;
  while ( gst_jpeg_parse( &seg, data, size, offset ) ) {
    // Advance past this segment for the next call. seg.size includes the
    // 2-byte length but excludes the sync byte and marker code; gst_jpeg_parse
    // sets seg.offset to immediately after the marker code, so seg.offset +
    // seg.size lands right after the segment payload.
    offset = seg.offset + ( seg.size > 0 ? static_cast<guint>( seg.size ) : 0 );

    // gst_jpeg_parse trusts the on-wire length field, so a truncated or
    // malformed JPEG can produce a segment whose declared payload extends past
    // `size`. Reject those before any APP read or helper-parser call below
    // dereferences past the buffer.
    if ( seg.size > 0 && static_cast<size_t>( seg.offset + seg.size ) > size ) {
      break;
    }

    GstJpegMarker m = seg.marker;
    // SOS / EOI: end of header section.
    if ( m == GST_JPEG_MARKER_SOS || m == GST_JPEG_MARKER_EOI )
      break;

    // SOF (excluding DHT 0xC4 and DAC 0xCC, which fall in the SOFn numeric range).
    if ( m >= GST_JPEG_MARKER_SOF_MIN && m <= GST_JPEG_MARKER_SOF_MAX && m != GST_JPEG_MARKER_DHT &&
         m != GST_JPEG_MARKER_DAC ) {
      if ( gst_jpeg_segment_parse_frame_header( &seg, &frame ) ) {
        have_sof = true;
        sof_marker = m;
      }
      // Header section ends at SOF for our purposes; APP markers come before.
      break;
    }

    // seg.offset points past the marker code; the first 2 bytes of the
    // segment are the big-endian length, so the identifier payload begins
    // at +2.
    if ( m == GST_JPEG_MARKER_APP0 && seg.size >= 7 ) {
      const uint8_t *p = seg.data + seg.offset + 2;
      if ( p[0] == 'J' && p[1] == 'F' && p[2] == 'I' && p[3] == 'F' && p[4] == 0 ) {
        have_jfif = true;
      }
    }
    // Adobe APP14: "Adobe" + Version(2) + Flags0(2) + Flags1(2) + Transform(1).
    // With the +2 length prefix accounted for, transform is at +13.
    if ( m == GST_JPEG_MARKER_APP14 && seg.size >= 14 ) {
      const uint8_t *p = seg.data + seg.offset + 2;
      if ( p[0] == 'A' && p[1] == 'd' && p[2] == 'o' && p[3] == 'b' && p[4] == 'e' ) {
        adobe_transform = p[11];
      }
    }
  }

  if ( !have_sof )
    return info;

  info.width = frame.width;
  info.height = frame.height;
  info.sof_marker = static_cast<int>( sof_marker ) - 0xC0;
  info.dims_valid = info.width > 0 && info.height > 0;

  // Colorspace/sampling are derivable for any SOFn; the sof-marker field on
  // caps lets strict decoders self-exclude during negotiation.

  if ( frame.num_components == 1 ) {
    info.colorspace = "GRAY";
    info.sampling = "GRAYSCALE";
    info.valid = true;
    return info;
  }

  if ( frame.num_components == 3 ) {
    const auto &c0 = frame.components[0];
    const auto &c1 = frame.components[1];
    const auto &c2 = frame.components[2];
    auto rgb_ids = [&]() {
      return c0.identifier == 'R' && c1.identifier == 'G' && c2.identifier == 'B';
    };
    auto bgr_ids = [&]() {
      return c0.identifier == 'B' && c1.identifier == 'G' && c2.identifier == 'R';
    };

    // Adobe APP14 takes precedence: transform=1 → YCbCr, transform=0 → RGB/CMYK.
    if ( adobe_transform == 1 ) {
      const char *s = ycbcr_sampling_string( c0, c1, c2 );
      if ( !s )
        return info;
      info.colorspace = "sYUV";
      info.sampling = s;
      info.valid = true;
      return info;
    }
    if ( adobe_transform == 0 ) {
      if ( rgb_ids() ) {
        info.colorspace = "sRGB";
        info.sampling = "RGB";
        info.valid = true;
      } else if ( bgr_ids() ) {
        info.colorspace = "sRGB";
        info.sampling = "BGR";
        info.valid = true;
      }
      return info;
    }

    // No Adobe marker: JFIF guarantees YCbCr.
    if ( have_jfif ) {
      const char *s = ycbcr_sampling_string( c0, c1, c2 );
      if ( !s )
        return info;
      info.colorspace = "sYUV";
      info.sampling = s;
      info.valid = true;
      return info;
    }

    // No JFIF either: rely on component IDs (some encoders strip standard markers).
    if ( rgb_ids() ) {
      info.colorspace = "sRGB";
      info.sampling = "RGB";
      info.valid = true;
      return info;
    }
    if ( bgr_ids() ) {
      info.colorspace = "sRGB";
      info.sampling = "BGR";
      info.valid = true;
      return info;
    }

    // Plain numeric IDs (1,2,3) or anything else: assume YCbCr per ITU T.81.
    const char *s = ycbcr_sampling_string( c0, c1, c2 );
    if ( !s )
      return info;
    info.colorspace = "sYUV";
    info.sampling = s;
    info.valid = true;
    return info;
  }

  // 4 components (CMYK / YCCK) are not handled.
  return info;
}

PngHeaderInfo parse_png_header( const uint8_t *data, size_t size )
{
  // PNG signature is 8 bytes, then a chunk: 4-byte length BE, 4-byte type
  // "IHDR", then 4-byte width BE + 4-byte height BE.
  PngHeaderInfo info;
  static const uint8_t kSignature[8] = { 0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A };
  if ( !data || size < 24 )
    return info;
  for ( int i = 0; i < 8; ++i ) {
    if ( data[i] != kSignature[i] )
      return info;
  }
  if ( data[12] != 'I' || data[13] != 'H' || data[14] != 'D' || data[15] != 'R' )
    return info;
  auto read_be32 = [&]( size_t off ) {
    return ( static_cast<uint32_t>( data[off] ) << 24 ) |
           ( static_cast<uint32_t>( data[off + 1] ) << 16 ) |
           ( static_cast<uint32_t>( data[off + 2] ) << 8 ) | static_cast<uint32_t>( data[off + 3] );
  };
  info.width = static_cast<int>( read_be32( 16 ) );
  info.height = static_cast<int>( read_be32( 20 ) );
  info.valid = info.width > 0 && info.height > 0;
  return info;
}

} // namespace gstreamer_ros_babel_fish
