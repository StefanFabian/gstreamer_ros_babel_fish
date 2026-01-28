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
#include "gstreamer_ros_babel_fish/rbfimagesink.hpp"
#include "gstreamer_ros_babel_fish/rbfimagesrc.hpp"
#include <gst/gst.h>

static gboolean plugin_init( GstPlugin *plugin )
{
  gboolean ret = TRUE;

  ret &= rbf_image_sink_plugin_init( plugin );
  ret &= rbf_image_src_plugin_init( plugin );

  return ret;
}

#define PACKAGE "gstreamer_ros_babel_fish"
#define VERSION "0.1.0"

GST_PLUGIN_DEFINE( GST_VERSION_MAJOR, GST_VERSION_MINOR, rosbabelfish,
                   "ROS 2 image bridge elements for GStreamer", plugin_init, VERSION, "LGPL",
                   PACKAGE, "https://github.com/example/gstreamer_ros_babel_fish" )
