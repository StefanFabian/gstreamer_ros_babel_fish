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
#ifndef GSTREAMER_ROS_BABEL_FISH__RBFIMAGESINK_HPP_
#define GSTREAMER_ROS_BABEL_FISH__RBFIMAGESINK_HPP_

#include <gst/base/gstbasesink.h>
#include <gst/gst.h>
#include <gst/video/video.h>

G_BEGIN_DECLS

#define RBF_TYPE_IMAGE_SINK ( rbf_image_sink_get_type() )
#define RBF_IMAGE_SINK( obj )                                                                      \
  ( G_TYPE_CHECK_INSTANCE_CAST( ( obj ), RBF_TYPE_IMAGE_SINK, RbfImageSink ) )
#define RBF_IMAGE_SINK_CLASS( klass )                                                              \
  ( G_TYPE_CHECK_CLASS_CAST( ( klass ), RBF_TYPE_IMAGE_SINK, RbfImageSinkClass ) )
#define RBF_IS_IMAGE_SINK( obj ) ( G_TYPE_CHECK_INSTANCE_TYPE( ( obj ), RBF_TYPE_IMAGE_SINK ) )
#define RBF_IS_IMAGE_SINK_CLASS( klass )                                                           \
  ( G_TYPE_CHECK_CLASS_TYPE( ( klass ), RBF_TYPE_IMAGE_SINK ) )

typedef struct _RbfImageSink RbfImageSink;
typedef struct _RbfImageSinkClass RbfImageSinkClass;
typedef struct _RbfImageSinkPrivate RbfImageSinkPrivate;

struct _RbfImageSink {
  GstBaseSink parent;
  RbfImageSinkPrivate *priv;
};

struct _RbfImageSinkClass {
  GstBaseSinkClass parent_class;
};

GType rbf_image_sink_get_type( void );

// Plugin registration helper
gboolean rbf_image_sink_plugin_init( GstPlugin *plugin );

G_END_DECLS

#endif // GSTREAMER_ROS_BABEL_FISH__RBFIMAGESINK_HPP_
