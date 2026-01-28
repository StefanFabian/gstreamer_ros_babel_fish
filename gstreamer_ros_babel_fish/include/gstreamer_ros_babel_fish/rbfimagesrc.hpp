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
#ifndef GSTREAMER_ROS_BABEL_FISH__RBFIMAGESRC_HPP_
#define GSTREAMER_ROS_BABEL_FISH__RBFIMAGESRC_HPP_

#include <gst/base/gstpushsrc.h>
#include <gst/gst.h>
#include <gst/video/video.h>

G_BEGIN_DECLS

#define RBF_TYPE_IMAGE_SRC ( rbf_image_src_get_type() )
#define RBF_IMAGE_SRC( obj )                                                                       \
  ( G_TYPE_CHECK_INSTANCE_CAST( ( obj ), RBF_TYPE_IMAGE_SRC, RbfImageSrc ) )
#define RBF_IMAGE_SRC_CLASS( klass )                                                               \
  ( G_TYPE_CHECK_CLASS_CAST( ( klass ), RBF_TYPE_IMAGE_SRC, RbfImageSrcClass ) )
#define RBF_IS_IMAGE_SRC( obj ) ( G_TYPE_CHECK_INSTANCE_TYPE( ( obj ), RBF_TYPE_IMAGE_SRC ) )
#define RBF_IS_IMAGE_SRC_CLASS( klass ) ( G_TYPE_CHECK_CLASS_TYPE( ( klass ), RBF_TYPE_IMAGE_SRC ) )

typedef struct _RbfImageSrc RbfImageSrc;
typedef struct _RbfImageSrcClass RbfImageSrcClass;
typedef struct _RbfImageSrcPrivate RbfImageSrcPrivate;

struct _RbfImageSrc {
  GstPushSrc parent;
  RbfImageSrcPrivate *priv;
};

struct _RbfImageSrcClass {
  GstPushSrcClass parent_class;
};

GType rbf_image_src_get_type( void );

// Plugin registration helper
gboolean rbf_image_src_plugin_init( GstPlugin *plugin );

G_END_DECLS

#endif // GSTREAMER_ROS_BABEL_FISH__RBFIMAGESRC_HPP_
