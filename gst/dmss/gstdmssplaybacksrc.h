/* GStreamer
 * Copyright (C) <2018-2019> Felipe Magno de Almeida <felipe@expertisesolutions.com.br>
 *     Author: Felipe Magno de Almeida <felipe@expertisesolutions.com.br>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 51 Franklin St, Fifth Floor,
 * Boston, MA 02110-1301, USA.
 */

#ifndef __GST_DMSS_PLAYBACK_SRC_H__
#define __GST_DMSS_PLAYBACK_SRC_H__

#include <gst/gst.h>
#include <gst/base/gstpushsrc.h>

#include <gio/gio.h>

G_BEGIN_DECLS

#define GST_TYPE_DMSS_SRC                       \
  (gst_dmss_playback_src_get_type())
#define GST_DMSS_SRC(obj) \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),GST_TYPE_DMSS_SRC,GstDmssPlaybackSrc))
#define GST_DMSS_PLAYBACK_SRC_CLASS(klass) \
  (G_TYPE_CHECK_CLASS_CAST((klass),GST_TYPE_DMSS_SRC,GstDmssPlaybackSrcClass))
#define GST_IS_DMSS_SRC(obj) \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),GST_TYPE_DMSS_SRC))
#define GST_IS_DMSS_SRC_CLASS(klass) \
  (G_TYPE_CHECK_CLASS_TYPE((klass),GST_TYPE_DMSS_SRC))
typedef struct _GstDmssPlaybackSrc GstDmssPlaybackSrc;
typedef struct _GstDmssPlaybackSrcClass GstDmssPlaybackSrcClass;

typedef enum
{
  GST_DMSS_PLAYBACK_SRC_CONTROL_OPEN = (GST_BASE_SRC_FLAG_LAST << 0),

  GST_DMSS_PLAYBACK_SRC_FLAG_LAST = (GST_BASE_SRC_FLAG_LAST << 2)
} GstDmssPlaybackSrcFlags;

struct _GstDmssPlaybackSrc
{
  GstPushSrc element;

  /*< private > */
  int port;
  gchar *host;
  gchar *user;
  gchar *password;
  guint timeout;
  guint channel;
  guint subchannel;
  gint session_id;
  gchar connection_id[16];

  GSocket *control_socket;
  GSocket *stream_socket;
  GCancellable *cancellable;

  GArray *queued_buffer;
  GstClock *system_clock;
  GstClockTime last_ack_time;
#if 1
  unsigned int bytes_downloaded;
#endif
};

struct _GstDmssPlaybackSrcClass
{
  GstPushSrcClass parent_class;
};

GType gst_dmss_playback_src_get_type (void);

G_END_DECLS
#endif /* __GST_DMSS_PLAYBACK_SRC_H__ */
