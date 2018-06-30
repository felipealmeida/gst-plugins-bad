/* GStreamer
 * Copyright (C) <2018> Felipe Magno de Almeida <felipe@expertisesolutions.com.br>
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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <gst/gst-i18n-plugin.h>
#include <gst/gst.h>
#include <gio/gio.h>
#include "gstdmssdemux.h"
#include "gstdmss.h"

GST_DEBUG_CATEGORY_STATIC (dmssdemux_debug);
#define GST_CAT_DEFAULT dmssdemux_debug

#define VIDEO_CAPS \
  GST_STATIC_CAPS (\
    "video/x-h264, stream-format=(string)byte-stream," \
      " alignment=(string)nal;" \
    "video/x-h265, stream-format=(string)byte-stream," \
      " alignment=(string)nal;" \
    "video/mpeg, " \
      "mpegversion = (int) 4; " \
)

#define AUDIO_CAPS \
  GST_STATIC_CAPS (\
    "audio/x-alaw, " \
      "rate = (int) [ 8000, 16000 ], channels = (int) [ 1, 2 ];" \
    "audio/x-mulaw, " \
      "rate = [ 8000, 16000 ], channels = [ 1, 2 ];" \
    "audio/mpeg, " \
      "mpegversion = (int) 4, " \
      "stream-format = (string) adts; " \
)

static GstStaticPadTemplate sink_template =
GST_STATIC_PAD_TEMPLATE ("sink",
    GST_PAD_SINK,
    GST_PAD_ALWAYS,
    GST_STATIC_CAPS ("application/x-dmss"));

static GstStaticPadTemplate video_template =
GST_STATIC_PAD_TEMPLATE ("video", GST_PAD_SRC,
    GST_PAD_SOMETIMES,
    VIDEO_CAPS);

static GstStaticPadTemplate audio_template =
GST_STATIC_PAD_TEMPLATE ("audio",
    GST_PAD_SRC,
    GST_PAD_SOMETIMES,
    AUDIO_CAPS);

#define gst_dmss_demux_parent_class parent_class
G_DEFINE_TYPE (GstDmssDemux, gst_dmss_demux, GST_TYPE_ELEMENT);

static void gst_dmss_demux_finalize (GObject * object);

static GstPad* gst_dmss_demux_add_audio_pad (GstDmssDemux * demux, GstCaps * caps);
static GstPad* gst_dmss_demux_add_video_pad (GstDmssDemux * demux, GstCaps * caps);

/* query functions */
static gboolean gst_dmss_demux_src_query (GstPad * pad, GstObject * parent,
    GstQuery * query);
static gboolean gst_dmss_demux_sink_query (GstPad * pad, GstObject * parent,
    GstQuery * query);

static gboolean gst_dmss_demux_handle_src_event (GstPad * pad, GstObject * parent,
    GstEvent * event);
static gboolean gst_dmss_demux_handle_sink_event (GstPad * pad, GstObject * parent,
    GstEvent * event);
static gboolean gst_dmss_demux_send_event (GstElement * element, GstEvent * event);

static GstFlowReturn gst_dmss_demux_chain (GstPad * pad, GstObject * parent,
    GstBuffer * buffer);

static gboolean gst_dmss_demux_sink_activate (GstPad * sinkpad,
    GstObject * parent);
static GstStateChangeReturn gst_dmss_demux_change_state (GstElement * element,
    GstStateChange transition);

static void
gst_dmss_demux_class_init (GstDmssDemuxClass * klass)
{
  GObjectClass *gobject_class;
  GstElementClass *gstelement_class;

  gobject_class = G_OBJECT_CLASS (klass);
  gstelement_class = GST_ELEMENT_CLASS (klass);

  gobject_class->finalize = gst_dmss_demux_finalize;

  gstelement_class->change_state = GST_DEBUG_FUNCPTR (gst_dmss_demux_change_state);
  gstelement_class->send_event = GST_DEBUG_FUNCPTR (gst_dmss_demux_send_event);
  
  gst_element_class_add_pad_template (gstelement_class,
      gst_static_pad_template_get (&sink_template));
  gst_element_class_add_pad_template (gstelement_class,
      gst_static_pad_template_get (&video_template));
  gst_element_class_add_pad_template (gstelement_class,
      gst_static_pad_template_get (&audio_template));

  gst_element_class_set_metadata(gstelement_class,
    "DMSS demuxer",
    "Demuxer for IP Camera",
    "Receive data from IP camera",
    "Felipe Magno de Almeida <felipe@expertisesolutions.com.br>");

  GST_DEBUG_CATEGORY_INIT (dmssdemux_debug, "dmssdemux", 0,
      "DMSS Demux");
}

static gboolean
gst_dmss_demux_push_event (GstDmssDemux * demux, GstEvent * event)
{
  gboolean res = FALSE;

  if (demux->videosrcpad) {
    gst_event_ref (event);
    res |= gst_pad_push_event (demux->videosrcpad, event);
  }

  if (demux->audiosrcpad)
    res |= gst_pad_push_event (demux->audiosrcpad, event);
  else {
    gst_event_unref (event);
    res = TRUE;
  }

  return res;
}

static GstClockTime
gst_dmss_demux_segment_init (GstDmssDemux *demux, guint16 epoch, guint16 ts)
{
  GstEvent *event;
  GstClockTime timestamp;
  
  g_assert (demux->need_segment);

  gst_segment_init (&demux->time_segment, GST_FORMAT_TIME);

  timestamp = epoch;
  timestamp *= 1000ull*1000ull*1000ull;
  timestamp += (((guint64)ts) % 1000) * 1000ull*1000ull;

  demux->time_segment.start = demux->time_segment.position = timestamp;

  event = gst_event_new_segment (&demux->time_segment);
  gst_dmss_demux_push_event (demux, event);
  demux->need_segment = FALSE;

  return timestamp;
}

static guint64
gst_dmss_demux_find_extended_header_value (guint8 prefix, guint64 extended_header[32])
{
  int i;
  gint64 v = -1;
  
  for (i = 0; i != 32; ++i)
  {
    if (prefix != 0x88 || prefix != 0x82)
    {
      guint8 pv = (extended_header[i] & 0xFF000000) >> 24;
      if (pv == prefix)
      {
        v = extended_header[i] & 0xFFFFFF;
        break;
      }
      else if (!pv)
        break;
    }
    else
    {
      guint8 pv = (extended_header[i] & 0xFF00000000000000) >> 56;
      if (pv == prefix)
      {
        v = extended_header[i] & 0xFFFFFFFFFFFFFF;
        break;
      }
      else if (!pv)
        break;
    }
  }

  return v;
}

static void
gst_dmss_demux_audio_prepare_buffer (GstDmssDemux* demux, GstBuffer* buffer, guint64 extended_header[32])
{
  GstDmssAudioFormat format;
  GstDmssAudioRate rate;
  guint64 e83;
  GstCaps* caps;
  int rate_num;

  e83 = gst_dmss_demux_find_extended_header_value (0x83, extended_header);

  format = (e83 & 0xFF00) >> 8;
  rate = e83 & 0xFF;
  if (!demux->audiosrcpad && (format != demux->audio_format || rate != demux->audio_rate))
  {
    switch (rate)
    {
    case GST_DMSS_AUDIO_8000:
      rate_num = 8000;
      break;
    case GST_DMSS_AUDIO_16000:
      rate_num = 16000;
      break;  
    default:
      GST_ELEMENT_ERROR (demux, RESOURCE, READ, (NULL),
         ("Unknown audio rate: %d", (int)rate));
      break;
    }
    
    switch (format)
    {
    case GST_DMSS_AUDIO_ALAW:
      caps = gst_caps_new_simple ("audio/x-alaw", "rate", G_TYPE_INT, rate_num, "channels", G_TYPE_INT, 1, NULL);
      break;
    case GST_DMSS_AUDIO_MULAW:
      caps = gst_caps_new_simple ("audio/x-mulaw", "rate", G_TYPE_INT, rate_num, "channels", G_TYPE_INT, 1, NULL);
      break;
    case GST_DMSS_AUDIO_G726:
      caps = gst_caps_new_simple ("audio/x-g726", "rate", G_TYPE_INT, rate_num, "channels", G_TYPE_INT, 1, NULL);
      break;
    case GST_DMSS_AUDIO_AAC:
      caps = gst_caps_new_simple ("audio/mpeg", "mpegversion", G_TYPE_INT, 4, "stream-format", G_TYPE_STRING, "adts", NULL);
      break;
    default:
      GST_ELEMENT_ERROR (demux, RESOURCE, READ, (NULL),
         ("Unknown audio format: %d", (int)format));
      break;
    }

    gst_dmss_demux_add_audio_pad (demux, caps);

    demux->audio_format = format;
    demux->audio_rate = rate;
  }
}

static void
gst_dmss_demux_video_prepare_buffer (GstDmssDemux* demux, GstBuffer* buffer, guint64 extended_header[32])
{
  GstDmssVideoFormat format;
  guint32 e81;
  GstCaps* caps;

  e81 = gst_dmss_demux_find_extended_header_value (0x81, extended_header);

  format = (e81 & 0xFF00) >> 8;
  if (!demux->videosrcpad && format != demux->video_format)
  {
    switch (format)
    {
    case GST_DMSS_VIDEO_H264:
      caps = gst_caps_new_simple ("video/x-h264", "stream-format", G_TYPE_STRING, "byte-stream", "alignment", G_TYPE_STRING, "nal", NULL);
      break;
    case GST_DMSS_VIDEO_H265:
      caps = gst_caps_new_simple ("video/x-h265", "stream-format", G_TYPE_STRING, "byte-stream", "alignment", G_TYPE_STRING, "nal", NULL);
      break;
    default:
      GST_ELEMENT_ERROR (demux, RESOURCE, READ, (NULL),
         ("Unknown Video format: %d", (int)format));
      break;
    }

    gst_dmss_demux_add_video_pad (demux, caps);

    demux->video_format = format;
  }  
}

static void
gst_dmss_demux_parse_extended_header (GstDmssDemux* demux, gchar* header, int size, guint64 extended_header[32])
{
  gchar *p = header, *end = header + size;
  int i = 0;
  guint32 copy32; // because of alignment
  
  while (p != end && i != 32)
  {
    GST_DEBUG ("reading extended header of %.02x", (unsigned int)(guint8)*p);
    
    if (end - p >= sizeof(guint32))
    {
      memcpy(&copy32, p, sizeof(copy32));
      extended_header[i] = GUINT32_FROM_BE(copy32);
      p += sizeof(guint32);
    }
    else
    {
      GST_ELEMENT_WARNING (demux, RESOURCE, READ, (NULL),
                           ("Couldn't parse extended header correctly"));
      break;
    }
    GST_DEBUG ("Value read for header %" G_GUINT64_FORMAT, extended_header[i]);
    ++i;
  }
}

static void
gst_dmss_demux_flush (GstDmssDemux *demux)
{
  int const prologue_size = 32;
  int const dhav_fixed_header_size = 24;
  int const dhav_epilogue_size = 8;
  guint64 extended_header[32];
  gsize size;
  GstBuffer *buffer;
  GstMapInfo map;
  guint8 dhav_packet_type;
  guint32 dhav_packet_size;
  guint32 dhav_head_size;
  guint32 dhav_body_size;
  gchar const *prologue;
  guint32 minimum_dhav_size = dhav_fixed_header_size + dhav_epilogue_size;
  guint16 frame_epoch;
  guint16 frame_ts, ring_diff_ts, reverse_ring_diff_ts;
  int diff_ts;
  GstClockTime absolute_timestamp;
  static int frame;
  gboolean is_audio;
  gchar const* error_msg;
  int start_offset;

  size = gst_adapter_available (demux->adapter);

  while (size >= prologue_size + minimum_dhav_size)
  {
    prologue = gst_adapter_map (demux->adapter, prologue_size + minimum_dhav_size);
    start_offset = 0;

    while ((prologue[prologue_size + start_offset + 0] != 'D' ||
            prologue[prologue_size + start_offset + 1] != 'H' ||
            prologue[prologue_size + start_offset + 2] != 'A' ||
            prologue[prologue_size + start_offset + 3] != 'V') &&
           size != prologue_size + minimum_dhav_size + start_offset)
    {
      ++start_offset;

      gst_adapter_unmap (demux->adapter);
      prologue = gst_adapter_map (demux->adapter, prologue_size + minimum_dhav_size + start_offset);
    }

    if (size == prologue_size + minimum_dhav_size + start_offset)
      goto prefix_error;

    prologue += start_offset;
    dhav_packet_type = prologue[prologue_size + 4];

    dhav_packet_size = GUINT32_FROM_LE(*(guint32*)&prologue[prologue_size + 12]);
    dhav_head_size = *(unsigned char*)&prologue[prologue_size + 22];
    dhav_body_size = dhav_packet_size - (dhav_fixed_header_size + dhav_epilogue_size + dhav_head_size);

    gst_adapter_unmap (demux->adapter);

    if (start_offset)
    {
      GST_DEBUG ("Packet didn't start at right offset. Skipped %d bytes", start_offset);
      gst_adapter_flush (demux->adapter, start_offset);
    }

    GST_DEBUG("DHAV packet (checking if downloaded) type: %.02x DHAV size: %d head size: %d body size: %d", (int)dhav_packet_type,
              (int)dhav_packet_size, (int)dhav_head_size, (int)dhav_body_size);
    
    is_audio = (dhav_packet_type == (unsigned char)0xf0);

    if (is_audio)
      GST_WARNING ("DHAV audio packet");

    if(!is_audio &&
       dhav_packet_type != (unsigned char)0xfc &&
       dhav_packet_type != (unsigned char)0xfd)
    {
      /* discard packet */
      GST_WARNING ("Discarding DHAV packet that is not video frame");
      gst_adapter_flush (demux->adapter, dhav_packet_size + prologue_size);
      continue;
    }

    if (dhav_packet_size + prologue_size <= size)
    {
      GST_DEBUG("DHAV packet fully downloaded (size downloaded: %d, packet size + prologue: %d)", (int)size, (int)dhav_packet_size + prologue_size);
      buffer = gst_adapter_take_buffer (demux->adapter, dhav_packet_size + prologue_size);

      gst_buffer_map (buffer, &map, GST_MAP_READ);
    
      if (map.data[prologue_size + dhav_packet_size - 8] != 'd' ||
          map.data[prologue_size + dhav_packet_size - 7] != 'h' ||
          map.data[prologue_size + dhav_packet_size - 6] != 'a' ||
          map.data[prologue_size + dhav_packet_size - 5] != 'v')
      {
        error_msg = "Packet doesn't end with dhav suffix";
        goto corrupted_error;
      }

      if (GUINT32_FROM_LE(*(guint32*)&map.data[prologue_size + dhav_packet_size - 4]) != dhav_packet_size)
      {
        error_msg = "Packet suffixed size doesn't match header packet size";
        goto corrupted_error;
      }

      frame_epoch = GUINT16_FROM_LE(*(guint16*)&prologue[prologue_size + 16]);
      frame_ts = GUINT16_FROM_LE(*(guint16*)&prologue[prologue_size + 20]);

      GST_INFO ("DHAV frame timing info epoch: %d timestamp: %d", (int)frame_epoch, (int)frame_ts);

      gst_dmss_demux_parse_extended_header (demux, (gchar*)map.data + prologue_size + dhav_fixed_header_size, dhav_head_size, extended_header);

      if (is_audio)
        gst_dmss_demux_audio_prepare_buffer (demux, buffer, extended_header);
      else
        gst_dmss_demux_video_prepare_buffer (demux, buffer, extended_header);
      
      gst_buffer_unmap (buffer, &map);

      if (demux->need_segment)
      {
        demux->last_timestamp = absolute_timestamp = gst_dmss_demux_segment_init(demux, frame_epoch, frame_ts);
        /* demux->last_epoch = frame_epoch; */
        demux->last_ts = frame_ts;
      }
      else
      {
        if (!frame_epoch && !frame_ts)
        {
          GST_ERROR ("Timing information in DHAV packet is zeroed. Let's fill with last information");
          frame_ts = demux->last_ts;
        }

        ring_diff_ts = frame_ts - demux->last_ts;
        reverse_ring_diff_ts = demux->last_ts - frame_ts;

        if (ring_diff_ts <= 100)
          diff_ts = ring_diff_ts;
        else if (reverse_ring_diff_ts <= 100)
          diff_ts = -(int)reverse_ring_diff_ts;        
        else
          GST_ERROR ("Timing information in DHAV packet is wrong!");

        absolute_timestamp = demux->last_timestamp;
        absolute_timestamp += (guint64)diff_ts * 1000ull*1000ull;

        demux->last_ts = frame_ts;

        if((absolute_timestamp >= demux->last_timestamp &&
            absolute_timestamp - demux->last_timestamp > 100*GST_MSECOND)
           || (absolute_timestamp < demux->last_timestamp &&
               demux->last_timestamp - absolute_timestamp > 100*GST_MSECOND))
        {
          GST_ERROR ("Diff %" G_GUINT64_FORMAT " Last timestamp: %" GST_TIME_FORMAT " and new timestamp %" GST_TIME_FORMAT, (absolute_timestamp - demux->last_timestamp), GST_TIME_ARGS (demux->last_timestamp), GST_TIME_ARGS(absolute_timestamp));
        }
        
        g_assert ((absolute_timestamp >= demux->last_timestamp &&
                   absolute_timestamp - demux->last_timestamp <= 100*GST_MSECOND)
                  || (absolute_timestamp < demux->last_timestamp &&
                      demux->last_timestamp - absolute_timestamp <= 100*GST_MSECOND));
        
        demux->last_timestamp = absolute_timestamp;
      }

      GST_INFO ("DHAV relative %" G_GUINT64_FORMAT ", timestamp: %" GST_TIME_FORMAT, absolute_timestamp, GST_TIME_ARGS (absolute_timestamp));
      
      GST_DEBUG ("Resizing buffer to offset %d and size %d", (int)prologue_size + dhav_head_size, (int)dhav_body_size);
      gst_buffer_resize (buffer, prologue_size + dhav_head_size + dhav_fixed_header_size, dhav_body_size);

      /* GST_BUFFER_TIMESTAMP (buffer) = absolute_timestamp; */
      GST_BUFFER_PTS (buffer) = absolute_timestamp;
      /* GST_BUFFER_DTS (buffer) = absolute_timestamp; */
      /* GST_BUFFER_DURATION (buffer) = 30; */
        
      GST_LOG_OBJECT (demux,
                      "%s buffer of size %" G_GSIZE_FORMAT ", ts %"
        GST_TIME_FORMAT /*", dur %" GST_TIME_FORMAT*/
        ", offset %" G_GINT64_FORMAT ", offset_end %" G_GINT64_FORMAT,
        (!is_audio ? "Video" : "Audio") ,
        gst_buffer_get_size (buffer),
        GST_TIME_ARGS (GST_BUFFER_TIMESTAMP (buffer)),
        /* GST_TIME_ARGS (GST_BUFFER_DURATION (buffer)), */
        GST_BUFFER_OFFSET (buffer), GST_BUFFER_OFFSET_END (buffer));

      GST_INFO ("Frame number %d", frame++);

      if (is_audio)
      {
        /* if (!demux->audiosrcpad) */
        /* { */
        /* } */
        if (demux->audiosrcpad)
        {
          GST_DEBUG ("pushed audio buffer");
          gst_pad_push (demux->audiosrcpad, buffer);
        }
      }
      else
        gst_pad_push (demux->videosrcpad, buffer);

      demux->waiting_dhav_end = FALSE;
      size = gst_adapter_available (demux->adapter);
    }
    else
    {
      demux->waiting_dhav_end = TRUE;
      GST_DEBUG("Needs to download more to complete DHAV packet");
      break;
    }
  }

  return;
prefix_error:
  GST_ELEMENT_WARNING (demux, RESOURCE, READ, (NULL),
                       ("DHAV packet doesn't start with the correct bytes"));
  gst_adapter_unmap(demux->adapter);
  gst_adapter_clear(demux->adapter);
  return;
corrupted_error:
  GST_ELEMENT_WARNING (demux, RESOURCE, READ, (NULL),
                       ("DHAV packet is corrupted: %s", error_msg));
  gst_object_unref (buffer);
  gst_buffer_unmap (buffer, &map);
  gst_adapter_clear(demux->adapter);
}

/* initialize the new element
 * instantiate pads and add them to element
 * set pad calback functions
 * initialize instance structure
 */
static void
gst_dmss_demux_init (GstDmssDemux * demux)
{
  demux->sinkpad = gst_pad_new_from_static_template (&sink_template, "sink");
  demux->videosrcpad = NULL;
  demux->audiosrcpad = NULL;
  demux->adapter = NULL;
  demux->need_segment = TRUE;
  demux->audio_format = GST_DMSS_AUDIO_FORMAT_UNKNOWN;
  demux->video_format = GST_DMSS_VIDEO_FORMAT_UNKNOWN;

  gst_pad_set_activate_function (demux->sinkpad,
      GST_DEBUG_FUNCPTR (gst_dmss_demux_sink_activate));
  
  gst_pad_set_query_function (demux->sinkpad,
      GST_DEBUG_FUNCPTR (gst_dmss_demux_sink_query));
  /* for push mode, this is the chain function */
  gst_pad_set_chain_function (demux->sinkpad,
      GST_DEBUG_FUNCPTR (gst_dmss_demux_chain));
  /* handling events (in push mode only) */
  gst_pad_set_event_function (demux->sinkpad,
      GST_DEBUG_FUNCPTR (gst_dmss_demux_handle_sink_event));

  /* now add the pad */
  gst_element_add_pad (GST_ELEMENT (demux), demux->sinkpad);
}

static void
gst_dmss_demux_finalize (GObject * object)
{
  G_OBJECT_CLASS (parent_class)->finalize (object);
}

static GstStateChangeReturn
gst_dmss_demux_change_state (GstElement * element, GstStateChange transition)
{
  /* GstDmssDemux *demux = GST_DMSS_DEMUX (element); */
  /* GstStateChangeReturn ret; */

  return GST_ELEMENT_CLASS (parent_class)->change_state (element, transition);
}

static gboolean
gst_dmss_demux_sink_query (GstPad * pad, GstObject * parent, GstQuery * query)
{
  GST_DEBUG ("%s:%d %s %d", __FILE__, __LINE__, __func__, (int)GST_QUERY_TYPE (query));

  switch (GST_QUERY_TYPE (query)) {
    case GST_QUERY_CONVERT:
      return FALSE;
    default:
      return gst_pad_query_default (pad, parent, query);
  }
}

static gboolean
gst_dmss_demux_src_query (GstPad * pad, GstObject * parent, GstQuery * query)
{
  gboolean ret;
  GstDmssDemux *demux = GST_DMSS_DEMUX (parent);

  switch (GST_QUERY_TYPE (query)) {
    case GST_QUERY_LATENCY:{
      if ((ret = gst_pad_peer_query (demux->sinkpad, query))) {
        GstClockTime latency;
        GstClockTime min, max;
        gboolean live;

        gst_query_parse_latency (query, &live, &min, &max);

        GST_DEBUG_OBJECT (demux, "Peer latency: min %"
            GST_TIME_FORMAT " max %" GST_TIME_FORMAT,
            GST_TIME_ARGS (min), GST_TIME_ARGS (max));

        /* latency = */
        /*     gst_util_uint64_scale (GST_SECOND, demux->output_buffer_duration_n, */
        /*     demux->output_buffer_duration_d); */
        latency = 2000*1000ul*1000ul;

        GST_WARNING_OBJECT (demux, "Our latency: min %" GST_TIME_FORMAT
            ", max %" GST_TIME_FORMAT,
            GST_TIME_ARGS (latency), GST_TIME_ARGS (latency));

        min += latency;
        if (max != GST_CLOCK_TIME_NONE)
          max += latency;

        GST_DEBUG_OBJECT (demux, "Calculated total latency : min %"
            GST_TIME_FORMAT " max %" GST_TIME_FORMAT,
            GST_TIME_ARGS (min), GST_TIME_ARGS (max));

        gst_query_set_latency (query, live, min, max);
        demux->last_latency = latency;
      }

      break;
    }
    default:
      ret = gst_pad_query_default (pad, parent, query);
      break;
  }

  return ret;
}

static gboolean
gst_dmss_demux_send_event (GstElement * element, GstEvent * event)
{
  GST_DEBUG ("%s:%d %s", __FILE__, __LINE__, __func__);

  return TRUE;
}

/* decide on push or pull based scheduling */
static gboolean
gst_dmss_demux_sink_activate (GstPad * sinkpad, GstObject * parent)
{
  GST_DEBUG_OBJECT (sinkpad, "activating push");
  return gst_pad_activate_mode (sinkpad, GST_PAD_MODE_PUSH, TRUE);
}

static gboolean gst_dmss_demux_handle_sink_event (GstPad * pad, GstObject * parent,
    GstEvent * event)
{
  GstDmssDemux *demux = GST_DMSS_DEMUX (parent);
  gboolean res = TRUE;

  switch (GST_EVENT_TYPE (event)) {
    case GST_EVENT_FLUSH_START:
      /* we are not blocking on anything exect the push() calls
       * to the peer which will be unblocked by forwarding the
       * event.*/
      res = gst_dmss_demux_push_event (demux, event);
      break;
    case GST_EVENT_FLUSH_STOP:
      /* gst_adapter_clear (dvdemux->adapter); */
      /* GST_DEBUG ("cleared adapter"); */
      /* gst_segment_init (&dvdemux->byte_segment, GST_FORMAT_BYTES); */
      /* gst_segment_init (&dvdemux->time_segment, GST_FORMAT_TIME); */
      /* dvdemux->discont = TRUE; */
      /* res = gst_dvdemux_push_event (dvdemux, event); */
      break;
    case GST_EVENT_SEGMENT:
    {
      const GstSegment *segment;

      gst_event_parse_segment (event, &segment);
      if (segment->format == GST_FORMAT_BYTES)
      {
        /* gst_segment_copy_into (segment, &demux->byte_segment); */
        /* demux->need_segment = TRUE; */
        /* demux->segment_seqnum = gst_event_get_seqnum (event); */

        gst_event_unref (event);
      }
      else
      {
        gst_event_unref (event);
        /* cannot accept this format */
        res = FALSE;
      }
      break;
    }
    case GST_EVENT_EOS:
      /* flush any pending data, should be nothing left. */
      gst_dmss_demux_flush (demux);
      /* forward event */
      res = gst_dmss_demux_push_event (demux, event);
      /* and clear the adapter */
      gst_adapter_clear (demux->adapter);
      break;
    case GST_EVENT_CAPS:
      gst_event_unref (event);
      break;
    default:
      res = gst_dmss_demux_push_event (demux, event);
      break;
  }

  return res;
}

static GstPad *
gst_dmss_demux_add_video_pad (GstDmssDemux * demux, GstCaps * caps)
{
  GstEvent *event;
  gchar *stream_id;

  demux->videosrcpad = gst_pad_new_from_static_template (&video_template, video_template.name_template);

  gst_pad_set_query_function (demux->videosrcpad, GST_DEBUG_FUNCPTR (gst_dmss_demux_src_query));
  
  gst_pad_set_event_function (demux->videosrcpad,
      GST_DEBUG_FUNCPTR (gst_dmss_demux_handle_src_event));
  gst_pad_use_fixed_caps (demux->videosrcpad);
  gst_pad_set_active (demux->videosrcpad, TRUE);
  
  stream_id =
      gst_pad_create_stream_id (demux->videosrcpad,
      GST_ELEMENT_CAST (demux),
      "video");

  event = gst_event_new_stream_start (stream_id);
  
  gst_pad_push_event (demux->videosrcpad, event);
  g_free (stream_id);

  gst_pad_set_caps (demux->videosrcpad, caps);

  gst_element_add_pad (GST_ELEMENT (demux), demux->videosrcpad);

  if (!demux->need_segment)
  {
    event = gst_event_new_segment (&demux->time_segment);
    gst_dmss_demux_push_event (demux, event);
  }
  
  return demux->videosrcpad;
}

static GstPad *
gst_dmss_demux_add_audio_pad (GstDmssDemux * demux, GstCaps * caps)
{
  GstEvent *event;
  gchar *stream_id;

  demux->audiosrcpad = gst_pad_new_from_static_template (&audio_template, audio_template.name_template);

  gst_pad_set_query_function (demux->audiosrcpad, GST_DEBUG_FUNCPTR (gst_dmss_demux_src_query));
  
  gst_pad_set_event_function (demux->audiosrcpad,
      GST_DEBUG_FUNCPTR (gst_dmss_demux_handle_src_event));
  gst_pad_use_fixed_caps (demux->audiosrcpad);
  gst_pad_set_active (demux->audiosrcpad, TRUE);
  
  stream_id =
      gst_pad_create_stream_id (demux->audiosrcpad,
      GST_ELEMENT_CAST (demux),
      "audio");

  event = gst_event_new_stream_start (stream_id);
  
  gst_pad_push_event (demux->audiosrcpad, event);
  g_free (stream_id);

  gst_pad_set_caps (demux->audiosrcpad, caps);

  gst_element_add_pad (GST_ELEMENT (demux), demux->audiosrcpad);

  if (!demux->need_segment)
  {
    event = gst_event_new_segment (&demux->time_segment);
    gst_dmss_demux_push_event (demux, event);
  }
  
  return demux->audiosrcpad;
}

/* streaming operation: 
 *
 * accumulate data until we have a frame, then decode. 
 */
static GstFlowReturn
gst_dmss_demux_chain (GstPad * pad, GstObject * parent, GstBuffer * buffer)
{
  GstDmssDemux *demux = GST_DMSS_DEMUX(parent);
  GstMapInfo map;
  gssize body_size;
  int const prologue_size = 32;
  int const minimum_dhav_packet_size = 24+8;
  guint8 dhav_packet_type;
  guint32 dhav_packet_size;
  guint32 dhav_head_size;
  guint32 dhav_body_size;
  GstBuffer *outbuf;
  gboolean is_video, is_audio;

  GST_DEBUG ("%s:%d %s", __FILE__, __LINE__, __func__);

  gst_buffer_map(buffer, &map, GST_MAP_READ);

  GST_DEBUG ("buffer received with command %.02x", (unsigned char)map.data[0]);
  if (((unsigned char*)map.data)[0] == (unsigned char)0xbc)
  {
    body_size = GUINT32_FROM_LE(*(guint32*)&map.data[4]);
    GST_DEBUG ("buffer received with DHAV payload size %d", (int)body_size);

    if (!body_size)
      goto discard_mapped_buffer;

    if (demux->waiting_dhav_end)
    {
      gst_buffer_unmap(buffer, &map);
      if(!demux->adapter)
        demux->adapter = gst_adapter_new();

      outbuf = gst_buffer_make_writable (buffer);
      gst_buffer_resize (outbuf, prologue_size, gst_buffer_get_size(outbuf) - prologue_size);
      
      gst_adapter_push (demux->adapter, outbuf);

      gst_dmss_demux_flush (demux);

      return GST_FLOW_OK;
    }
    else
    {
      if(body_size < minimum_dhav_packet_size)
        goto discard_mapped_buffer;
      
      if(map.data[prologue_size + 0] == 'D' &&
         map.data[prologue_size + 1] == 'H' &&
         map.data[prologue_size + 2] == 'A' &&
         map.data[prologue_size + 3] == 'V')
      {
        dhav_packet_type = map.data[prologue_size + 4];

        dhav_packet_size = GUINT32_FROM_LE(*(guint32*)&map.data[prologue_size + 12]);
        dhav_head_size = *(unsigned char*)&map.data[prologue_size + 22];
        dhav_body_size = dhav_packet_size - (24 + 8 + dhav_head_size);

        GST_DEBUG("DHAV packet (start) type: %.02x DHAV size: %d head size: %d body size: %d", (int)dhav_packet_type,
                  (int)dhav_packet_size, (int)dhav_head_size, (int)dhav_body_size);

        gst_buffer_unmap(buffer, &map);

        // We've got a video frame
        is_video = (dhav_packet_type == (unsigned char)0xfc ||
                    dhav_packet_type == (unsigned char)0xfd);

        is_audio = (dhav_packet_type == (unsigned char)0xf0);

        if (is_video || is_audio)
        {
          if(!demux->adapter)
            demux->adapter = gst_adapter_new();
          gst_adapter_push (demux->adapter, buffer);

          demux->waiting_dhav_end = TRUE;
          
          gst_dmss_demux_flush (demux);
          
          return GST_FLOW_OK;
        }
        else
        {
          GST_DEBUG("Not a video or audio frame, discarding");
          gst_buffer_unref (buffer);
          return GST_FLOW_OK;
        }
      }
      else
      {
        GST_ERROR("Buffer doesn't contain one or more DHAV packet");
        gst_buffer_unmap(buffer, &map);
        gst_buffer_unref (buffer);
        return GST_FLOW_OK;
      }
    }
  }
  else
  {
    gst_buffer_unmap(buffer, &map);
    gst_buffer_unref (buffer);
  }

  return GST_FLOW_OK;
discard_mapped_buffer:
  gst_buffer_unmap (buffer, &map);
  return GST_FLOW_OK;
}

/* handle an event on the source pad, it's most likely a seek */
static gboolean
gst_dmss_demux_handle_src_event (GstPad * pad, GstObject * parent,
    GstEvent * event)
{
  gboolean res = FALSE;
  GstDmssDemux *demux = GST_DMSS_DEMUX (parent);

  switch (GST_EVENT_TYPE (event)) {
    case GST_EVENT_QOS:
    {
      GstQOSType type;
      gdouble proportion;
      GstClockTimeDiff diff;
      GstClockTime timestamp;
      GstEvent* latency_event;

      gst_event_parse_qos (event, &type, &proportion, &diff, &timestamp);

      GST_WARNING ("QoS type: %d timestamp %" GST_TIME_FORMAT " diff %" GST_TIME_FORMAT,
                   (int)type, GST_TIME_ARGS(timestamp), GST_TIME_ARGS(diff));

      if (type == GST_QOS_TYPE_UNDERFLOW)
      {
        demux->last_latency += 1000*1000ul*1000ul + diff;
        GST_WARNING ("New latency: %" GST_TIME_FORMAT, GST_TIME_ARGS(demux->last_latency));
        res = gst_element_send_event (GST_ELEMENT(demux), gst_event_new_latency (demux->last_latency));
        res = gst_pad_push_event (demux->sinkpad, gst_event_new_latency (demux->last_latency));
      }
      else
      {
        res = gst_pad_push_event (demux->sinkpad, event);
      }
      break;
    }
    default:
      res = gst_pad_push_event (demux->sinkpad, event);
      break;
  }

  return res;
}

gssize gst_dmss_receive_packet_no_body(GSocket* socket, GCancellable *cancellable, GError **err, gchar* buffer)
{
  gssize size;
  gssize offset;
  gssize body_size;
  static int const buffer_size = 32;
  
  offset = 0;
  do
  {
    if ((size = g_socket_receive (socket, &buffer[offset],
            buffer_size - offset, cancellable, err)) <= 0)
    {
      if (size == 0 && *err && (*err)->code == G_IO_ERROR_CONNECTION_CLOSED)
        return 0;
      else
        goto recv_error;
    }

    offset += size;
  } while (offset != buffer_size);

  body_size = GUINT32_FROM_LE(*(guint32*)&buffer[4]);
  return body_size; // body size
 recv_error:
  return -1;
}

int gst_dmss_receive_packet(GSocket* socket, GCancellable *cancellable, GError **err,
                            gchar* ext_buffer, gssize* ext_size)
{
  gssize size, body_size;
  gssize offset;
  gchar buffer[32];

  offset = 0;
  do
  {
    if ((size = g_socket_receive (socket, &buffer[offset],
            sizeof(buffer) - offset, cancellable, err)) < 0)
      goto recv_error;

    if(offset < *ext_size)
      memcpy(&ext_buffer[offset], &buffer[offset], *ext_size - offset < size ? *ext_size - offset : size);

    offset += size;
  } while (offset != 32);

  body_size = GUINT32_FROM_LE(*(guint32*)&buffer[4]);

  offset = 0;
  while (offset != body_size)
  {
    if ((size = g_socket_receive (socket, &buffer[offset % sizeof(buffer)],
            (body_size - offset) > sizeof(buffer) - (offset % sizeof(buffer))
            ? sizeof(buffer) - (offset % sizeof(buffer))
            : (body_size - offset), cancellable, err)) < 0)
      goto recv_error;

    if(offset + 32 < *ext_size)
      memcpy(&ext_buffer[offset+32], &buffer[offset+32], *ext_size - (offset+32) < size ? *ext_size - (offset+32) : size);

    offset += size;
  }

  return 32 + body_size;
 recv_error:
  return -1;
}

int gst_dmss_receive_packet_ignore(GSocket* socket, GCancellable *cancellable, GError **err)
{
  gchar buffer[32];
  gssize size = 32;
  return gst_dmss_receive_packet(socket, cancellable, err, buffer, &size);
}

