plugin_LTLIBRARIES = libgstdmss.la

libgstdmss_la_SOURCES = \
	plugin.c \
	gstdmsssrc.c \
	gstdmssdemux.c

libgstdmss_la_CFLAGS = \
	$(GST_PLUGINS_BAD_CFLAGS) $(GST_PLUGINS_BASE_CFLAGS) \
	$(GST_BASE_CFLAGS) $(GST_CFLAGS) $(GIO_CFLAGS)
libgstdmss_la_LIBADD = $(GST_BASE_LIBS) $(GST_PLUGINS_BASE_LIBS) \
	$(GST_LIBS) $(GIO_LIBS)
libgstdmss_la_LDFLAGS = $(GST_PLUGIN_LDFLAGS) $(GST_ALL_LDFLAGS) $(GIO_LDFLAGS)

noinst_HEADERS = \
	gstdmss.h \
	gstdmssdemux.h \
	gstdmsssrc.h
