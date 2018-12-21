gstdmss_sources = [
  'plugin.c',
  'gstdmsssrc.c',
  'gstdmssdemux.c',
  'gstdmssprotocol.c'
]

gstdmss = library('gstdmss',
  gstdmss_sources,
  c_args : gst_plugins_bad_args,
  include_directories: [configinc, libsinc],
  dependencies : [gio_dep, gstbase_dep, gstnet_dep, gstvideo_dep],
  install : true,
  install_dir : plugins_install_dir,
)
pkgconfig.generate(gstdmss, install_dir : plugins_pkgconfig_install_dir)
