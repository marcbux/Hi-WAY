name             'hiway'
maintainer       "Jim Dowling"
maintainer_email "jdowling@kth.se"
license          "GPL 2.0"
description      'Installs/Configures Hiway'
long_description IO.read(File.join(File.dirname(__FILE__), 'README.md'))
version          "1.0"

recipe           "hiway::hadoop", "Configures Hadoop to be compatible with Hi-WAY"
recipe           "hiway::install", "Installs Hi-WAY"
recipe           "hiway::wordcount.cf", "Sets up and runs a basic cuneiform workflow on Hi-WAY"

depends 'hadoop'

%w{ ubuntu debian rhel centos }.each do |os|
  supports os
end

attribute "hadoop/version",
:display_name => "Hadoop version",
:description => "Version of hadoop",
:type => 'string',
:default => "2.4.0"
