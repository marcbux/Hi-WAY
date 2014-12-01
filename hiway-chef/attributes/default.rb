include_attribute "hadoop"

default[:hiway][:version]                         = "0.2.0-SNAPSHOT"
default[:hiway][:user]                            =  node[:hadoop][:yarn][:user]
default[:hiway][:group]                           =  node[:hadoop][:group]
default[:hiway][:dir]                             =  node[:hadoop][:dir]
default[:hiway][:home]                            =  "#{default[:hiway][:dir]}/hiway-#{default[:hiway][:version]}"
default[:hiway][:url]                             = "https://github.com/marcbux/Hi-WAY/releases/download/#{node[:hiway][:version]}/hiway-dist-#{node[:hiway][:version]}.tar.gz"
default[:hiway][:checksum]                        = ""

default[:hiway][:variantcall][:bowtie2][:version] = "2.2.2"
default[:hiway][:variantcall][:bowtie2][:url]     = "http://downloads.sourceforge.net/project/bowtie-bio/bowtie2/#{node[:hiway][:variantcall][:bowtie2][:version]}/bowtie2-#{node[:hiway][:variantcall][:bowtie2][:version]}-linux-x86_64.zip"
