node.default['java']['jdk_version'] = 7
node.default['java']['install_flavor'] = "openjdk"
include_recipe "java"

remote_file "#{Chef::Config[:file_cache_path]}/hiway-dist-#{node[:hiway][:version]}.tar.gz" do
  source node[:hiway][:url]
  checksum node[:hiway][:checksum]
  owner node[:hiway][:user]
  group node[:hiway][:group]
  mode "0775"
  action :create_if_missing
end

installed_hiway = "/tmp/.hiway_installed_#{node[:hiway][:version]}"
bash "install_hiway" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  code <<-EOF
  set -e && set -o pipefail
  #{node[:hadoop][:home]}/bin/hdfs dfs -mkdir -p /user/#{node[:hiway][:user]}
  tar xfz #{Chef::Config[:file_cache_path]}/hiway-dist-#{node[:hiway][:version]}.tar.gz -C #{node[:hiway][:dir]}
  touch #{installed_hiway}
  EOF
    not_if { ::File.exists?( "#{installed_hiway}" ) }
end

template "#{node[:hadoop][:conf_dir]}/hiway-site.xml" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  source "install.hiway-site.xml.erb"
  mode "0755"
end

link "#{node[:hadoop][:dir]}/hiway" do
  to node[:hiway][:home]
end
