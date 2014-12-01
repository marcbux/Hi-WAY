template "#{node[:hiway][:home]}/variantcall.setup.cf" do
  user node[:hiway][:user]
  source "variantcall.setup.cf.erb"
end

template "#{node[:hiway][:home]}/variantcall.cf" do
  user node[:hiway][:user]
  source "variantcall.cf.erb"
end

remote_file "#{Chef::Config[:file_cache_path]}/bowtie2-#{node[:hiway][:variantcall][:bowtie2][:version]}-linux-x86_64.zip" do
  source node[:hiway][:variantcall][:bowtie2][:url]
  owner node[:hiway][:user]
  group node[:hiway][:group]
  mode "0775"
  action :create_if_missing
end

installed_bowtie2 = "/tmp/.installed_bowtie2_#{node[:hiway][:version]}"
bash "install_bowtie2" do
  user "root"
  code <<-EOF
  set -e && set -o pipefail
  apt-get install unzip -y
  unzip #{Chef::Config[:file_cache_path]}/bowtie2-#{node[:hiway][:variantcall][:bowtie2][:version]}-linux-x86_64.zip -d #{node[:hiway][:dir]}
  ln -f -s #{node[:hiway][:dir]}/bowtie2-#{node[:hiway][:variantcall][:bowtie2][:version]}/bowtie2* /usr/bin/
  touch #{installed_bowtie2}
  EOF
    not_if { ::File.exists?( "#{installed_bowtie2}" ) }
end
