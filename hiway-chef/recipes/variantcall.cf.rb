template "#{node[:hiway][:home]}/#{node[:hiway][:variantcall][:setupworkflow]}" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  source "#{node[:hiway][:variantcall][:setupworkflow]}.erb"
end

template "#{node[:hiway][:home]}/#{node[:hiway][:variantcall][:workflow]}" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  source "#{node[:hiway][:variantcall][:workflow]}.erb"
end

remote_file "#{Chef::Config[:file_cache_path]}/#{node[:hiway][:variantcall][:bowtie2][:zip]}" do
  source node[:hiway][:variantcall][:bowtie2][:url]
  owner node[:hiway][:user]
  group node[:hiway][:group]
  mode "0775"
  action :create_if_missing
end

remote_file "#{Chef::Config[:file_cache_path]}/#{node[:hiway][:variantcall][:samtools][:tarbz2]}" do
  source node[:hiway][:variantcall][:samtools][:url]
  owner node[:hiway][:user]
  group node[:hiway][:group]
  mode "0775"
  action :create_if_missing
end

remote_file "#{node[:hiway][:dir]}/#{node[:hiway][:variantcall][:varscan][:jar]}" do
  source node[:hiway][:variantcall][:varscan][:url]
  owner node[:hiway][:user]
  group node[:hiway][:group]
  mode "0775"
  action :create_if_missing
end

remote_file "#{Chef::Config[:file_cache_path]}/#{node[:hiway][:variantcall][:annovar][:targz]}" do
  source node[:hiway][:variantcall][:annovar][:url]
  owner node[:hiway][:user]
  group node[:hiway][:group]
  mode "0775"
  action :create_if_missing
end

installed_bowtie2 = "/tmp/.installed_bowtie2"
bash "install_bowtie2" do
  user "root"
  code <<-EOF
  set -e && set -o pipefail
  apt-get install unzip -y
  unzip #{Chef::Config[:file_cache_path]}/#{node[:hiway][:variantcall][:bowtie2][:zip]} -d #{node[:hiway][:dir]}
  ln -f -s #{node[:hiway][:variantcall][:bowtie2][:home]}/bowtie2* /usr/bin/
  touch #{installed_bowtie2}
  EOF
    not_if { ::File.exists?( "#{installed_bowtie2}" ) }
end

installed_samtools = "/tmp/.installed_samtools"
bash "install_samtools" do
  user "root"
  code <<-EOF
  set -e && set -o pipefail
  apt-get install libncurses5-dev -y
  tar xjvf #{Chef::Config[:file_cache_path]}/#{node[:hiway][:variantcall][:samtools][:tarbz2]} -C #{node[:hiway][:dir]}
  make -C #{node[:hiway][:variantcall][:samtools][:home]}
  ln -s #{node[:hiway][:variantcall][:samtools][:home]}/samtools /usr/bin/
  touch #{installed_samtools}
  EOF
    not_if { ::File.exists?( "#{installed_samtools}" ) }
end

installed_varscan = "/tmp/.installed_varscan"
bash "install_varscan" do
  user "root"
  code <<-EOF
  set -e && set -o pipefail
  ln -s #{node[:hiway][:dir]}/#{node[:hiway][:variantcall][:varscan][:jar]} \$JAVA_HOME/jre/lib/ext/
  touch #{installed_varscan}
  EOF
    not_if { ::File.exists?( "#{installed_varscan}" ) }
end

installed_annovar = "/tmp/.installed_annovar"
bash "install_annovar" do
  user "root"
  code <<-EOF
  set -e && set -o pipefail
  tar xvfz #{Chef::Config[:file_cache_path]}/#{node[:hiway][:variantcall][:annovar][:targz]} -C #{node[:hiway][:dir]}
  ln -f -s #{node[:hiway][:variantcall][:annovar][:home]}/*.pl /usr/bin/
  touch #{installed_annovar}
  EOF
    not_if { ::File.exists?( "#{installed_annovar}" ) }
end

#prepared_variantcall = "/tmp/.prepared_variantcall"
#bash "prepare_variantcall" do
#  user "#{node[:hiway][:user]}"
#  group node[:hiway][:group]
#  code <<-EOF
#  set -e && set -o pipefail
#  #{node[:hadoop][:home]}/bin/yarn jar #{node[:hiway][:home]}/hiway-core-#{node[:hiway][:version]}.jar -w #{node[:hiway][:home]}/#{node[:hiway][:variantcall][:setupworkflow]}}
#  touch #{prepared_variantcall}
#  EOF
#    not_if { ::File.exists?( "#{prepared_variantcall}" ) }
#end

#ran_variantcall = "/tmp/.ran_variantcall"
#bash "run_variantcall" do
#  user "#{node[:hiway][:user]}"
#  group node[:hiway][:group]
#  code <<-EOF
#  set -e && set -o pipefail
#  #{node[:hadoop][:home]}/bin/yarn jar #{node[:hiway][:home]}/hiway-core-#{node[:hiway][:version]}.jar -w #{node[:hiway][:home]}/#{node[:hiway][:variantcall][:workflow]}}
#  touch #{ran_variantcall}
#  EOF
#    not_if { ::File.exists?( "#{ran_variantcall}" ) }
#end
