remote_file "#{Chef::Config[:file_cache_path]}/#{node[:hiway][:targz]}" do
  source node[:hiway][:url]
  checksum node[:hiway][:checksum]
  owner node[:hiway][:user]
  group node[:hiway][:group]
  mode "0775"
  action :create_if_missing
end

installed_galaxy_prerequisites = "/tmp/.installed_galaxy_prerequisites"
bash "install_galaxy_prerequisites" do
  user "root"
  code <<-EOF
  set -e && set -o pipefail
  apt-get install mercurial -y
  apt-get install curl -y
  apt-get install python-dev -y
  apt-get install python-cheetah -y
  touch #{installed_galaxy_prerequisites}
  EOF
    not_if { ::File.exists?( "#{installed_galaxy_prerequisites}" ) }
end

installed_galaxy = "/tmp/.prepared_wordcount"
bash "install_galaxy" do
  user "#{node[:hiway][:user]}"
  group node[:hiway][:group]
  code <<-EOF
  hg clone #{node[:hiway][:galaxy][:repository]} #{node[:hiway][:galaxy][:home]}
  hg update stable #{node[:hiway][:galaxy][:home]}
  cp #{node[:hiway][:galaxy][:home]}/config/galaxy.ini.sample #{node[:hiway][:galaxy][:home]}/config/galaxy.ini
  cp #{node[:hiway][:galaxy][:home]}/config/tool_conf.xml.sample #{node[:hiway][:galaxy][:home]}/config/tool_conf.xml
  cp #{node[:hiway][:galaxy][:home]}/config/shed_tool_conf.xml #{node[:hiway][:galaxy][:home]}/shed_tool_conf.xml
  sed -i 's/#master_api_key = changethis/master_api_key = reverse/g' #{node[:hiway][:galaxy][:home]}/config/galaxy.ini
  sed -i 's/#allow_user_dataset_purge = False/allow_user_dataset_purge = True/g' #{node[:hiway][:galaxy][:home]}/config/galaxy.ini
  sed -i 's/#tool_dependency_dir = None/tool_dependency_dir = dependencies/g' #{node[:hiway][:galaxy][:home]}/config/galaxy.ini
  sed -i 's/#admin_users = None/admin_users = hiway@hiway.com/g' #{node[:hiway][:galaxy][:home]}/config/galaxy.ini
  curl --data "username=hiway&password=reverse&email=hiway@hiway.com" http://localhost:8080/api/users?key=reverse | grep "id" | sed 's/[\",]//g' | sed 's/id: //' > #{node[:hiway][:galaxy][:home]}/id
  curl -X POST http://localhost:8080/api/users/$id/api_key?key=reverse | sed 's/\"//g' > #{node[:hiway][:galaxy][:home]}/api
  sh run.sh &
  touch #{installed_galaxy}
  EOF
    not_if { ::File.exists?( "#{installed_galaxy}" ) }
end
