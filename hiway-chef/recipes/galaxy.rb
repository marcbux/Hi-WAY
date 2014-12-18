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
  user node[:hiway][:user]
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
  #{node[:hiway][:galaxy][:home]}/scripts/api/install_tool_shed_repositories.py --url http://toolshed.g2.bx.psu.edu/ --api `echo #{node[:hiway][:galaxy][:home]}/api` --local http://localhost:8080/ --name join --owner devteam --revision de21bdbb8d28 --repository-deps --tool-deps --panel-section-name galaxy101
  #{node[:hiway][:galaxy][:home]}/scripts/api/install_tool_shed_repositories.py --url http://toolshed.g2.bx.psu.edu/ --api `echo #{node[:hiway][:galaxy][:home]}/api` --local http://localhost:8080/ --name fastq_trimmer_by_quality --owner devteam --revision 1cdcaf5fc1da --repository-deps --tool-deps --panel-section-name RNAseq
  #{node[:hiway][:galaxy][:home]}/scripts/api/install_tool_shed_repositories.py --url http://toolshed.g2.bx.psu.edu/ --api `echo #{node[:hiway][:galaxy][:home]}/api` --local http://localhost:8080/ --name fastqc --owner devteam --revision e28c965eeed4 --repository-deps --tool-deps --panel-section-name RNAseq
  #{node[:hiway][:galaxy][:home]}/scripts/api/install_tool_shed_repositories.py --url http://toolshed.g2.bx.psu.edu/ --api `echo #{node[:hiway][:galaxy][:home]}/api` --local http://localhost:8080/ --name fastx_clipper --owner devteam --revision 8192b4014977 --repository-deps --tool-deps --panel-section-name RNAseq
  #{node[:hiway][:galaxy][:home]}/scripts/api/install_tool_shed_repositories.py --url http://toolshed.g2.bx.psu.edu/ --api `echo #{node[:hiway][:galaxy][:home]}/api` --local http://localhost:8080/ --name tophat2 --owner devteam --revision ae06af1118dc --repository-deps --tool-deps --panel-section-name RNAseq
  #{node[:hiway][:galaxy][:home]}/scripts/api/install_tool_shed_repositories.py --url http://toolshed.g2.bx.psu.edu/ --api `echo #{node[:hiway][:galaxy][:home]}/api` --local http://localhost:8080/ --name picard --owner devteam --revision ab1f60c26526 --repository-deps --tool-deps --panel-section-name RNAseq
  #{node[:hiway][:galaxy][:home]}/scripts/api/install_tool_shed_repositories.py --url http://toolshed.g2.bx.psu.edu/ --api `echo #{node[:hiway][:galaxy][:home]}/api` --local http://localhost:8080/ --name cufflinks --owner devteam --revision 9aab29e159a7 --repository-deps --tool-deps --panel-section-name RNAseq
  #{node[:hiway][:galaxy][:home]}/scripts/api/install_tool_shed_repositories.py --url http://toolshed.g2.bx.psu.edu/ --api `echo #{node[:hiway][:galaxy][:home]}/api` --local http://localhost:8080/ --name cuffmerge --owner devteam --revision 424d49834830 --repository-deps --tool-deps --panel-section-name RNAseq
  #{node[:hiway][:galaxy][:home]}/scripts/api/install_tool_shed_repositories.py --url http://toolshed.g2.bx.psu.edu/ --api `echo #{node[:hiway][:galaxy][:home]}/api` --local http://localhost:8080/ --name cuffcompare --owner devteam --revision 67695d7ff787 --repository-deps --tool-deps --panel-section-name RNAseq
  #{node[:hiway][:galaxy][:home]}/scripts/api/install_tool_shed_repositories.py --url http://toolshed.g2.bx.psu.edu/ --api `echo #{node[:hiway][:galaxy][:home]}/api` --local http://localhost:8080/ --name cuffdiff --owner devteam --revision 604fa75232a2 --repository-deps --tool-deps --panel-section-name RNAseq
  #{node[:hiway][:galaxy][:home]}/scripts/api/install_tool_shed_repositories.py --url http://toolshed.g2.bx.psu.edu/ --api `echo #{node[:hiway][:galaxy][:home]}/api` --local http://localhost:8080/ --name column_maker --owner devteam --revision 08a01b2ce4cd --repository-deps --tool-deps --panel-section-name RNAseq
  touch #{installed_galaxy}
  EOF
    not_if { ::File.exists?( "#{installed_galaxy}" ) }
end
