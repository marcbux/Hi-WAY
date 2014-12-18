template "#{node[:hiway][:home]}/#{node[:hiway][:galaxy101][:workflow]}" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  source "#{node[:hiway][:galaxy101][:workflow]}.erb"
  mode "0774"
end

template "#{node[:hiway][:home]}/Exons.bed" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  source "galaxy101.Exons.bed.erb"
  mode "0774"
end

template "#{node[:hiway][:home]}/SNPs.bed" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  source "galaxy101.SNPs.bed.erb"
  mode "0774"
end

prepared_galaxy101 = "/tmp/.prepared_galaxy101"
bash "prepare_galaxy101" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  code <<-EOF
  set -e && set -o pipefail
  #{node[:hadoop][:home]}/bin/hdfs dfs -put #{node[:hiway][:home]}/Exons.bed
  #{node[:hadoop][:home]}/bin/hdfs dfs -put #{node[:hiway][:home]}/SNPs.bed
  touch #{prepared_galaxy101}
  EOF
    not_if { ::File.exists?( "#{prepared_galaxy101}" ) }
end

#ran_galaxy101 = "/tmp/.ran_galaxy101"
#bash "run_galaxy101" do
#  user node[:hiway][:user]
#  group node[:hiway][:group]
#  code <<-EOF
#  set -e && set -o pipefail
#  #{node[:hadoop][:home]}/bin/yarn jar #{node[:hiway][:home]}/hiway-core-#{node[:hiway][:version]}.jar -w #{node[:hiway][:home]}/#{node[:hiway][:galaxy101][:workflow]} -l galaxy 
#  touch #{ran_galaxy101}
#  EOF
#    not_if { ::File.exists?( "#{ran_galaxy101}" ) }
#end
