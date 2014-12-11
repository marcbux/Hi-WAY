template "#{node[:hiway][:home]}/#{node[:hiway][:wordcount][:workflow]}" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  source "#{node[:hiway][:wordcount][:workflow]}.erb"
  mode "0774"
end

directory "#{node[:hiway][:home]}/wordcount" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  mode "0774"
  recursive true
  action :create
end

template "#{node[:hiway][:home]}/wordcount/benzko.txt" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  source "wordcount.benzko.txt.erb"
  mode "0774"
end

template "#{node[:hiway][:home]}/wordcount/gronemeyer.txt" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  source "wordcount.gronemeyer.txt.erb"
  mode "0774"
end

bash "prepare_wordcount" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  code <<-EOF
  set -e && set -o pipefail
  #{node[:hadoop][:home]}/bin/hdfs dfs -put #{node[:hiway][:home]}/wordcount
  EOF
    not_if "#{node[:hadoop][:home]}/bin/hdfs dfs -test -e /user/#{node[:hiway][:user]}/wordcount/gronemeyer.txt"
end

ran_wordcount = "/tmp/.ran_wordcount"
bash "run_wordcount" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  code <<-EOF
  set -e && set -o pipefail
  #{node[:hadoop][:home]}/bin/yarn jar #{node[:hiway][:home]}/hiway-core-#{node[:hiway][:version]}.jar -w #{node[:hiway][:home]}/#{node[:hiway][:wordcount][:workflow]}
  touch #{ran_wordcount}
  EOF
    not_if { ::File.exists?( "#{ran_wordcount}" ) }
end
