template "#{node[:hiway][:home]}/wordcount.cf" do
  user node[:hiway][:user]
  source "wordcount.cf.erb"
end

directory "#{node[:hiway][:home]}/wordcount" do
  user node[:hiway][:user]
end

template "#{node[:hiway][:home]}/wordcount/benzko.txt" do
  user node[:hiway][:user]
  source "wordcount.benzko.txt.erb"
end

template "#{node[:hiway][:home]}/wordcount/gronemeyer.txt" do
  user node[:hiway][:user]
  source "wordcount.gronemeyer.txt.erb"
end

bash "setup_word_count" do
  user "#{node[:hiway][:user]}"
  code <<-EOF
  set -e && set -o pipefail
  #{node[:hadoop][:home]}/bin/hdfs dfs -put #{node[:hiway][:home]}/wordcount
  touch #{node[:hiway][:home]}/.setup_word_count
  EOF
     not_if { ::File.exists?( "#{node[:hiway][:home]}/.setup_word_count" ) }
end

bash "run_word_count" do
  user "#{node[:hiway][:user]}"
  code <<-EOF
  set -e && set -o pipefail
  #{node[:hadoop][:home]}/bin/yarn jar #{node[:hiway][:home]}/hiway-core-0.2.0-SNAPSHOT.jar -w #{node[:hiway][:home]}/wordcount.cf
  touch #{node[:hiway][:home]}/.run_word_count
  EOF
     not_if { ::File.exists?( "#{node[:hiway][:home]}/.run_word_count" ) }
end
