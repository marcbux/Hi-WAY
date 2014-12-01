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

prepared_wordcount = "/tmp/.prepared_wordcount_#{node[:hiway][:version]}"
bash "prepare_wordcount" do
  user "#{node[:hiway][:user]}"
  code <<-EOF
  set -e && set -o pipefail
  #{node[:hadoop][:home]}/bin/hdfs dfs -put #{node[:hiway][:home]}/wordcount
  touch #{prepared_wordcount}
  EOF
    not_if { ::File.exists?( "#{prepared_wordcount}" ) }
end

ran_wordcount = "/tmp/.ran_wordcount_#{node[:hiway][:version]}"
bash "run_wordcount" do
  user "#{node[:hiway][:user]}"
  code <<-EOF
  set -e && set -o pipefail
  #{node[:hadoop][:home]}/bin/yarn jar #{node[:hiway][:home]}/hiway-core-#{node[:hiway][:version]}.jar -w #{node[:hiway][:home]}/wordcount.cf
  touch #{ran_wordcount}
  EOF
    not_if { ::File.exists?( "#{ran_wordcount}" ) }
end
