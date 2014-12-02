configured_hadoop_for_hiway = "/tmp/.configured_hadoop_for_hiway"
bash "configure_hadoop_for_hiway" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  code <<-EOF
  set -e && set -o pipefail
  if grep -q "yarn.application.classpath" #{node[:hadoop][:home]}/etc/hadoop/yarn-site.xml
  then
    perl -i -0pe 's%<name>yarn.application.classpath</name>\\s*<value>%<name>yarn.application.classpath</name>\\n\\t\\t<value>#{node[:hiway][:home]}/\\*, #{node[:hiway][:home]}/lib/\\*, %' #{node[:hadoop][:home]}/etc/hadoop/yarn-site.xml
  else
    sed -i 's%</configuration>%<name>yarn.application.classpath</name>\\n\\t\\t<value>\$HADOOP_CONF_DIR, \$HADOOP_COMMON_HOME/share/hadoop/common/\\*, \$HADOOP_COMMON_HOME/share/hadoop/common/lib/\\*, \$HADOOP_HDFS_HOME/share/hadoop/hdfs/\\*, \$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/\\*, \$HADOOP_YARN_HOME/share/hadoop/yarn/\\*, \$HADOOP_YARN_HOME/share/hadoop/yarn/lib/\\*, \$HIWAY_HOME/\\*, \$HIWAY_HOME/lib/\\*</value>\\n\\t</property>\\n</configuration>%' #{node[:hadoop][:home]}/etc/hadoop/yarn-site.xml
  fi
  touch #{configured_hadoop_for_hiway}
  EOF
    not_if { ::File.exists?( "#{configured_hadoop_for_hiway}" ) }
end

service "namenode" do
  action :start
end

service "resourcemanager" do
  action :start
end

service "historyserver" do
  action :start
end

service "datanode" do
  action :start
end

service "nodemanager" do
  action :start
end
