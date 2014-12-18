template "#{node[:hiway][:home]}/#{node[:hiway][:RNASeq][:workflow]}" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  source "#{node[:hiway][:RNASeq][:workflow]}.erb"
  mode "0774"
end

template "#{node[:hiway][:home]}/mm9_ref_annotation.gtf.tar.gz" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  source "RNASeq.mm9_ref_annotation.gtf.tar.gz.erb"
  mode "0774"
end

remote_file "#{Chef::Config[:file_cache_path]}/GSM1533014_MD_O1_WT_Colon.fastq" do
  source http://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?cmd=dload&run_list=SRR1632911&format=fastq
  owner node[:hiway][:user]
  group node[:hiway][:group]
  mode "0775"
  action :create_if_missing
end

remote_file "#{Chef::Config[:file_cache_path]}/GSM1533014_MD_O2_WT_Colon.fastq" do
  source http://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?cmd=dload&run_list=SRR1632912&format=fastq
  owner node[:hiway][:user]
  group node[:hiway][:group]
  mode "0775"
  action :create_if_missing
end

remote_file "#{Chef::Config[:file_cache_path]}/GSM1533014_MD_O3_WT_Colon.fastq" do
  source http://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?cmd=dload&run_list=SRR1632913&format=fastq
  owner node[:hiway][:user]
  group node[:hiway][:group]
  mode "0775"
  action :create_if_missing
end

remote_file "#{Chef::Config[:file_cache_path]}/GSM1533014_MD_Y1_WT_Colon.fastq" do
  source http://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?cmd=dload&run_list=SRR1632942&format=fastq
  owner node[:hiway][:user]
  group node[:hiway][:group]
  mode "0775"
  action :create_if_missing
end

remote_file "#{Chef::Config[:file_cache_path]}/GSM1533014_MD_Y2_WT_Colon.fastq" do
  source http://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?cmd=dload&run_list=SRR1632943&format=fastq
  owner node[:hiway][:user]
  group node[:hiway][:group]
  mode "0775"
  action :create_if_missing
end

remote_file "#{Chef::Config[:file_cache_path]}/GSM1533014_MD_Y3_WT_Colon.fastq" do
  source http://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?cmd=dload&run_list=SRR1632944&format=fastq
  owner node[:hiway][:user]
  group node[:hiway][:group]
  mode "0775"
  action :create_if_missing
end

prepared_RNASeq = "/tmp/.prepared_RNASeq"
bash "prepare_RNASeq" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  code <<-EOF
  set -e && set -o pipefail
  tar xzvf #{node[:hiway][:home]}/mm9_ref_annotation.gtf.tar.gz -C #{node[:hiway][:home]}
  #{node[:hadoop][:home]}/bin/hdfs dfs -put #{node[:hiway][:home]}/mm9_ref_annotation.gtf
  #{node[:hadoop][:home]}/bin/hdfs dfs -put #{Chef::Config[:file_cache_path]}/GSM1533014_MD_O1_WT_Colon.fastq
  #{node[:hadoop][:home]}/bin/hdfs dfs -put #{Chef::Config[:file_cache_path]}/GSM1533014_MD_O2_WT_Colon.fastq
  #{node[:hadoop][:home]}/bin/hdfs dfs -put #{Chef::Config[:file_cache_path]}/GSM1533014_MD_O3_WT_Colon.fastq
  #{node[:hadoop][:home]}/bin/hdfs dfs -put #{Chef::Config[:file_cache_path]}/GSM1533014_MD_Y1_WT_Colon.fastq
  #{node[:hadoop][:home]}/bin/hdfs dfs -put #{Chef::Config[:file_cache_path]}/GSM1533014_MD_Y2_WT_Colon.fastq
  #{node[:hadoop][:home]}/bin/hdfs dfs -put #{Chef::Config[:file_cache_path]}/GSM1533014_MD_Y3_WT_Colon.fastq
  touch #{prepared_RNASeq}
  EOF
    not_if { ::File.exists?( "#{prepared_RNASeq}" ) }
end

#ran_RNASeq = "/tmp/.ran_RNASeq"
#bash "run_RNASeq" do
#  user node[:hiway][:user]
#  group node[:hiway][:group]
#  code <<-EOF
#  set -e && set -o pipefail
#  #{node[:hadoop][:home]}/bin/yarn jar #{node[:hiway][:home]}/hiway-core-#{node[:hiway][:version]}.jar -w #{node[:hiway][:home]}/#{node[:hiway][:RNASeq][:workflow]} -l galaxy
#  touch #{ran_RNASeq}
#  EOF
#    not_if { ::File.exists?( "#{ran_RNASeq}" ) }
#end
