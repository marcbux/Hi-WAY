prepared_montage_m17_4 = "/tmp/.prepared_montage_m17_4"
bash "prepare_montage_m17_4" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  code <<-EOF
  set -e && set -o pipefail
  
  touch #{prepared_montage_m17_4}
  EOF
    not_if { ::File.exists?( "#{prepared_montage_m17_4}" ) }
end

#ran_montage_m17_4 = "/tmp/.ran_montage_m17_4"
#bash "run_montage_m17_4" do
#  user node[:hiway][:user]
#  group node[:hiway][:group]
#  code <<-EOF
#  set -e && set -o pipefail
#  #{node[:hadoop][:home]}/bin/yarn jar #{node[:hiway][:home]}/hiway-core-#{node[:hiway][:version]}.jar -w #{node[:hiway][:home]}/#{node[:hiway][:montage_m17_4][:workflow]} -l dax
#  touch #{ran_montage_m17_4}
#  EOF
#    not_if { ::File.exists?( "#{ran_montage_m17_4}" ) }
#end
