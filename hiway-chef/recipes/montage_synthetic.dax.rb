remote_file "#{node[:hiway][:home]}/#{node[:hiway][:montage_synthetic][:workflow]}" do
  source node[:hiway][:montage_synthetic][:url]
  owner node[:hiway][:user]
  group node[:hiway][:group]
  mode "0775"
  action :create_if_missing
end

ran_montage_synthetic = "/tmp/.ran_montage_synthetic"
bash "run_montage_synthetic" do
  user "#{node[:hiway][:user]}"
  group node[:hiway][:group]
  code <<-EOF
  set -e && set -o pipefail
  #{node[:hadoop][:home]}/bin/yarn jar #{node[:hiway][:home]}/hiway-core-#{node[:hiway][:version]}.jar -w #{node[:hiway][:home]}/#{node[:hiway][:montage_synthetic][:workflow]} -l dax
  touch #{ran_montage_synthetic}
  EOF
    not_if { ::File.exists?( "#{ran_montage_synthetic}" ) }
end
