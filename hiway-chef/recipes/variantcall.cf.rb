template "#{node[:hiway][:home]}/#{node[:hiway][:variantcall][:setupworkflow]}" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  source "#{node[:hiway][:variantcall][:setupworkflow]}.erb"
  mode "0774"
end

template "#{node[:hiway][:home]}/#{node[:hiway][:variantcall][:workflow]}" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  source "#{node[:hiway][:variantcall][:workflow]}.erb"
  mode "0774"
end

bash "prepare_variantcall" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  code <<-EOF
  set -e && set -o pipefail
  #{node[:hadoop][:home]}/bin/yarn jar #{node[:hiway][:home]}/hiway-core-#{node[:hiway][:version]}.jar -w #{node[:hiway][:home]}/#{node[:hiway][:variantcall][:setupworkflow]}} -s #{node[:hiway][:home]}/variantcall_setup_summary.json
  EOF
#    not_if { ::File.exists?( "#{prepared_variantcall}" ) }
end

bash "run_variantcall" do
  user node[:hiway][:user]
  group node[:hiway][:group]
  code <<-EOF
  set -e && set -o pipefail
  #{node[:hadoop][:home]}/bin/yarn jar #{node[:hiway][:home]}/hiway-core-#{node[:hiway][:version]}.jar -w #{node[:hiway][:home]}/#{node[:hiway][:variantcall][:workflow]}} -s #{node[:hiway][:home]}/variantcall_summary.json
  EOF
#    not_if { ::File.exists?( "#{ran_variantcall}" ) }
end
