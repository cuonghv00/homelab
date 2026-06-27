# infrastructure/terraform/proxmox/outputs.tf
output "maintenance_ips" {
  description = "DHCP IP each VM received in maintenance mode (target for talosctl apply-config --insecure)"
  value = {
    for name, vm in proxmox_virtual_environment_vm.talos :
    name => try(
      [for ip in flatten(vm.ipv4_addresses) : ip if ip != "127.0.0.1"][0],
      "pending-agent"
    )
  }
}

output "planned_static_ips" {
  description = "Final static IPs assigned by the Talos machine config"
  value       = { for name, n in local.nodes : name => n.ip }
}
