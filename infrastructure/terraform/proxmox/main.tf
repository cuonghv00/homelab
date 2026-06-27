# infrastructure/terraform/proxmox/main.tf
locals {
  iso_url = "https://factory.talos.dev/image/${var.talos_schematic_id}/${var.talos_version}/metal-amd64.iso"

  nodes = {
    "talos-cp-01" = { vmid = 101, cores = 4, memory = 4096,  disk = 50,  ip = "192.168.10.101" }
    "talos-w-01"  = { vmid = 102, cores = 4, memory = 8192,  disk = 150, ip = "192.168.10.102" }
    "talos-w-02"  = { vmid = 103, cores = 4, memory = 8192,  disk = 150, ip = "192.168.10.103" }
  }
}

resource "proxmox_virtual_environment_download_file" "talos_iso" {
  content_type = "iso"
  datastore_id = var.iso_datastore
  node_name    = var.proxmox_node
  file_name    = "talos-${var.talos_version}-${substr(var.talos_schematic_id, 0, 8)}-amd64.iso"
  url          = local.iso_url
  overwrite    = false
}

resource "proxmox_virtual_environment_vm" "talos" {
  for_each = local.nodes

  name      = each.key
  vm_id     = each.value.vmid
  node_name = var.proxmox_node

  # Talos has no cloud-init; boot the ISO into maintenance mode, install to disk.
  agent {
    enabled = true   # qemu-guest-agent extension reports the IP to Proxmox
  }

  cpu {
    cores = each.value.cores
    type  = "host"
  }

  memory {
    dedicated = each.value.memory
  }

  scsi_hardware = "virtio-scsi-single"

  disk {
    datastore_id = var.vm_datastore
    interface    = "scsi0"
    size         = each.value.disk
    file_format  = "raw"
    iothread     = true
    discard      = "on"
  }

  cdrom {
    file_id = proxmox_virtual_environment_download_file.talos_iso.id
  }

  network_device {
    bridge = var.network_bridge
    model  = "virtio"
  }

  operating_system {
    type = "l26"
  }

  # Boot from disk first (after install); empty disk falls through to the ISO (maintenance mode).
  boot_order = ["scsi0", "ide3"]

  # Talos reboots itself on config apply; don't let Terraform fight the agent timeout on first boot.
  timeout_create = 600

  lifecycle {
    ignore_changes = [cdrom] # keep ISO attached; don't churn after install
  }
}
