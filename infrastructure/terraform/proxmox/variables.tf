# infrastructure/terraform/proxmox/variables.tf
variable "proxmox_endpoint" {
  type        = string
  description = "Proxmox API endpoint, e.g. https://192.168.10.X:8006/"
}

variable "proxmox_api_token" {
  type        = string
  sensitive   = true
  description = "API token: terraform@pve!provider=<uuid>"
}

variable "proxmox_node" {
  type        = string
  description = "Proxmox node name (see: pvesh get /nodes)"
  default     = "pve"
}

variable "vm_datastore" {
  type        = string
  description = "Datastore for VM disks (e.g. local-lvm)"
  default     = "local-lvm"
}

variable "iso_datastore" {
  type        = string
  description = "Datastore that supports ISO images / snippets (e.g. local)"
  default     = "local"
}

variable "network_bridge" {
  type        = string
  description = "Proxmox bridge for VM NICs"
  default     = "vmbr0"
}

variable "talos_schematic_id" {
  type        = string
  description = "Talos Image Factory schematic id (created in Task 2 Step 1)"
}

variable "talos_version" {
  type        = string
  description = "Talos Linux version to deploy (Image Factory tag, e.g. v1.13.5)"
  default     = "v1.13.5"
}
