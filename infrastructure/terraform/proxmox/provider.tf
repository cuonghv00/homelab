# infrastructure/terraform/proxmox/provider.tf
provider "proxmox" {
  endpoint  = var.proxmox_endpoint   # e.g. https://192.168.10.X:8006/
  api_token = var.proxmox_api_token  # terraform@pve!provider=<uuid>
  insecure  = true                   # Proxmox uses a self-signed cert by default
}
