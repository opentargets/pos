variable "vm_pos_boot_disk_size" {
  description = "POS VM boot disk size, default '500GB'"
  type        = string
  default     = 600
}

variable "vm_pos_machine_type" {
  description = "Machine type for POS vm, default 'n2-highmem-96'"
  type        = string
  default     = "n2d-standard-64"
}

variable "pos_logs_path_root" {
  description = "GCS root path where POS pipeline logs will be uploaded for the different POS sessions, default 'gs://open-targets-ops/logs/platform-pos'"
  type        = string
  default     = "gs://open-targets-ops/logs/platform-pos"
}

variable "clickhouse_disk_name" {
  description = "Name of the Clickhouse data disk"
  type        = string
  default     = "platform-dev-ch"
}

variable "clickhouse_data_disk_size" {
  description = "Clickhouse data disk size to deploy"
  type        = string
  default     = "400"
}

variable "clickhouse_snapshot_source" {
  description = "Snapshot to use for Clickhouse data disk source"
  type        = string
  default     = null
}

variable "clickhouse_backup_base_path" {
  description = "Base path in GCS bucket where ClickHouse backups will be stored"
  type        = string
  default     = "https://storage.googleapis.com/open-targets-data-backends/clickhouse/"
}

variable "open_search_disk_name" {
  description = "Name of the OpenSearch data disk"
  type        = string
  default     = "platform-dev-os"
}

variable "open_search_data_disk_size" {
  description = "Opensearch data disk size to deploy"
  type        = string
  default     = "400"
}

variable "open_search_snapshot_source" {
  description = "Snapshot to use for OpenSearch data disk source"
  type        = string
  default     = null
}

variable "pos_git_branch" {
  description = "Git branch to use for POS deployment"
  type        = string
  default     = "main"
}

variable "pos_shutdown_after_run" {
  description = "Whether to shutdown the POS VM after the run is complete"
  type        = bool
  default     = true
}

# ---- Otter config ---- #

variable "pos_step" {
  description = "POS step to execute, default 'backend'"
  type        = string
  default     = "backend"
}

variable "pos_num_processes" {
  description = "Number of processes to use for POS step execution, default '10'"
  type        = number
  default     = 10
}

variable "pos_config_file" {
  description = "Path to the POS configuration file to be used in the deployment"
  type        = string
  default     = "../config/config.yaml"
}

