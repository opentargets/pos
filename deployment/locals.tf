locals {
  posvm_remote_user_name        = "otops"
  timestamp                     = formatdate("YYYYMMDD-hhmm", timestamp())
  clickhouse_compact_disk_name  = coalesce(var.clickhouse_compact_disk_name, "${var.clickhouse_disk_name}-compact")
  open_search_compact_disk_name = coalesce(var.open_search_compact_disk_name, "${var.open_search_disk_name}-compact")
  // Compact disks are only created when a size is given
  compact_disks = {
    for d in [
      { name = local.clickhouse_compact_disk_name, size = var.clickhouse_compact_disk_size, description = "Clickhouse compact disk for snapshots" },
      { name = local.open_search_compact_disk_name, size = var.open_search_compact_disk_size, description = "OpenSearch compact disk for snapshots" },
    ] : d.name => d if d.size != null
  }
  base_labels = {
    "team"    = "open-targets"
    "subteam" = "backend"
    "product" = "platform"
    "tool"    = "pos"
  }
}