# Platform Output Support (POS) profile.

# ==== VM config ====

# Custom geos for building the data backend
# vm_pos_machine_type = "n1-standard-8"
# vm_pos_boot_disk_size = 60

# ==== Snapshots ====

# Uncomment the following lines to use existing snapshots for ClickHouse and OpenSearch.
# clickhouse_snapshot_source = "pos-20251001-1113-ch"
# open_search_snapshot_source = "pos-20251001-1113-os"

# ==== Compact disks ====

# Uncomment to create smaller disks that the data is rsynced to before the disk snapshot is taken,
# so the snapshot can be restored to a disk of this size. Must fit the final database data.
clickhouse_compact_disk_size = 170
open_search_compact_disk_size = 60
