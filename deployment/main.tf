resource "random_string" "posvm" {
  length  = 8
  lower   = true
  upper   = false
  special = false
}

resource "tls_private_key" "posvm" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

#Create the HMAC key for the associated service account 
resource "google_storage_hmac_key" "key" {
  service_account_email = "pos-service-account@open-targets-prod.iam.gserviceaccount.com"
  project               = "open-targets-prod"
}

// Create a disk volume for Clickhouse data
resource "google_compute_disk" "clickhouse_data_disk" {
  project     = "open-targets-eu-dev"
  name        = var.clickhouse_disk_name
  description = "Clickhouse data disk"
  type        = "pd-ssd"
  size        = var.clickhouse_snapshot_source == null ? var.clickhouse_data_disk_size : null
  labels      = local.base_labels
  snapshot    = var.clickhouse_snapshot_source
}

// Create a disk volume for ElasticSearch data
resource "google_compute_disk" "open_search_data_disk" {
  project     = "open-targets-eu-dev"
  name        = var.open_search_disk_name
  description = "OpenSearch data disk"
  type        = "pd-ssd"
  size        = var.open_search_snapshot_source == null ? var.open_search_data_disk_size : null
  labels      = local.base_labels
  snapshot    = var.open_search_snapshot_source
}

// Create a VM instance for the POS service
resource "google_compute_instance" "posvm" {
  name         = "posvm-${random_string.posvm.result}"
  machine_type = var.vm_pos_machine_type
  # instance_termination_action = "DELETE"
  # max_run_duration = "10800s" // 3 hours  
  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      type  = "pd-ssd"
      size  = var.vm_pos_boot_disk_size
    }
  }

  // Attach Clickhouse data disk
  attached_disk {
    source      = google_compute_disk.clickhouse_data_disk.self_link
    device_name = var.clickhouse_disk_name
  }

  // Attach OpenSearch data disk
  attached_disk {
    source      = google_compute_disk.open_search_data_disk.self_link
    device_name = var.open_search_disk_name
  }

  network_interface {
    network = "default"
    access_config {
      // ephemeral public ip
    }
  }

  metadata = {
    pos_config = file(var.pos_config_file)
    startup-script = templatefile(
      "startup.sh",
      {
        POS_USER_NAME        = local.posvm_remote_user_name
        BRANCH               = var.pos_git_branch
        OPENSEARCH_DISK_NAME = var.open_search_disk_name
        CLICKHOUSE_DISK_NAME = var.clickhouse_disk_name
        FORMAT_OS_DISK       = var.open_search_snapshot_source == null ? "true" : "false"
        FORMAT_CH_DISK       = var.clickhouse_snapshot_source == null ? "true" : "false"
        STEP                 = var.pos_step
        NUM_PROCESSES        = var.pos_num_processes
        TIMESTAMP            = local.timestamp
        SHUTDOWN_AFTER_RUN   = var.pos_shutdown_after_run
      }
    )
    ssh-keys               = "${local.posvm_remote_user_name}:${tls_private_key.posvm.public_key_openssh}"
    google-logging-enabled = false
    s3_config = templatefile(
      "s3_config.tftpl",
      {
        GCS_BASE_PATH = var.clickhouse_backup_base_path
        ACCESS_KEY    = google_storage_hmac_key.key.access_id
        SECRET_KEY    = google_storage_hmac_key.key.secret
      }
    )
  }
  service_account {
    email  = "pos-service-account@open-targets-eu-dev.iam.gserviceaccount.com"
    scopes = ["cloud-platform"]
  }

  labels = local.base_labels

  lifecycle {
    create_before_destroy = true
  }
}