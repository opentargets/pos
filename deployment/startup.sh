#!/bin/bash

flag_startup_completed="/tmp/posvm_startup_complete"

function startup_complete() {
  log "Startup script completed"
  touch $${flag_startup_completed}
}

# Set trap to run 'startup_complete' function on exit
trap startup_complete EXIT

# Check if startup script has already been run
if [[ -f $${flag_startup_completed} ]]; then
  log "Startup script already completed, skipping"
  exit 0
fi

function log() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@"
}

function create_dir_for_group() {
  local dir=$1
  local group=$2
  local mode=$3
  mkdir -p $${dir}
  chgrp -R $${group} $${dir}
  chmod -R g+$${mode} $${dir}
}

function install_packages() {
    # Install packages
    apt-get remove -y --purge man-db
    apt-get update -y
    apt-get install -y wget vim curl git htop pigz ca-certificates gnupg lsb-release zip unzip

    # Install Java with SDKMAN (required for croissant)
    curl -s "https://get.sdkman.io?ci=true" | bash
    source "/.sdkman/bin/sdkman-init.sh"
    yes | sdk install java 17.0.15-amzn
    export JAVA_HOME=$(sdk home java current)

    # Install Docker
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian \
      $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
    apt-get update -y
    apt-get install -y docker-ce docker-ce-cli containerd.io
    usermod -aG docker ${POS_USER_NAME}

    # Install UV
    export HOME=/home/${POS_USER_NAME}
    curl -LsSf https://astral.sh/uv/install.sh | sh
    create_dir_for_group /home/${POS_USER_NAME} google-sudoers rwx
    source "$HOME/.local/bin/env"

    # Install POS repo, get the config and setup the log locations.
    git clone https://github.com/opentargets/pos.git /opt/pos
    cd /opt/pos
    git checkout ${BRANCH}
    create_dir_for_group /opt/pos google-sudoers rwx
    curl "http://metadata.google.internal/computeMetadata/v1/instance/attributes/pos_config" -H "Metadata-Flavor: Google" > /etc/opt/pos_config.yaml
    uv --directory /opt/pos sync
    curl "http://metadata.google.internal/computeMetadata/v1/instance/attributes/pos_run_script" -H "Metadata-Flavor: Google" > /opt/pos_run.sh
    chgrp -R google-sudoers /opt/pos_run.sh && chmod g+x /opt/pos_run.sh
    create_dir_for_group /var/log/pos/opensearch google-sudoers rwx
    create_dir_for_group /var/log/pos/clickhouse google-sudoers rwx
    curl "http://metadata.google.internal/computeMetadata/v1/instance/attributes/s3_config" -H "Metadata-Flavor: Google" > /opt/pos/config/clickhouse/config.d/s3_config.xml
}


function mount_disk() {
  local disk_name=$1
  local mount_point=$2
  local format_disk=$3
  # Format the disk using ext4 with no reserved blocks
  prefix="/dev/disk/by-id/google-"
  device_name="$${prefix}$${disk_name}"
  if [[ $${format_disk} == true ]]; then
    log "Formatting disk $${device_name} with ext4"
    mkfs.ext4 -m 0 $${device_name}
  else
    log "Disk $${device_name} will not be formatted"
  fi
  # Mount the disk
  log "Mounting disk $${device_name} to $${mount_point}"
  mkdir -p $${mount_point}
  mount -o defaults $${device_name} $${mount_point}
  create_dir_for_group $${mount_point} google-sudoers rw
}

function uv_run() {
    step=$1
    processes=$2
    if [ -z "$processes" ]; then
        processes=10
    fi
    GCE_METADATA_MTLS_MODE=none uv --directory /opt/pos run pos -c /etc/opt/pos_config.yaml -p $processes -s $step
}

function copy_clickhouse_configs() {
  log "[INFO] Syncing ClickHouse configs"
  mkdir -p /mnt/clickhouse/config.d
  mkdir -p /mnt/clickhouse/users.d
  cp -vR /opt/pos/config/clickhouse/config.d/config.xml /mnt/clickhouse/config.d/config.xml
  cp -vR /opt/pos/config/clickhouse/users.d /mnt/clickhouse/
}


# Main script

install_packages
mount_disk ${OPENSEARCH_DISK_NAME} /mnt/opensearch ${FORMAT_OS_DISK}
mount_disk ${CLICKHOUSE_DISK_NAME} /mnt/clickhouse ${FORMAT_CH_DISK}
create_dir_for_group /mnt/opensearch/data google-sudoers rw
create_dir_for_group /mnt/clickhouse/data google-sudoers rw
copy_clickhouse_configs

uv_run ${STEP} ${NUM_PROCESSES} > /var/log/pos.log 2>&1

if [[ ${SHUTDOWN_AFTER_RUN} == true ]]; then
  journalctl -u google-startup-scripts.service > /var/log/google-startup-scripts.log
  gsutil -m cp /var/log/google-startup-scripts.log gs://open-targets-ops/logs/platform-pos/${TIMESTAMP}/pos/google-startup-scripts.log
  gsutil -m cp /var/log/pos.log gs://open-targets-ops/logs/platform-pos/${TIMESTAMP}/pos/pos.log
  poweroff
fi
