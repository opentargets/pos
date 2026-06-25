#!/bin/bash

#SBATCH -J ot_platform_ebi_ftp_sync
#SBATCH -t 08:00:00
#SBATCH --mem=10G
#SBATCH -e /nfs/ftp/private/otftpuser/slurm/logs/ot_platform_ebi_ftp_sync-%J.err
#SBATCH -o /nfs/ftp/private/otftpuser/slurm/logs/ot_platform_ebi_ftp_sync-%J.out
#SBATCH --mail-type=BEGIN,END,FAIL

# This is an SLURM job that uploads Open Targets Platform release data to EBI FTP Service

# Defaults
[ -z "${RELEASE_ID_PROD}" ] && export RELEASE_ID_PROD='dev.default_release_id'
[ -z "${DATA_LOCATION_SOURCE}" ] && export DATA_LOCATION_SOURCE="open-targets-pre-data-releases/${RELEASE_ID_PROD}"
[ -z $PATH_OPS_ROOT_FOLDER ] && echo "PATH to operations root folder is required" && exit 1
[ -z $PATH_OPS_CREDENTIALS ] && echo "PATH to operations credentials folder is required" && exit 1

# TODO - Credentials file default

# Helpers and environment
export job_name="${SLURM_JOB_NAME}-${SLURM_JOB_ID}"
export path_private_base='/nfs/ftp/private/otftpuser'
export path_private_base_ftp_upload="${path_private_base}/opentargets_ebi_ftp_upload"
export path_ebi_ftp_base='/nfs/ftp/public/databases/opentargets/platform'
export path_ebi_ftp_destination="${path_ebi_ftp_base}/${RELEASE_ID_PROD}"
export path_ebi_ftp_destination_latest="${path_ebi_ftp_base}/latest"
export path_slurm_base="${path_private_base}/slurm"
export path_slurm_logs="${path_slurm_base}/logs"
export path_slurm_job_workdir="${path_slurm_base}/${job_name}"
export path_slurm_job_logs="${path_slurm_logs}/${job_name}"
export path_slurm_job_stderr="${path_slurm_job_logs}/output.err"
export path_slurm_job_stdout="${path_slurm_job_logs}/output.out"
export path_slurm_job_sbatch_stderr="${path_slurm_logs}/${job_name}.err"
export path_slurm_job_sbatch_stdout="${path_slurm_logs}/${job_name}.out"
export path_data_source="${DATA_LOCATION_SOURCE}/"
export filename_release_checksum="release_data_integrity"

# Logging functions
function log_heading {
    tag=$1
    shift
    echo -e "[=[$tag]= ---| $@ |--- ]"
}

function log_body {
    tag=$1
    shift
    echo -e "\t[$tag]---> $@"
}

function log_error {
    echo -e "[ERROR] $@"
}

# Environment summary
function print_summary {
    echo -e "[=================================== JOB DATASHEET =====================================]"
    echo -e "\t- Release Number                     : ${RELEASE_ID_PROD}"
    echo -e "\t- Job Name                           : ${job_name}"
    echo -e "\t- PATH Private base                  : ${path_private_base}"
    echo -e "\t- PATH EBI FTP base destination      : ${path_ebi_ftp_base}"
    echo -e "\t- PATH EBI FTP destination folder    : ${path_ebi_ftp_destination}"
    echo -e "\t- PATH EBI FTP destination latest    : ${path_ebi_ftp_destination_latest}"
    echo -e "\t- PATH SLURM base                    : ${path_slurm_base}"
    echo -e "\t- PATH SLURM logs                    : ${path_slurm_logs}"
    echo -e "\t- PATH SLURM Job workdir             : ${path_slurm_job_workdir}"
    echo -e "\t- PATH SLURM Job logs stderr         : ${path_slurm_job_stderr}"
    echo -e "\t- PATH SLURM Job logs stdout         : ${path_slurm_job_stdout}"
    echo -e "\t- PATH SLURM SBATCH Job logs stderr  : ${path_slurm_job_sbatch_stderr}"
    echo -e "\t- PATH SLURM SBATCH Job logs stdout  : ${path_slurm_job_sbatch_stdout}"
    echo -e "\t- PATH Data Source                   : ${path_data_source}"
    echo -e "\t- PATH Operations root folder        : ${PATH_OPS_ROOT_FOLDER}"
    echo -e "\t- PATH Operations credentials folder : ${PATH_OPS_CREDENTIALS}"
    echo -e "[===================================|==============|====================================]"
}

# Prepare destination folders
function make_dirs {
    log_body "MKDIR" "Check/Create ${path_slurm_base}"
    sudo -u otftpuser -- bash -c "mkdir ${path_slurm_base} && chmod 770 ${path_slurm_base}"
    log_body "MKDIR" "Check/Create ${path_slurm_logs}"
    sudo -u otftpuser -- bash -c "mkdir ${path_slurm_logs} && chmod 770 ${path_slurm_logs}"
    log_body "MKDIR" "Check/Create ${path_slurm_job_workdir}"
    sudo -u otftpuser -- bash -c "mkdir ${path_slurm_job_workdir} && chmod 770 ${path_slurm_job_workdir}"
    log_body "MKDIR" "Check/Create ${path_slurm_job_logs}"
    sudo -u otftpuser -- bash -c "mkdir ${path_slurm_job_logs} && chmod 770 ${path_slurm_job_logs}"
    log_body "MKDIR" "Check/Create ${path_ebi_ftp_destination}"
    sudo -u otftpuser -- bash -c "mkdir ${path_ebi_ftp_destination} && chmod 775 ${path_ebi_ftp_destination}"
}

# GCP functions
function activate_service_account {
    log_heading "GCP" "Activating service account at '${PATH_OPS_CREDENTIALS}'"
    singularity exec docker://google/cloud-sdk:latest gcloud auth activate-service-account --key-file=${PATH_OPS_CREDENTIALS}
}

function deactivate_service_account {
    active_account=$(singularity exec docker://google/cloud-sdk:latest gcloud auth list --filter=status:ACTIVE --format="value(account)")
    log_heading "GCP" "Deactivating service account '${active_account}'"
    singularity exec docker://google/cloud-sdk:latest gcloud auth revoke ${active_account}
}

function pull_data_from_gcp {
    log_heading "GCP" "Pulling data from GCP, '${path_data_source}' ---> to ---> '${path_ebi_ftp_destination}"
    singularity exec --bind /nfs/ftp:/nfs/ftp docker://google/cloud-sdk:latest gcloud storage rsync -r -x ^input/fda-inputs/* -x ^output/etl/parquet/failedMatches/* -x ^output/etl/json/failedMatches/* ${path_data_source} ${path_ebi_ftp_destination}/
    log_heading "PERMISSIONS" "Adjusting file tree permissions at '${path_ebi_ftp_destination}'"
    # We don't really need to do this for the production folder, but it's nice to have the permissions set correctly (although you'd need to be 'otftpuser' to do it)
    find ${path_ebi_ftp_destination} -type d -exec chmod 775 {} \;
    find ${path_ebi_ftp_destination} -type f -exec chmod 644 {} \;
    log_heading "GCP" "Done pulling data from GCP"
}

# Helper functions
function compute_checksums {
    log_heading "CHECKSUM" "Compute SHA1 checksum for all the files in this release"
    current_dir=`pwd`
    cd ${path_ebi_ftp_destination}
    find . -type f ! -iname "${filename_release_checksum}*" -exec sha1sum \{} \; > ${filename_release_checksum}
    sha1sum ${filename_release_checksum} > ${filename_release_checksum}.sha1
    log_heading "DATA" "Add the data integrity information back to the source bucket"
    singularity exec --bind /nfs/ftp:/nfs/ftp docker://google/cloud-sdk:latest gsutil cp ${filename_release_checksum}* ${path_data_source}
    cd ${current_dir}
    log_heading "CHECKSUM" "Done computing SHA1 checksum for all the files in this release"
}

function ftp_update_latest_symlink {
    log_heading "FTP" "Update latest symlink"
    log_heading "LATEST" "Update 'latest' link at '${path_ebi_ftp_destination_latest}' to point to '${path_ebi_ftp_destination}'"
    ln -nsf $( basename ${path_ebi_ftp_destination} ) ${path_ebi_ftp_destination_latest}
}

# Bootstrap
function bootstrap {
    log_heading "BOOTSTRAP" "Bootstrapping"
    activate_service_account
    log_heading "FILESYSTEM" "Preparing destination folders"
    make_dirs
    log_heading "BOOTSTRAP" "Done"
}

# Cleanup
function cleanup {
    log_heading "CLEAN" "Cleaning up"
    deactivate_service_account
    log_body "CLEAN" "Remove operations folder at '${PATH_OPS_ROOT_FOLDER}'"
    rm -rf ${PATH_OPS_ROOT_FOLDER}
    log_heading "CLEAN" "Done"
}




# Main
print_summary
log_heading "JOB" "Starting job '${job_name}'"
bootstrap
pull_data_from_gcp
compute_checksums
ftp_update_latest_symlink
cleanup
log_heading "JOB" "END OF JOB ${job_name}"
