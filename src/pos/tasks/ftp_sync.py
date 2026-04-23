# FTP sync

import os
from importlib.resources import files
from pathlib import Path

from loguru import logger
from otter.storage.synchronous.google import GoogleStorage
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from paramiko import SSHClient


class FtpSyncError(Exception):
    """Base class for exceptions in this module."""


class FtpSyncSpec(Spec):
    """Configuration fields for the Ftp sync task."""

    source_data: str
    release_id: str
    gcs_credentials_file_remote: str
    host: str = 'codon-slurm-login'
    slurm_queue: str = 'datamover'
    remote_working_dir: str = 'ot-ops'


class FtpSync(Task):
    def __init__(self, spec: FtpSyncSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: FtpSyncSpec
        self.context: TaskContext
        self.ftp_sync_script = files('scripts').joinpath('ftp_sync.sh').read_text()
        self.remote_credentials_path = f'{self.spec.remote_working_dir}/credentials/gcp_credentials.json'

    @report
    def run(self) -> Task:
        with self._ssh_client() as ssh:
            self._write_files_to_server(ssh)
            self._launch_ftp_sync(ssh)
        return self

    def _ssh_client(self) -> SSHClient:
        client = SSHClient()
        client.load_system_host_keys()
        client.connect(self.spec.host, username=os.getenv('USER'))
        return client

    def _get_credentials_file(self) -> str:
        google_storage = GoogleStorage()
        content, _ = google_storage.read_text(self.spec.gcs_credentials_file_remote)
        return content

    def _write_files_to_server(self, ssh: SSHClient) -> None:
        with ssh.open_sftp() as sftp:
            sftp.chdir(self._get_remote_home(ssh))
            sftp.mkdir(self.spec.remote_working_dir, mode=0o750)
            sftp.mkdir(str(Path(self.remote_credentials_path).parent), mode=0o750)
            logger.debug('writing gcs credentials file to remote server')
            with sftp.open(self.remote_credentials_path, 'w') as f:
                f.write(self._get_credentials_file())
            sftp.chmod(self.remote_credentials_path, 0o640)
            logger.debug('writing ftp sync script to remote server')
            with sftp.open(f'{self.spec.remote_working_dir}/ftp_sync.sh', 'w') as f:
                f.write(self.ftp_sync_script)
            sftp.chmod(f'{self.spec.remote_working_dir}/ftp_sync.sh', 0o750)

    def _get_remote_home(self, ssh: SSHClient) -> str:
        _, stdout, _ = ssh.exec_command('echo $HOME')
        return stdout.read().decode().strip()

    def _launch_ftp_sync(self, ssh: SSHClient) -> None:
        launch_command = (
            f'export PATH_OPS_ROOT_FOLDER={self._get_remote_home(ssh)}/{self.spec.remote_working_dir};'
            f'export PATH_OPS_CREDENTIALS={self._get_remote_home(ssh)}/{self.remote_credentials_path};'
            f'export RELEASE_ID_PROD={self.spec.release_id};'
            f'export DATA_LOCATION_SOURCE={self.spec.source_data};'
            f'sbatch --partition \
                {self.spec.slurm_queue} {self._get_remote_home(ssh)}/{self.spec.remote_working_dir}/ftp_sync.sh'
        )
        _, s, _ = ssh.exec_command(launch_command)
        logger.info(f'{s.read().decode()}')
