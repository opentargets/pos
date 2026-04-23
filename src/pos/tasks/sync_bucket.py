# Sync bucket

import subprocess

from loguru import logger
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.fs import check_dir


class SyncBucketError(Exception):
    """Base class for exceptions in this module."""


class SyncBucketSpec(Spec):
    """Configuration fields for the sync bucket task.

    This task has the following custom configuration fields:
        - source (str): The gcloud URL of the parquet file/directory or files.
        - destination (Path): The path, relative to `work_path` to download the
            outputs to.
    """

    source: str
    destination: str


class SyncBucket(Task):
    def __init__(self, spec: SyncBucketSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: SyncBucketSpec
        self.context: TaskContext

    @report
    def run(self) -> Task:
        if self.spec.destination.startswith('gs://'):
            destination_folder = self.spec.destination
        else:
            destination_folder = self.context.config.work_path.joinpath(self.spec.destination)
            logger.debug(f'checking if {destination_folder} exists. If not, create it')
            # Checking if the destination folder exists. If not, create it.
            check_dir(destination_folder)

        logger.debug(f'syncing {self.spec.source} with {self.spec.destination}')
        rsync_command = [  # refactor to use gcloud storage api - removes dep on local install of gsutil
            'gsutil',
            '-m',
            'rsync',
            '-r',
            self.spec.source,
            str(destination_folder),
        ]
        subprocess.run(rsync_command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        return self
