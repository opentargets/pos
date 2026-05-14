# Sync from GCS to AWS using rclone

from pathlib import Path

from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report

from pos.rclone.sync import Sync


class RcloneSyncSpec(Spec):
    """Configuration fields for the rclone sync task.

    This task has the following custom configuration fields:
        - source (str): The gcs path to sync from.
        - destination (str): The aws path to sync to.
        - config (str): The path to the rclone config file.
        - include (str): The path to the include file.
        - sync_flags (str): The sync flags to pass to rclone.
    """

    source: str
    destination: str
    config: str = 'config/rclone.conf'
    include: str = 'config/aws_sync.txt'
    sync_flags: str = ''


class RcloneSync(Task):
    def __init__(self, spec: RcloneSyncSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: RcloneSyncSpec
        self.context: TaskContext

    @report
    def run(self) -> Task:
        sync = Sync(
            Path(_remove_storage_prefix(self.spec.source)),
            Path(_remove_storage_prefix(self.spec.destination)),
            Path(self.spec.config),
            Path(self.spec.include),
        )
        flags = self.spec.sync_flags.split(' ') if self.spec.sync_flags else []
        sync.sync(sync_flags=flags)
        return self


def _remove_storage_prefix(source: str) -> str:
    return source.replace('gs://', '').replace('s3://', '')
