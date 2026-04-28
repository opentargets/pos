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
    """

    source: str
    destination: str
    config: str = 'config/rclone.conf'


class RcloneSync(Task):
    def __init__(self, spec: RcloneSyncSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: RcloneSyncSpec
        self.context: TaskContext

    @report
    def run(self) -> Task:
        sync = Sync(Path(self.spec.source), Path(self.spec.destination), Path(self.spec.config))
        sync.sync()
        return self
