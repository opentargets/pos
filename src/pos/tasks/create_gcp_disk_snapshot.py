# Data prep task


import subprocess

from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError

from pos.gcp.labels import GCPLabels
from pos.gcp.snapshot_disk import GCPSnapshotDisk


class CreateGcpDiskSnapshotError(OtterError):
    """Base class for exceptions in this module."""


class CreateGcpDiskSnapshotSpec(Spec):
    """Configuration fields for the GCP Disk Image task."""

    gcp_project_id: str
    gcp_disk_name: str
    gcp_snapshot_name: str  # 'dev-250310-os or dev-250310-ch'
    gcp_disk_zone: str  # 'europe-west1-d'
    mount_point: str  # '/mnt/opensearch' or '/mnt/clickhouse'
    gcp_storage_location: str = 'europe-west1'
    gcp_labels_team: str = 'open-targets'
    gcp_labels_subteam: str = 'backend'
    gcp_labels_product: str = 'platform'
    gcp_labels_tool: str = 'pos'


class CreateGcpDiskSnapshot(Task):
    def __init__(self, spec: CreateGcpDiskSnapshotSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: CreateGcpDiskSnapshotSpec
        self._labels = self._set_labels()

    @report
    def run(self) -> Task:
        snapshot = GCPSnapshotDisk(
            project_id=self.spec.gcp_project_id,
            zone=self.spec.gcp_disk_zone,
            source_disk_name=self.spec.gcp_disk_name,
            snapshot_name=self.spec.gcp_snapshot_name,
            storage_locations=[self.spec.gcp_storage_location],
            labels=self._labels,
        )
        try:
            self._complete_pending_disk_writes()
            self._discard_unused_disk_blocks()
            snapshot.create()
        except (RuntimeError, TimeoutError) as e:
            raise CreateGcpDiskSnapshotError(f'failed to create GCP disk image: {e}')
        return self

    def _complete_pending_disk_writes(self) -> None:
        """Ensure all pending disk writes are completed before snapshotting."""
        complete_pending_disk_writes = subprocess.run(['sync'], check=True)
        if complete_pending_disk_writes.returncode != 0:
            raise CreateGcpDiskSnapshotError('failed to complete pending disk writes before snapshot')

    def _discard_unused_disk_blocks(self) -> None:
        """Discard unused disk blocks to optimize snapshot size."""
        discard_unused_disk_blocks = subprocess.run(['fstrim', '-v', self.spec.mount_point], check=True)
        if discard_unused_disk_blocks.returncode != 0:
            raise CreateGcpDiskSnapshotError('failed to discard unused disk blocks before snapshot')

    def _set_labels(self) -> GCPLabels:
        return GCPLabels(
            team=self.spec.gcp_labels_team,
            subteam=self.spec.gcp_labels_subteam,
            product=self.spec.gcp_labels_product,
            tool=self.spec.gcp_labels_tool,
            release=self.context.scratchpad.sentinel_dict.get('release', ''),
        )
