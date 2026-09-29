# Data prep task


import subprocess
from pathlib import Path
from typing import Self

from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError
from pydantic import model_validator

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
    # Optional smaller disk the data is rsynced to and snapshotted instead of the
    # data disk, so the snapshot can be restored to a disk just big enough for it.
    # Leave compact_disk_name empty to snapshot the data disk directly.
    compact_disk_name: str = ''
    compact_mount_point: str = ''  # '/mnt/opensearch-compact' or '/mnt/clickhouse-compact'
    source_mount_point: str = ''  # root of the data disk: '/mnt/opensearch' or '/mnt/clickhouse'

    @model_validator(mode='after')
    def _check_compact_disk_fields(self) -> Self:
        if self.compact_disk_name and not (self.compact_mount_point and self.source_mount_point):
            raise ValueError('compact_disk_name requires compact_mount_point and source_mount_point')
        return self


class CreateGcpDiskSnapshot(Task):
    def __init__(self, spec: CreateGcpDiskSnapshotSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: CreateGcpDiskSnapshotSpec
        self._labels = self._set_labels()

    @report
    def run(self) -> Task:
        disk_name, trim_mount_point = self.spec.gcp_disk_name, self.spec.mount_point
        if self.spec.compact_disk_name:
            disk_name, trim_mount_point = self.spec.compact_disk_name, self.spec.compact_mount_point
        snapshot = GCPSnapshotDisk(
            project_id=self.spec.gcp_project_id,
            zone=self.spec.gcp_disk_zone,
            source_disk_name=disk_name,
            snapshot_name=self.spec.gcp_snapshot_name,
            storage_locations=[self.spec.gcp_storage_location],
            labels=self._labels,
        )
        try:
            self._complete_pending_disk_writes()
            if self.spec.compact_disk_name:
                self._copy_to_compact_disk()
                self._complete_pending_disk_writes()
            self._discard_unused_disk_blocks(trim_mount_point)
            snapshot.create()
        except (RuntimeError, TimeoutError) as e:
            raise CreateGcpDiskSnapshotError(f'failed to create GCP disk image: {e}')
        return self

    def _complete_pending_disk_writes(self) -> None:
        """Ensure all pending disk writes are completed before snapshotting."""
        complete_pending_disk_writes = subprocess.run(['sync'], check=True)
        if complete_pending_disk_writes.returncode != 0:
            raise CreateGcpDiskSnapshotError('failed to complete pending disk writes before snapshot')

    def _copy_to_compact_disk(self) -> None:
        """Mirror the data disk onto the compact disk.

        Hard links are preserved (-H) as ClickHouse relies on them heavily, otherwise
        the copy could be larger than the source.
        """
        for mount_point in (self.spec.source_mount_point, self.spec.compact_mount_point):
            if not Path(mount_point).is_mount():
                raise CreateGcpDiskSnapshotError(f'{mount_point} is not a mounted disk, is the compact disk attached?')
        subprocess.run(
            [
                'rsync',
                '-aHAX',
                '--delete',
                '--exclude',
                '/lost+found',
                f'{self.spec.source_mount_point.rstrip("/")}/',
                f'{self.spec.compact_mount_point.rstrip("/")}/',
            ],
            check=True,
        )

    def _discard_unused_disk_blocks(self, mount_point: str) -> None:
        """Discard unused disk blocks to optimize snapshot size."""
        discard_unused_disk_blocks = subprocess.run(['fstrim', '-v', mount_point], check=True)
        if discard_unused_disk_blocks.returncode != 0:
            raise CreateGcpDiskSnapshotError('failed to discard unused disk blocks before snapshot')

    def _set_labels(self) -> GCPLabels:
        return GCPLabels(
            team=self.spec.gcp_labels_team,
            subteam=self.spec.gcp_labels_subteam,
            product=self.spec.gcp_labels_product,
            tool=self.spec.gcp_labels_tool,
            release=self.context.scratchpad.sentinel_dict.get('release', '').replace('.', ''),
        )
