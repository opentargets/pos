# Data prep task


import subprocess
from pathlib import Path
from typing import Self

from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError
from pydantic import field_validator, model_validator

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
    # A string, as otter validates the spec once before replacing '${...}' placeholders.
    compact_disk_size: str = ''
    source_mount_point: str = ''  # root of the data disk: '/mnt/opensearch' or '/mnt/clickhouse'

    @field_validator('compact_disk_size', mode='before')
    @classmethod
    def _normalise_size(cls, value: object) -> str:
        # scratchpad substitution turns an unset value into '' or 'None'
        if value is None or str(value).strip().lower() in {'', 'none', 'null'}:
            return ''
        return str(value).strip()

    @model_validator(mode='after')
    def _check_compact_disk_fields(self) -> Self:
        size = self.compact_disk_size
        if not size or '${' in size:  # unset, or a placeholder not replaced yet
            return self
        if not size.isdigit() or int(size) <= 0:
            raise ValueError(f'compact_disk_size must be a positive whole number of GB, got {size!r}')
        if not self.source_mount_point:
            raise ValueError('compact_disk_size requires source_mount_point')
        return self

    @property
    def use_compact_disk(self) -> bool:
        return bool(self.compact_disk_size)

    @property
    def compact_disk_name(self) -> str:
        return f'{self.gcp_disk_name}-compact'

    @property
    def compact_mount_point(self) -> str:
        return f'{self.source_mount_point.rstrip("/")}-compact'


class CreateGcpDiskSnapshot(Task):
    def __init__(self, spec: CreateGcpDiskSnapshotSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: CreateGcpDiskSnapshotSpec
        self._labels = self._set_labels()

    @report
    def run(self) -> Task:
        disk_name, trim_mount_point = self.spec.gcp_disk_name, self.spec.mount_point
        if self.spec.use_compact_disk:
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
            if self.spec.use_compact_disk:
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
                f'{self.spec.compact_mount_point}/',
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
