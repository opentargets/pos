from __future__ import annotations

import sys
from typing import Any

from google.api_core.extended_operation import ExtendedOperation
from google.cloud.compute_v1 import DisksClient, GetSnapshotRequest, Snapshot, SnapshotsClient
from loguru import logger

from pos.gcp.labels import GCPLabels


class GCPSnapshotDisk:
    def __init__(
        self,
        project_id: str,
        zone: str,
        source_disk_name: str,
        snapshot_name: str,
        storage_locations: list[str] | None = None,
        labels: GCPLabels | None = None,
    ) -> None:
        if not labels:
            labels = GCPLabels()
        self._project_id = project_id
        self._zone = zone
        self._source_disk_name = source_disk_name
        self._snapshot_name = snapshot_name
        self._snapshot: Snapshot = Snapshot(
            name=self._snapshot_name, storage_locations=storage_locations, labels=labels.model_dump()
        )

    def create(self) -> None:
        """Creates a new disk image.

        Creates a snapshot of the source disk.
        Then creates an image from the snapshot.
        Then deletes the snapshot.
        """
        logger.debug(f'Creating snapshot {self._snapshot.name} from disk {self._source_disk_name}')
        self._snapshot_disk()

    def _snapshot_disk(self) -> None:
        """Creates a snapshot of the source disk.

        Returns:
            A Snapshot object.
        """
        operation = DisksClient().create_snapshot(
            project=self._project_id, zone=self._zone, snapshot_resource=self._snapshot, disk=self._source_disk_name
        )
        return wait_for_extended_operation(operation, 'snapshot creation from disk')


def wait_for_extended_operation(
    operation: ExtendedOperation, verbose_name: str = 'operation', timeout: int = 1800
) -> Any:
    """Waits for the extended (long-running) operation to complete.

    If the operation is successful, it will return its result.
    If the operation ends with an error, an exception will be raised.
    If there were any warnings during the execution of the operation
    they will be printed to sys.stderr.

    Args:
        operation: a long-running operation you want to wait on.
        verbose_name: (optional) a more verbose name of the operation,
            used only during error and warning reporting.
        timeout: how long (in seconds) to wait for operation to finish.
            If None, wait indefinitely.

    Returns:
        Whatever the operation.result() returns.

    Raises:
        This method will raise the exception received from `operation.exception()`
        or RuntimeError if there is no exception set, but there is an `error_code`
        set for the `operation`.

        In case of an operation taking longer than `timeout` seconds to complete,
        a `concurrent.futures.TimeoutError` will be raised.
    """
    result = operation.result(timeout=timeout)

    if operation.error_code:
        logger.error(
            f'Error during {verbose_name}: [Code: {operation.error_code}]: {operation.error_message}',
            file=sys.stderr,
            flush=True,
        )
        logger.error(f'Operation ID: {operation.name}', file=sys.stderr, flush=True)
        raise operation.exception() or RuntimeError(operation.error_message)

    if operation.warnings:
        logger.warning(f'warnings during {verbose_name}', file=sys.stderr, flush=True)
        for warning in operation.warnings:
            logger.warning(f'{warning.code}: {warning.message}', file=sys.stderr, flush=True)

    return result


def snapshot_exists(project_id: str, snapshot_name: str) -> bool:
    """Check if the snapshot already exists.

    Returns:
        True if the snapshot exists, False otherwise.
    """
    try:
        client = SnapshotsClient()
        request = GetSnapshotRequest(project=project_id, snapshot=snapshot_name)
        snapshot = client.get(request=request)
        if snapshot:
            logger.warning(f'Snapshot {snapshot_name} already exists in project {project_id}.')
            return True
        return False
    except Exception:
        return False
