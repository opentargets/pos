# tarball task

import subprocess
from pathlib import Path

from loguru import logger
from otter.manifest.model import Artifact
from otter.storage.synchronous.handle import StorageHandle
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError
from otter.util.fs import check_destination


class TarballError(OtterError):
    """Base class for exceptions in this module."""


class TarballSpec(Spec):
    """Configuration fields for the create tarball task.

    source: The path of the folder to archive
    destination: The path of the tarball to create
    """

    source: Path
    destination: Path


class Tarball(Task):
    def __init__(self, spec: TarballSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: TarballSpec
        self.source: Path = spec.source
        self.local_path: Path = context.config.work_path / spec.destination
        self.remote_uri: str | None = None
        if self.context.config.release_uri:
            self.remote_uri = f'{self.context.config.release_uri}/{self.spec.destination}'
        self.destination = self.remote_uri or self.local_path

    @report
    def run(self) -> Task:
        if not self.source.exists():
            raise TarballError(f'{self.source} does not exist')
        check_destination(self.local_path, delete=True)
        try:
            tar(self.source, self.local_path)
        except subprocess.CalledProcessError as e:
            raise TarballError(f'failed to create tarball: {e.stderr.decode()}')

        # upload the result to remote storage
        if self.remote_uri:
            logger.debug(f'uploading tarball to {self.remote_uri}')
            s = StorageHandle(self.local_path)
            d = StorageHandle(self.remote_uri)
            s.copy_to(d)
            logger.debug('upload successful')
        self.artifacts = [Artifact(source=str(self.source), destination=str(self.destination))]
        return self


def tar(source: Path | str, destination: Path | str) -> None:
    """Create a tarball from a source directory.

    Uses pigz to parallelize the compression.

    Args:
        source: The path of the folder to archive
        destination: The path of the tarball to create
    """
    logger.debug(f'archiving {source} to {destination}')
    subprocess.run(['tar', '--use-compress-program=pigz', '-cf', str(destination), '-C', str(source), '.'], check=True)
