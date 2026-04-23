# OT Croissant Task

import json
from datetime import datetime
from pathlib import Path

from loguru import logger
from ot_croissant.crumbs.metadata import PlatformOutputMetadata  # type: ignore[import-untyped]
from otter.manifest.model import Artifact
from otter.storage.synchronous.handle import StorageHandle
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError, ScratchpadError
from otter.util.fs import check_dir


class OtCroissantError(OtterError):
    """Base class for exceptions in this module."""


class OtCroissantSpec(Spec):
    """Configuration fields for the OT Croissant task.

    This task has the following custom configuration fields:
        - ftp_address (str): The URL of the ftp where the data \
          is going to be published.
        - gcp_address (str): The URL of the google bucket where \
          the data is going to be published.
        - dataset_path (Path): The path where the parquet outputs \
          are stored. These outputs are going to be used to extract \
          the schema.
        - output (str): Path (relative to `work_path` or `release_uri`) \
          to store the metadata at.
        - prepared_data_parent (Path): The parent path where the prepared data \
          is stored. This is the path where OpenSearch will load from.
    """

    ftp_address: str | None = None
    gcp_address: str
    aws_address: str | None = None
    dataset_path: Path
    output: str
    prepared_data_parent: Path


def datetime_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f'Type {type(obj)} not serializable')


class OtCroissant(Task):
    def __init__(self, spec: OtCroissantSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: OtCroissantSpec
        self.local_path: Path = self.context.config.work_path / self.spec.output
        self.prepared_data_path: Path = (
            self.context.config.work_path / self.spec.prepared_data_parent / 'croissant' / self.spec.output
        )
        self.remote_uri: str | None = None
        if context.config.release_uri:
            self.remote_uri = f'{context.config.release_uri}/{self.spec.output}'

    @report
    def run(self) -> Task:
        release = self.context.scratchpad.sentinel_dict.get('release')
        if not release:
            raise ScratchpadError('"release" not found in the scratchpad')

        # Converting the list of paths to a list of strings and prepending the work_path
        logger.debug('converting the list of paths to a list of strings and prepending the work_path')
        # Get the directory path from self.spec.dataset_paths
        directory_path = str(self.context.config.work_path / self.spec.dataset_path)

        # List all sub-folders in the directory
        datasets = [str(path) for path in sorted(Path(directory_path).iterdir()) if path.is_dir()]

        logger.debug(f'generating metadata for release {release}')
        logger.debug(f'ftp: {self.spec.ftp_address}, gcp: {self.spec.gcp_address}')
        logger.debug(f'type of ftp_address: {type(self.spec.ftp_address)}')
        metadata = PlatformOutputMetadata(
            datasets=datasets,
            ftp_location=self.spec.ftp_address,
            gcp_location=self.spec.gcp_address,
            aws_location=self.spec.aws_address,
            version=release,
            date_published=datetime.today().strftime('%Y-%m-%d'),
            data_integrity_hash='sha256',
        )
        logger.debug(f'Metadata generated: {metadata}')

        output_folder = self.local_path.parents[0]
        check_dir(output_folder)

        self._write_formatted_json(metadata)
        self._write_ndjson(metadata)

        # TODO: set all the inputs for artifact. This has to be done after the functionality is implemented in otter
        self.artifacts = [
            Artifact(
                source=f'{self.spec.gcp_address}',
                destination=self.remote_uri or str(self.local_path),
            )
        ]
        return self

    def _write_formatted_json(self, metadata: PlatformOutputMetadata) -> None:
        """Write the metadata to a local and remote file in JSON format."""
        self._write_json(metadata=metadata, path=self.local_path, indent=2, default=datetime_serializer)
        # upload the result to remote storage
        if self.remote_uri:
            logger.info(f'uploading {self.local_path} to {self.remote_uri}')
            s = StorageHandle(self.local_path)
            d = StorageHandle(self.remote_uri)
            s.copy_to(d)
            logger.debug('metadata upload successful')

    def _write_ndjson(self, metadata: PlatformOutputMetadata) -> None:
        """Write the metadata to a local file in NDJSON format as required to upload to OpenSearch."""
        Path(self.prepared_data_path).parent.mkdir(exist_ok=True, parents=True)
        self._write_json(metadata=metadata, path=self.prepared_data_path, indent=None, default=datetime_serializer)

    def _write_json(self, metadata: PlatformOutputMetadata, path: Path, **kwargs) -> None:
        """Write the metadata to a local file in JSON format."""
        with open(path, 'w+') as f:
            logger.debug(f'writting metadata to {path}')
            metadata_json = metadata.to_json()
            json.dump(metadata_json, f, **kwargs)
            f.write('\n')
            logger.debug(f'metadata written successfully to {path}')
