# Data prep task

import json
from collections.abc import Generator
from itertools import chain
from pathlib import Path
from typing import Any

from loguru import logger
from opensearchpy import OpenSearch, helpers
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError

from pos.services.opensearch import OpenSearchInstanceManager
from pos.utils import get_config


class OpenSearchLoadError(OtterError):
    """Base class for exceptions in this module."""


class OpenSearchLoadSpec(Spec):
    """Configuration fields for the create index OpenSearch task."""

    service_name: str = 'os-pos'
    dataset: str
    json_parent: str
    prefix: str
    external_opensearch: bool = False
    es_host: str = 'localhost'
    es_port: int = 9200
    dataset_config_path: str = 'config/datasets.yaml'


class OpenSearchLoad(Task):
    def __init__(self, spec: OpenSearchLoadSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: OpenSearchLoadSpec
        try:
            self._config = get_config(self.spec.dataset_config_path).opensearch
            self._index_name = self._get_index_name()
            self._id_field = self._config[self.spec.dataset].get('id_field')
            self._id_value = self._config[self.spec.dataset].get('id_value')
            self._output_dir = self._config[self.spec.dataset]['output_dir']
        except AttributeError:
            raise OpenSearchLoadError(f'Unable to load config for {self.spec.dataset}')

    @report
    def run(self) -> Task:
        logger.debug(f'loading data into index {self._index_name}')
        if not self.spec.external_opensearch:
            opensearch = OpenSearchInstanceManager(self.spec.service_name).client()
        else:
            opensearch = OpenSearch(
                [{'host': self.spec.es_host, 'port': self.spec.es_port}], use_ssl=False, timeout=7200
            )
        json_files = self._get_json_files()
        if not json_files:
            logger.warning(f'no .json files found for dataset {self.spec.dataset}, no data loaded')
            return self
        for success, info in helpers.parallel_bulk(
            opensearch,
            index=self._index_name,
            actions=chain.from_iterable(self._generate_data(f) for f in json_files),
            thread_count=4,
            chunk_size=2000,
            queue_size=-1,
        ):
            if not success:
                logger.error(f'Failed to index document into {self._index_name}: {info}')
        opensearch.indices.refresh(index=self._index_name)
        return self

    def _generate_data(self, json_file: str | Path) -> Generator[dict[str, Any]] | Generator[str]:
        with open(json_file) as rows:
            if self._id_value:
                logger.info(f'Using {self._id_value} as the document id')
                for row in rows:
                    doc = {'_source': row, '_id': self._id_value}
                    yield doc
            elif self._id_field:
                logger.info(f'Using {self._id_field} as the document id')
                for row in rows:
                    doc = {'_source': row, '_id': json.loads(row)[self._id_field]}
                    yield doc
            else:
                logger.info('no document id field specified')
                for doc in rows:
                    yield doc

    def _get_json_files(self) -> list[Path]:
        json_dir = Path(f'{self.context.config.work_path}/{self.spec.json_parent}/{self._output_dir}')
        return sorted(json_dir.glob('*.json'))

    def _get_index_name(self) -> str:
        return f'{self.spec.prefix}_{self._config[self.spec.dataset]["index"]}'
