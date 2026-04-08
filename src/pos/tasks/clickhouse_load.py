# Clickhouse load task
from pathlib import Path

import clickhouse_connect
from clickhouse_connect.driver.tools import insert_file
from loguru import logger
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError

from pos.services.clickhouse import ClickhouseInstanceManager
from pos.utils import get_config


class ClickhouseLoadError(OtterError):
    """Base class for exceptions in this module."""


class ClickhouseLoadSpec(Spec):
    """Configuration fields for the Load Clickhouse task."""

    service_name: str = 'ch-pos'
    dataset: str
    external_clickhouse: bool = False
    clickhouse_host: str = 'localhost'
    clickhouse_username: str = 'default'
    clickhouse_password: str = ''
    clickhouse_database: str = 'ot'
    clickhouse_port: int = 8123
    data_dir_parent: str
    dataset_config_path: str = 'config/datasets.yaml'


class ClickhouseLoad(Task):
    def __init__(self, spec: ClickhouseLoadSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: ClickhouseLoadSpec
        try:
            self._config = get_config(self.spec.dataset_config_path).clickhouse
            self._table_name = self._config[self.spec.dataset]['table']
            self._input_dir = self._config[self.spec.dataset]['input_dir']
            self._pre_load_sql = self._config[self.spec.dataset].get('preload_script')
            self._post_load_sql = self._config[self.spec.dataset].get('postload_script')
            self._glob_pattern = self._config[self.spec.dataset].get('glob_pattern', '*.parquet')
        except AttributeError:
            raise ClickhouseLoadError(f'unable to load config for {self.spec.dataset}')

    @report
    def run(self) -> Task:
        logger.debug('loading clickhouse service')
        if not self.spec.external_clickhouse:
            clickhouse_client = ClickhouseInstanceManager(
                name=self.spec.service_name,
                host=self.spec.clickhouse_host,
                username=self.spec.clickhouse_username,
                password=self.spec.clickhouse_password,
                database=self.spec.clickhouse_database,
                port=self.spec.clickhouse_port
            ).client()
        else:
            clickhouse_client = clickhouse_connect.get_client(
                host=self.spec.clickhouse_host,
                port=self.spec.clickhouse_port,
                username=self.spec.clickhouse_username,
                password=self.spec.clickhouse_password,
                database=self.spec.clickhouse_database
            )
        if not clickhouse_client:
            raise ClickhouseLoadError(f'Clickhouse service {self.spec.service_name} failed to start')
        # create table tables
        pre_load_statements = Path(self._pre_load_sql).read_text().split(';')
        self._execute_statements(clickhouse_client, pre_load_statements)
        # load data
        files = self._get_parquet_path().glob(self._glob_pattern)
        for file in files:
            logger.debug(f'Inserting file {file} into Clickhouse table {self._table_name}')
            insert_file(clickhouse_client, self._table_name, str(file), fmt='Parquet')
        if self._post_load_sql is not None:
            # run post load sql
            post_load_statements = Path(self._post_load_sql).read_text().split(';')
            self._execute_statements(clickhouse_client, post_load_statements)
        return self

    def _get_parquet_path(self) -> Path:
        return Path(f'{self.context.config.work_path}/{self.spec.data_dir_parent}/{self._input_dir}')

    def _execute_statements(self, client, statements: list[str]) -> None:
        for sql in statements:
            if sql.strip():
                client.query(sql)
