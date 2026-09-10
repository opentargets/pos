# Clickhouse start task
import clickhouse_connect
from clickhouse_connect.driver.exceptions import DatabaseError
from loguru import logger
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError

from pos.services.clickhouse import create_database


class ClickhouseCreateDatabaseError(OtterError):
    """Base class for exceptions in this module."""


class ClickhouseCreateDatabaseSpec(Spec):
    """Configuration fields for the Clickhouse create database task."""

    host: str = 'localhost'
    username: str = 'default'
    password: str = ''
    port: int = 8123
    clickhouse_database: str = 'ot'
    exist_ok: bool = False


class ClickhouseCreateDatabase(Task):
    def __init__(self, spec: ClickhouseCreateDatabaseSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: ClickhouseCreateDatabaseSpec

    @report
    def run(self) -> Task:
        logger.debug('Create Clickhouse database')
        try:
            client = clickhouse_connect.get_client(
                host=self.spec.host,
                port=self.spec.port,
                username=self.spec.username,
                password=self.spec.password,
                database='default',
            )
            create_database(client, self.spec.clickhouse_database, exists_ok=self.spec.exist_ok)
        except DatabaseError as db_err:
            raise ClickhouseCreateDatabaseError(f'Create Clickhouse database error : {db_err}') from db_err
        return self
