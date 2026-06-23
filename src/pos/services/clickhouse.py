"""Clickhouse service module."""

from collections import namedtuple
from dataclasses import asdict, dataclass
from pathlib import Path
from string import Template
from urllib.parse import urljoin

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import DatabaseError
from docker.types.containers import Ulimit
from loguru import logger

from pos.services.containerized_service import ContainerizedService, ContainerizedServiceError, reset_timeout


@dataclass
class ClickhouseBackupQueryParameters:
    database: str
    table: str
    backup_path: str
    export_path: str


class ClickhouseInstanceManagerError(Exception):
    """Base class for exceptions in this module."""


class ClickhouseInstanceManager(ContainerizedService):
    """Clickhouse instance manager.

    Args:
        name: Container name
        dockerfile: Path to Dockerfile (default: 'config/clickhouse/Dockerfile')
        clickhouse_version: Clickhouse version (default: '23.3.1.2823')
        host: Clickhouse host (default: None)
        username: Clickhouse username (default: None)
        password: Clickhouse password (default: '')
        database: Database name (default: 'ot')
        port: Clickhouse HTTP port (default: 8123)
        init_timeout: Initialization timeout in seconds (default: 10)


    Raises:
        ClickhouseInstanceManagerError: If Clickhouse instance manager fails to start
    """

    def __init__(
        self,
        name: str,
        dockerfile: Path = Path('config/clickhouse/Dockerfile'),
        clickhouse_version: str = '23.3.1.2823',
        host: str | None = None,
        username: str | None = None,
        password: str = '',
        database: str = 'default',
        port: int = 8123,
        init_timeout: int = 10,
    ) -> None:
        super().__init__(name, dockerfile, clickhouse_version, init_timeout)
        self.name = name
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.port = port

    def start(self, volume_data: str, volume_logs: str) -> None:
        """Start Clickhouse instance.

        Args:
            volume_data: Data volume
            volume_logs: Logs volume

        Raises:
            ClickhouseInstanceManagerError: If Clickhouse failed to start
        """
        ports: dict[str, int | list[int] | tuple[str, int] | None] | None = {
            '9000': 9000,
            str(self.port): 8123,
            '9363': 9363,
        }
        config_path = str(Path('config/clickhouse/config.d').absolute())
        users_path = str(Path('config/clickhouse/users.d').absolute())

        volumes = {
            volume_data: {'bind': '/var/lib/clickhouse', 'mode': 'rw'},
            volume_logs: {'bind': '/var/log/clickhouse-server', 'mode': 'rw'},
            config_path: {'bind': '/etc/clickhouse-server/config.d', 'mode': 'rw'},
            users_path: {'bind': '/etc/clickhouse-server/users.d', 'mode': 'rw'},
        }
        logger.debug(f'volumes: {volumes}')
        ulimits = [Ulimit(name='nofile', soft=262144, hard=262144)]
        try:
            self._run_container(ports=ports, volumes=volumes, ulimits=ulimits)
        except ContainerizedServiceError:
            raise ClickhouseInstanceManagerError(f'clickhouse instance {self.name} failed to start')

    def client(self, reset_timeout: bool = True) -> Client | None:
        """Get Clickhouse client.

        Args:
            reset_timeout: Reset the timeout (default: True)

        Returns:
            Clickhouse client
        """
        if reset_timeout:
            self.reset_init_timeout()
        client: Client | None = None
        while not client and self._init_timeout > 0:
            if not self.is_running():
                self._wait(1)
                continue
            try:
                client = clickhouse_connect.get_client(
                    host=self.host,
                    username=self.username,
                    password=self.password,
                    database=self.database,
                    port=self.port,
                    send_receive_timeout=600,
                )
            except DatabaseError:
                self._wait(1)
                if self._init_timeout == 0:
                    raise ClickhouseInstanceManagerError(f'Failed to connect to Clickhouse database {self.database}')
                continue
        return client

    @reset_timeout
    def is_healthy(self) -> bool:
        """Check if Clickhouse instance is healthy.

        Returns:
            True if Clickhouse is healthy, False otherwise
        """
        logger.debug('waiting for clickhouse health')
        healthy = False
        while self._init_timeout > 0:
            if not (c := self.client(False)):
                self._wait(1)
                continue
            logger.debug('clickhouse client is available')
            if not c.ping():
                self._wait(1)
                continue
            logger.debug('clickhouse client ping is successful')
            if c.query('SELECT 1').result_set[0][0] == 1:
                healthy = True
                logger.debug('clickhouse is healthy')
                break
            self._wait(1)
        return healthy


def create_database(client: Client, database: str, exists_ok: bool = True) -> None:
    """Create Clickhouse database if it does not exist.

    Args:
        client: Clickhouse client
        database: Database name
        exists_ok: If True, do not raise an error if the database already exists (default: True)
    """
    if exists_ok:
        query = 'CREATE DATABASE IF NOT EXISTS {database:Identifier}'
    else:
        query = 'CREATE DATABASE {database:Identifier}'
    client.query(query=query, parameters={'database': database})


def get_table_engine(client: Client, database: str, table: str) -> str | None:
    """Get the engine type of a ClickHouse table.

    Args:
        client (Client): ClickHouse client instance.
        database (str): Name of the database.
        table (str): Name of the table.

    Returns:
        str | None: The engine type of the table, or None if not found.
    """
    query = Template(
        """SELECT engine \
        FROM system.tables \
        WHERE database='${database}' AND name='${table}'"""  # noqa: RUF027
    ).substitute({'database': database, 'table': table})
    table_engine_query = client.query(query=query).first_row
    return table_engine_query[0] if table_engine_query else None


def backup_table(client: Client, parameters: ClickhouseBackupQueryParameters) -> None:
    """Backup ClickHouse table to S3 compatible storage (GCS).

    Args:
        client (Client): ClickHouse client instance.
        parameters (ClickhouseBackupQueryParameters): Dataclass containing query parameters.

    """
    query = Template("BACKUP TABLE `${database}`.`${table}` TO S3('${backup_path}')").substitute(asdict(parameters))
    client.query(query=query)


def restore_table(client: Client, parameters: ClickhouseBackupQueryParameters) -> None:
    """Restore ClickHouse table from S3 compatible storage (GCS).

    Args:
        client (Client): ClickHouse client instance.
        parameters (ClickhouseBackupQueryParameters): Dataclass containing query parameters.
    """
    query = Template("RESTORE TABLE `${database}`.`${table}` FROM S3('${backup_path}')").substitute(asdict(parameters))
    client.query(query=query)


def export_to_s3(client: Client, parameters: ClickhouseBackupQueryParameters) -> None:
    """Export ClickHouse table to S3 compatible storage (GCS).

    This is used for tables with the EmbeddedRocksDB engine which
    is not supported by the BACKUP/RESTORE commands.

    Args:
        client (Client): ClickHouse client instance.
        parameters (ClickhouseBackupQueryParameters): Dataclass containing query parameters.
    """
    query = Template(
        """INSERT INTO FUNCTION s3(\
                '${export_path}', Parquet) \
                SELECT * FROM `${database}`.`${table}`"""
    ).substitute(asdict(parameters))
    client.query(query)


def import_from_s3(client: Client, parameters: ClickhouseBackupQueryParameters) -> None:
    """Import ClickHouse table from S3 compatible storage (GCS).

    This is used for tables with the EmbeddedRocksDB engine which
    is not supported by the BACKUP/RESTORE commands.

    Args:
        client (Client): ClickHouse client instance.
        parameters (ClickhouseBackupQueryParameters): Dataclass containing query parameters.
    """
    query = Template("INSERT INTO `${database}`.`${table}` SELECT * FROM s3('${export_path}')").substitute(
        asdict(parameters)
    )
    client.query(query)


BackupURLs = namedtuple('BackupURLs', ['backup_url', 'export_url'])


def make_backup_urls(gcs_base_path: str, database: str, table: str) -> BackupURLs:
    """Make ClickHouse backup and export URLs.

    Args:
        gcs_base_path: GCS base path
        database: Database name
        table: Table name

    Returns:
        Named tuple with backup and export URLs
    """
    backup_url = urljoin(
        gcs_base_path,
        '/'.join([
            database,
            table,
        ])
        + '/',
    )
    export_url = urljoin(backup_url, 'export.parquet.lz4')
    return BackupURLs(backup_url=backup_url, export_url=export_url)
