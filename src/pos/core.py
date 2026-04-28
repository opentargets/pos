import asyncio
import functools
import os
from contextlib import contextmanager
from pathlib import Path

import click
from click_default_group import DefaultGroup
from loguru import logger
from otter import Runner
from otter.manifest.model import Result
from otter.storage.synchronous.google import GoogleStorage

from pos.gcp.snapshot_disk import snapshot_exists
from pos.gcp.vm import ComputeEngineSSHTunnel
from pos.services.terraform import TerraformError, TerraformRunner, WorkspaceAction
from pos.utils import get_config

OPENSEARCH_PORT = 9200
CLICKHOUSE_PORT = 8123


@contextmanager
def disable_logger(name):
    logger.disable(name)
    try:
        yield
    finally:
        logger.enable(name)


def pos_runner() -> None:
    runner = Runner('pos')
    runner.start()
    runner.register_tasks('pos.tasks')
    s = asyncio.run(runner.run())
    if s.manifest.result not in [Result.PENDING, Result.SUCCESS]:
        logger.error(f'step {s.name} failed')
        raise SystemExit(1)


def common_params(func):
    @click.option(
        '--config-path',
        '-c',
        'config',
        type=click.Path(exists=True, path_type=Path),
        default='config/config.yaml',
        show_default=True,
        help='Path to configuration YAML file.',
    )
    @click.option(
        '--step',
        '-s',
        'step',
        required=True,
        prompt=True,
        help='Step to run.',
    )
    @click.option(
        '--work-path',
        '-w',
        'work_path',
        type=click.Path(),
        help='The local working path. This is where files will be downloaded and '
        'the manifest and logs will be written to.',
    )
    @click.option(
        '--release-uri',
        '-r',
        'release_uri',
        type=click.STRING,
        help='If set, this URI will be used as the release location. This is where '
        'files will be uploaded and the manifest and logs will be written to.'
        'If omitted, the run will be local only.',
    )
    @click.option(
        '--pool-size',
        '-p',
        'pool_size',
        type=click.INT,
        help='The number of worker proccesses that will be spawned to run tasks'
        'in the step in parallel. It should be similar to the number of cores,'
        'but could be higher because there is a lot of I/O blocking.',
    )
    @click.option(
        '--log-level',
        '-l',
        'log_level',
        type=click.Choice(['TRACE', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
        help='Log level for the application.',
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def remote_params(func):
    @click.option(
        '--pos-branch',
        'pos_branch',
        type=click.STRING,
        default='main',
        show_default=True,
        help='The POS git branch to use for the remote run.',
    )
    @click.option(
        '--tfvar',
        'tfvar',
        type=(str, str),
        multiple=True,
        help='Terraform variable overrides as key-value pairs. e.g., --tfvar key value',
    )
    @click.option(
        '--tfvar-file',
        'tfvar_file',
        type=click.Path(exists=True, path_type=Path),
        help='Path to a Terraform variable file.',
    )
    @click.option(
        '--auto-approve',
        'auto_approve',
        is_flag=True,
        default=False,
        show_default=True,
        help='Automatically approve Terraform actions without prompting.',
    )
    @click.option(
        '--terraform-dir',
        'tfdir',
        type=click.Path(exists=True, path_type=Path),
        default=Path(os.getcwd()).joinpath('deployment'),
        show_default=True,
        help='Path to the Terraform configuration directory.',
    )
    @click.option(
        '--workspace',
        'workspace',
        type=click.STRING,
        prompt=True,
        default='default',
        help='Terraform workspace to use.',
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def restore_database_params(func):
    @click.option(
        '--product',
        'product',
        type=click.Choice(['platform', 'ppp']),
        required=True,
        prompt=True,
        help='Platform or PPP',
    )
    @click.option(
        '--target-instance',
        'target_instance',
        type=click.STRING,
        help='OpenSearch/ClickHouse instance to restore.',
    )
    @click.option(
        '--gcp-project',
        'gcp_project',
        type=click.STRING,
        default='open-targets-prod',
        show_default=True,
        help='GCP project where the target database instance is located.',
    )
    @click.option(
        '--gcp-zone',
        'gcp_zone',
        type=click.STRING,
        default='europe-west1-d',
        show_default=True,
        help='GCP zone where the target database instance is located.',
    )
    @click.option(
        '--connection-timeout',
        'connection_timeout',
        type=click.INT,
        default=5,
        show_default=True,
        help='Timeout in seconds for establishing the SSH tunnel.',
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


@click.group(help='Platform Output Support (POS) CLI', cls=DefaultGroup, default='local', default_if_no_args=True)
def pos():
    click.echo('Platform Output Support (POS) CLI')


@pos.command(no_args_is_help=True)
@common_params
def local(config, step, work_path, release_uri, pool_size, log_level) -> None:
    """Run any POS step locally (default command).

    Depending on the step and where you are running this,
    this may not work.
    """
    os.environ['POS_CONFIG_PATH'] = str(config)
    pos_runner()


@pos.command(no_args_is_help=True)
@click.option(
    '--config-path',
    '-c',
    'config',
    type=click.Path(exists=True, path_type=Path),
    default='config/config.yaml',
    show_default=True,
    help='Path to configuration YAML file.',
)
@click.option(
    '--step',
    '-s',
    'step',
    required=True,
    prompt=True,
    help='Step to run.',
)
@click.option(
    '--pool-size',
    '-p',
    'pool_size',
    type=click.INT,
    help='The number of worker proccesses that will be spawned to run tasks'
    'in the step in parallel. It should be similar to the number of cores,'
    'but could be higher because there is a lot of I/O blocking.',
)
@click.option(
    '--os-from-snapshot',
    'opensearch_from_snapshot',
    type=click.STRING,
    help='GCS disk snapshot name to restore OpenSearch from.',
)
@click.option(
    '--ch-from-snapshot',
    'clickhouse_from_snapshot',
    type=click.STRING,
    help='GCS disk snapshot name to restore ClickHouse from.',
)
@click.option(
    '--shutdown-after-run/--no-shutdown-after-run',
    'shutdown_after_run',
    default=True,
    show_default=True,
    help='Whether to shutdown the POS VM after the run is complete.',
)
@remote_params
def remote(
    pos_branch: str,
    config: Path,
    step: str,
    pool_size: int,
    opensearch_from_snapshot: str,
    clickhouse_from_snapshot: str,
    tfdir: Path,
    tfvar: tuple[str, str],
    tfvar_file: Path,
    auto_approve: bool,
    shutdown_after_run: bool,
    workspace: str,
) -> None:
    """Run any POS step remotely on a machine defined by Terraform."""
    terraform = TerraformRunner(tfdir)
    get_config(config_file=str(config))

    tfvars_dict: dict[str, str] = {
        'open_search_disk_name': get_config(str(config)).scratchpad.opensearch_disk_name,
        'clickhouse_disk_name': get_config(str(config)).scratchpad.clickhouse_disk_name,
        'pos_git_branch': pos_branch,
        'pos_step': step,
        'pos_config_file': str(config.absolute()),
        'pos_shutdown_after_run': str(shutdown_after_run).lower(),
    }
    if opensearch_from_snapshot:
        tfvars_dict.update({'open_search_snapshot_source': opensearch_from_snapshot})
    if clickhouse_from_snapshot:
        tfvars_dict.update({'clickhouse_snapshot_source': clickhouse_from_snapshot})
    for key, value in tfvar:
        tfvars_dict.update({key: value})
    if pool_size:
        tfvars_dict.update({'pos_num_processes': str(pool_size)})
    tfvar_file_abs = tfvar_file.absolute() if tfvar_file else None
    try:
        terraform.init()
        terraform.workspace(action=WorkspaceAction.NEW, name=workspace)
        terraform.apply(tfvars=tfvars_dict, tfvar_file=tfvar_file_abs, auto_approve=auto_approve)
    except TerraformError as e:
        click.echo(f'Terraform execution failed: {e}')


@pos.command(no_args_is_help=True)
@click.option(
    '--product',
    'product',
    type=click.Choice(['platform', 'ppp']),
    required=True,
    prompt=True,
    help='Product to create backend for.',
)
@click.option(
    '--pool-size',
    '-p',
    'pool_size',
    type=click.INT,
    default=80,
    show_default=True,
    help='The number of worker proccesses that will be spawned to run tasks'
    'in the step in parallel. It should be similar to the number of cores,'
    'but could be higher because there is a lot of I/O blocking.',
)
@remote_params
def backend(
    product: str,
    pos_branch: str,
    pool_size: int,
    tfdir: Path,
    tfvar: tuple[str, str],
    tfvar_file: Path,
    auto_approve: bool,
    workspace: str,
) -> None:
    """Create platform backend using remote POS execution.

    Use this to creates the following resources:
    - Google Disk snapshots for ClickHouse and OpenSearch
    - OpenSearch snapshot in a remote GCS repository
    - ClickHouse backup in a remote GCS bucket
    """
    config = Path('config').joinpath('config.yaml')
    if product == 'ppp':
        config = Path('config').joinpath('config_ppp.yaml')
    # check snapshot sources
    if _backend_targets_already_exist(config):
        click.echo('Backend targets already exist, aborting backend creation.')
        return
    tfvars_dict: dict[str, str] = {
        'open_search_disk_name': get_config(str(config)).scratchpad.opensearch_disk_name,
        'clickhouse_disk_name': get_config(str(config)).scratchpad.clickhouse_disk_name,
        'pos_git_branch': pos_branch,
        'pos_step': 'backend',
        'pos_config_file': str(config.absolute()),
    }
    terraform = TerraformRunner(tfdir)
    for key, value in tfvar:
        tfvars_dict.update({key: value})
    tfvars_dict.update({'pos_num_processes': str(pool_size)})
    tfvar_file_abs = tfvar_file.absolute() if tfvar_file else None
    try:
        terraform.init()
        terraform.workspace(action=WorkspaceAction.NEW, name=workspace)
        terraform.apply(tfvars=tfvars_dict, tfvar_file=tfvar_file_abs, auto_approve=auto_approve)
    except TerraformError as e:
        click.echo(f'Terraform execution failed: {e}')


@pos.command()
@remote_params
@click.option(
    '--product',
    'product',
    type=click.Choice(['platform', 'ppp']),
    required=True,
    prompt=True,
    help='Product to create backend for.',
)
@click.option(
    '--os-from-snapshot',
    'opensearch_from_snapshot',
    type=click.STRING,
    required=True,
    prompt=True,
    help='GCS disk snapshot name to restore OpenSearch from.',
)
@click.option(
    '--ch-from-snapshot',
    'clickhouse_from_snapshot',
    type=click.STRING,
    required=True,
    prompt=True,
    help='GCS disk snapshot name to restore ClickHouse from.',
)
def tarballs(
    product: str,
    opensearch_from_snapshot: str,
    clickhouse_from_snapshot: str,
    tfvar: tuple[str, str],
    tfdir: Path,
    tfvar_file: Path,
    auto_approve: bool,
    pos_branch: str,
    workspace: str,
) -> None:
    """Create platform tarballs using remote POS execution.

    Use this to creates the following resources:
    - Tarballs for ClickHouse and OpenSearch data directories
    """
    config = Path('config').joinpath('config.yaml')
    if product == 'ppp':
        config = Path('config').joinpath('config_ppp.yaml')
    tfvars_dict: dict[str, str] = {
        'open_search_disk_name': get_config(str(config)).scratchpad.opensearch_disk_name,
        'clickhouse_disk_name': get_config(str(config)).scratchpad.clickhouse_disk_name,
        'open_search_snapshot_source': opensearch_from_snapshot,
        'clickhouse_snapshot_source': clickhouse_from_snapshot,
        'pos_step': 'tarballs',
        'pos_config_file': str(config.absolute()),
        'pos_git_branch': pos_branch,
        'vm_pos_machine_type': 'n1-highcpu-16',
    }
    for key, value in tfvar:
        tfvars_dict.update({key: value})
    tfvar_file_abs = tfvar_file.absolute() if tfvar_file else None
    terraform = TerraformRunner(tfdir)
    try:
        terraform.init()
        terraform.workspace(action=WorkspaceAction.NEW, name=workspace)
        terraform.apply(tfvars=tfvars_dict, tfvar_file=tfvar_file_abs, auto_approve=auto_approve)
    except TerraformError as e:
        click.echo(f'Terraform execution failed: {e}')


@pos.command()
@click.option(
    '--terraform-dir',
    'tfdir',
    type=click.Path(exists=True, path_type=Path),
    default=Path(os.getcwd()).joinpath('deployment'),
    show_default=True,
    help='Path to the Terraform configuration directory.',
)
@click.option(
    '--auto-approve',
    'auto_approve',
    is_flag=True,
    default=False,
    show_default=True,
    help='Automatically approve Terraform actions without prompting.',
)
@click.option(
    '--workspace',
    'workspace',
    type=click.STRING,
    prompt=True,
    default='default',
    help='Terraform workspace to use.',
)
def clean_remote(tfdir: Path, auto_approve: bool, workspace: str) -> None:
    """Clean up remote POS resources after a remote run."""
    tf = TerraformRunner(tfdir)
    try:
        tf.workspace(action=WorkspaceAction.SELECT, name=workspace)
        tf.destroy(auto_approve=auto_approve)
        if workspace != 'default':
            tf.workspace(action=WorkspaceAction.SELECT, name='default')
            tf.workspace(action=WorkspaceAction.DELETE, name=workspace)
    except TerraformError as e:
        click.echo(f'Terraform execution failed: {e}')


@pos.command(no_args_is_help=True)
@click.option(
    '--instance',
    'instance',
    type=click.Choice(['dev', 'prod']),
    required=True,
    prompt=True,
    help='BigQuery instance to initialize.',
)
def bigquery(instance) -> None:
    """Populate BigQuery."""
    config = Path('config').joinpath('config.yaml')
    step = 'bigquery_dev_load_all'
    if instance == 'prod':
        step = 'bigquery_prod_load_all'
    os.environ['POS_CONFIG_PATH'] = str(config)
    os.environ['POS_STEP'] = step
    if instance == 'prod':
        if click.confirm('Release platform data BigQuery prod?'):
            pos_runner()
    else:
        pos_runner()


@pos.command()
@click.option(
    '--product',
    'product',
    type=click.Choice(['platform', 'ppp']),
    required=True,
    prompt=True,
    help='Product to create backend for.',
)
def gcs_sync(product) -> None:
    """Release data to GCS."""
    config = Path('config').joinpath('config.yaml')
    if product == 'ppp':
        config = Path('config').joinpath('config_ppp.yaml')
    step = 'gcs_sync'
    os.environ['POS_CONFIG_PATH'] = str(config)
    os.environ['POS_STEP'] = step
    if click.confirm(f'Release {product} data to GCS?'):
        pos_runner()


@pos.command()
def aws_sync() -> None:
    """Release data to AWS."""
    config = Path('config').joinpath('config.yaml')
    step = 'aws_sync'
    os.environ['POS_CONFIG_PATH'] = str(config)
    os.environ['POS_STEP'] = step
    aws_conf = get_config(str(config)).steps.aws_sync[0]
    source = aws_conf.source
    destination = aws_conf.destination
    if click.confirm(f'Release {source} to {destination}?'):
        pos_runner()


@pos.command()
def ftp_sync():
    """Release data to FTP. Not available for PPP."""
    config = Path('config').joinpath('config.yaml')
    step = 'ftp_sync'
    os.environ['POS_CONFIG_PATH'] = str(config)
    os.environ['POS_STEP'] = step
    if click.confirm('Release platform data to public FTP?'):
        pos_runner()


@pos.command(no_args_is_help=True)
@restore_database_params
@click.option(
    '--local-port',
    'local_port',
    type=click.INT,
    default=CLICKHOUSE_PORT,
    show_default=True,
    help='Local port to bind the SSH tunnel.',
)
@click.option(
    '--remote-port',
    'remote_port',
    type=click.INT,
    default=CLICKHOUSE_PORT,
    show_default=True,
    help='Remote port on the instance to forward to.',
)
def restore_clickhouse(
    product: str,
    target_instance: str,
    gcp_project: str,
    gcp_zone: str,
    connection_timeout: int,
    local_port: int,
    remote_port: int,
) -> None:
    """Restore ClickHouse from a backup."""
    config = Path('config').joinpath('config.yaml')
    if product == 'ppp':
        config = Path('config').joinpath('config_ppp.yaml')
    os.environ['POS_CONFIG_PATH'] = str(config)
    os.environ['POS_STEP'] = 'clickhouse_restore_all'
    if target_instance:
        with ComputeEngineSSHTunnel(
            instance_name=target_instance,
            zone=gcp_zone,
            project=gcp_project,
            local_port=local_port,
            remote_port=remote_port,
            timeout=connection_timeout,
        ) as _:
            click.echo(f'SSH tunnel established to ClickHouse instance {target_instance}.')
            if click.confirm('Restore ClickHouse from latest backup?'):
                pos_runner()
    elif click.confirm('Restore ClickHouse from latest backup? (no ssh tunnel established)'):
        pos_runner()


@pos.command(no_args_is_help=True)
@restore_database_params
@click.option(
    '--local-port',
    'local_port',
    type=click.INT,
    default=OPENSEARCH_PORT,
    show_default=True,
    help='Local port to bind the SSH tunnel.',
)
@click.option(
    '--remote-port',
    'remote_port',
    type=click.INT,
    default=OPENSEARCH_PORT,
    show_default=True,
    help='Remote port on the instance to forward to.',
)
def restore_opensearch(
    product: str,
    target_instance: str,
    gcp_project: str,
    gcp_zone: str,
    connection_timeout: int,
    local_port: int,
    remote_port: int,
) -> None:
    """Restore OpenSearch from an OpenSearch snapshot."""
    config = Path('config').joinpath('config.yaml')
    if product == 'ppp':
        config = Path('config').joinpath('config_ppp.yaml')
    os.environ['POS_CONFIG_PATH'] = str(config)
    os.environ['POS_STEP'] = 'opensearch_restore'
    if target_instance:
        with ComputeEngineSSHTunnel(
            instance_name=target_instance,
            zone=gcp_zone,
            project=gcp_project,
            local_port=local_port,
            remote_port=remote_port,
            timeout=connection_timeout,
        ) as _:
            click.echo(f'SSH tunnel established to OpenSearch instance {target_instance}.')
            if click.confirm('Restore OpenSearch from latest backup?'):
                pos_runner()
    elif click.confirm('Restore OpenSearch from latest backup? (no ssh tunnel established)'):
        pos_runner()


@disable_logger('otter.storage.google')
def _backend_targets_already_exist(config: Path) -> bool:
    scratchpad = get_config(str(config)).scratchpad
    disk_snapshot_project_id = scratchpad.disk_snapshot_project_id
    opensearch_disk_snapshot_name = scratchpad.opensearch_disk_snapshot_name
    clickhouse_disk_snapshot_name = scratchpad.clickhouse_disk_snapshot_name
    database_namespace = scratchpad.database_namespace
    backup_bucket = scratchpad.opensearch_snapshot_bucket
    gcs = GoogleStorage()
    gcs_backups_exists = [
        len(gcs.glob(location=f'gs://{backup_bucket}/opensearch/{database_namespace}/')) > 0,
        len(gcs.glob(location=f'gs://{backup_bucket}/clickhouse/{database_namespace}/')) > 0,
    ]
    if any(gcs_backups_exists):
        logger.warning('GCS backup paths already exist.')
    snaphots_exist = [
        snapshot_exists(project_id=disk_snapshot_project_id, snapshot_name=opensearch_disk_snapshot_name),
        snapshot_exists(project_id=disk_snapshot_project_id, snapshot_name=clickhouse_disk_snapshot_name),
    ]
    if any(snaphots_exist) or any(gcs_backups_exists):
        return True
    return False
