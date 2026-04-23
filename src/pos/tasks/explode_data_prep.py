# Data prep task

from pathlib import Path
from typing import Literal

from box import Box
from loguru import logger
from otter.scratchpad.model import Scratchpad
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError

from pos.tasks.data_prep import DataPrepSpec
from pos.utils import get_config


class ExplodeDataPrepError(OtterError):
    """Base class for exceptions in this module."""


class ExplodeDataPrepSpec(Spec):
    """Configuration fields for the data prep task.

    This task has the following custom configuration fields:
        - parquet_parent (str): The path or URL of the parquet parent directory.
        i.e. here /path/to/parquet/<dataset>/1.parquet it would be /path/to/parquet
        - json_parent (str): The path or URL of the json parent directory.
        i.e. here /path/to/json/<dataset>/1.json it would be /path/to/json
    """

    parquet_parent: str
    json_parent: str
    dataset: str
    step: str
    dataset_config_path: str = 'config/datasets.yaml'
    mode: Literal['overwrite', 'concatenate'] = 'concatenate'


class ExplodeDataPrep(Task):
    def __init__(self, spec: ExplodeDataPrepSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: ExplodeDataPrepSpec
        self.scratchpad = Scratchpad({})
        try:
            self._config: Box = get_config(self.spec.dataset_config_path)[self.spec.step]
            self._input_dir = Path(self._config[self.spec.dataset].input_dir)
            self._output_dir = Path(self._config[self.spec.dataset].output_dir)
        except AttributeError:
            raise ExplodeDataPrepError(f'Unable to load config for {self.spec.dataset}')
        self.abs_input_dir = self.context.config.work_path.joinpath(self.spec.parquet_parent) / self._input_dir
        self.abs_output_dir = self.context.config.work_path.joinpath(self.spec.json_parent) / self._output_dir
        logger.debug(f'input dir: {self.abs_input_dir}')
        logger.debug(f'output dir: {self.abs_output_dir}')

    @report
    def run(self) -> Task:
        logger.debug(f'Exploding {self.spec.dataset}')
        files = self.abs_input_dir.glob('*.parquet')
        for file in files:
            spec = DataPrepSpec(
                name=f'data_prep {file}',
                source=str(file),
                destination=str(self._get_json_destination()),
            )
            if self.spec.mode == 'overwrite':
                json_file = self._get_json_destination()
                if json_file.exists():
                    json_file.unlink()
            subtask_spec = spec.model_validate(self.scratchpad.replace_dict(spec.model_dump()))
            self.context.specs.append(subtask_spec)
        return self

    def _get_parquet_source(self) -> Path:
        return self.abs_input_dir.joinpath('*.parquet')

    def _get_json_destination(self) -> Path:
        return self.abs_output_dir.joinpath(f'{self.spec.dataset}.json')
