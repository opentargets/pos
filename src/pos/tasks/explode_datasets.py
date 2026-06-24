# Explode datasets task

"""Generate tasks by iterating over dataset keys defined in a datasets YAML config."""

from queue import Queue
from typing import Any, Self

from loguru import logger
from otter.scratchpad.model import Scratchpad
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError

from pos.utils import get_config


class ExplodeDatasetsError(OtterError):
    """Base class for exceptions in this module."""


class ExplodeDatasetsSpec(Spec):
    """Configuration fields for the explode_datasets task."""

    dataset_config_path: str = 'config/datasets.yaml'
    """Path to the datasets YAML file whose keys will be iterated over."""
    section: str
    """Top-level section within the datasets YAML to read dataset keys from.
    For example ``clickhouse`` or ``opensearch``."""
    do: list[Spec]
    """The tasks to generate for each dataset. Each task in the list will be
    duplicated once per dataset key found in *section*."""
    each_placeholder: str = 'each'
    """The placeholder string used inside the ``do`` specs to refer to the
    current dataset key.  For example, with the default value ``each``,
    write ``${each}`` in a spec field to have it substituted with the
    dataset name at runtime."""

    def model_post_init(self, __context: Any) -> None:
        # allows keys to be missing from the global scratchpad
        self.scratchpad_ignore_missing = True


class ExplodeDatasets(Task):
    """Generate tasks by iterating over dataset keys in a YAML config section.

    This task reads *section* from *dataset_config_path* and produces one copy
    of every spec in ``do`` for each dataset key found there.  Inside the
    ``do`` specs, ``${each_placeholder}`` is replaced with the dataset key name,
    just like the built-in ``Explode`` task does for its ``foreach`` list.

    .. warning:: ``${each_placeholder}`` **MUST** appear in the ``name`` of
        every spec inside ``do``, because task names must be unique.

    Example:

    .. code-block:: yaml

        steps:
          load_internal_data:
            - name: explode_datasets load all clickhouse datasets
              section: clickhouse
              dataset_config_path: config/internal_datasets.yaml
              each_placeholder: each
              do:
                - name: clickhouse_load ${each}
                  dataset: ${each}
                  clickhouse_database: my_db
                  data_dir_parent: clickhouse_data_to_load
                  dataset_config_path: config/internal_datasets.yaml

    """

    def __init__(self, spec: ExplodeDatasetsSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: ExplodeDatasetsSpec
        self.scratchpad = Scratchpad({})

    @report
    def run(self) -> Self:
        try:
            config = get_config(self.spec.dataset_config_path)
            dataset_keys = list(config[self.spec.section].keys())
        except (KeyError, AttributeError) as exc:
            raise ExplodeDatasetsError(
                f"Unable to read section '{self.spec.section}' "
                f"from '{self.spec.dataset_config_path}': {exc}"
            ) from exc

        logger.debug(
            f"exploding '{self.spec.section}' into "
            f"{len(self.spec.do)} tasks by {len(dataset_keys)} iterations"
        )

        new_tasks = 0
        subtask_queue: Queue[Spec] = self.context.sub_queue
        for dataset_key in dataset_keys:
            for do_spec in self.spec.do:
                self.scratchpad.store(self.spec.each_placeholder, dataset_key)
                subtask_spec = do_spec.model_validate(self.scratchpad.replace_dict(do_spec.model_dump()))
                subtask_spec.task_queue = subtask_queue
                subtask_queue.put(subtask_spec)
                new_tasks += 1

        logger.info(f"exploded into {new_tasks} new tasks")
        # disabled for now to allow python versions < 3.13
        # subtask_queue.shutdown()
        subtask_queue.join()
        return self
