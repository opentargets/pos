"""Parquet to JSON conversion utility."""

import sys
from collections.abc import Iterator
from logging import Logger
from pathlib import Path
from typing import Any

import orjson
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from polars.exceptions import PolarsError
from pyarrow.fs import FileSystem
from smart_open import open


class Parquet2JSONError(Exception):
    """Base class for exceptions in this module."""


class Converter:
    """Parquet to JSON converter class."""

    def __init__(self, log: Logger, hive_partitioning: bool = False):
        self._hive_partitioning = hive_partitioning
        self.log = log

    def get_pyarrow_schema(
        self,
        filesystem: FileSystem,
        path: str,
        parquet_path: str,
    ) -> pa.Schema:
        """Get the schema of a parquet file using pyarrow.

        Currently this is required because of issues with Polars and
        reading list[struct] columns.
        """
        try:
            schema = pq.read_schema(path, filesystem=filesystem)
        except OSError:
            schema = (
                pl
                .read_parquet(
                    parquet_path,
                    n_rows=1,
                    hive_partitioning=self._hive_partitioning,
                )
                .to_arrow()
                .schema
            )
        self.log.debug(f'schema:\n{schema}')
        return schema

    def read_parquet(self, parquet_path: str) -> pl.DataFrame:
        """Read a parquet file and return a DataFrame."""
        try:
            # pyarrow does not automatically detect the filesystem
            path_no_wildcards = parquet_path.split('*', 1)[0]
            filesystem, path = FileSystem.from_uri(path_no_wildcards)
            schema = self.get_pyarrow_schema(filesystem, path, parquet_path)
            return pl.read_parquet(
                path,
                hive_partitioning=self._hive_partitioning,
                use_pyarrow=True,
                pyarrow_options={
                    'filesystem': filesystem,
                    'schema': schema,
                },
            )
        except (FileNotFoundError, pa.ArrowInvalid, PolarsError) as e:
            raise Parquet2JSONError(f'failed to read {parquet_path}: {e}') from e

    def write_json(self, df: pl.DataFrame, path: str) -> None:
        """Write the DataFrame as line-delimited JSON."""
        rows = df.iter_rows(named=True)
        if path is None:
            self._json_lines_to_stdout(rows)
        else:
            Path(path).parent.mkdir(exist_ok=True, parents=True)
            self._json_lines_to_file(rows, path)

    def _drop_nulls_recursively(
        self,
        collection: dict[str, Any] | list[Any],
    ) -> dict[str, Any] | list[Any]:
        """Recursively drop null values from a Dict or List."""
        if isinstance(collection, list):
            return [self._drop_nulls_recursively(x) for x in collection if x is not None]
        if isinstance(collection, dict):
            return {k: (self._drop_nulls_recursively(v)) for k, v in collection.items() if v is not None}
        return collection

    def _serialize_rows(self, rows: Iterator[dict[str, Any]]) -> Iterator[str]:
        """Serialize a row to a JSON string."""
        for row in rows:
            formatted_row = orjson.dumps(self._drop_nulls_recursively(row), option=orjson.OPT_APPEND_NEWLINE).decode(
                'utf-8'
            )
            yield formatted_row

    def _json_lines_to_stdout(self, rows: Iterator[dict[str, Any]]) -> None:
        """Write a list of rows to stdout as JSON lines."""
        sys.stdout.writelines(self._serialize_rows(rows))

    def _json_lines_to_file(self, rows: Iterator[dict[str, Any]], path: str) -> None:
        """Write a list of rows to a file as JSON lines."""
        with open(path, 'a', encoding='UTF-8') as f:
            f.writelines(self._serialize_rows(rows))


def convert(parquet_path: str, json_path: str, log: Logger, hive_partitioning: bool = False) -> None:
    """Convert a parquet file to a JSON file."""
    try:
        converter = Converter(log, hive_partitioning=hive_partitioning)
        log.debug('Reading %s', parquet_path)
        df = converter.read_parquet(parquet_path)
        log.debug('Writing %s', json_path)
        converter.write_json(df, json_path)
    except PolarsError as e:
        raise Parquet2JSONError(e) from e
