from __future__ import annotations

import json
import typing as t
from collections import namedtuple
from datetime import datetime
from functools import lru_cache

import base58
import farmhash
import numpy as np

from .constant import (
    LAKE_MANIFEST_FILENAME,
    STREAM_MANIFEST_FILENAME,
    TAP_MANIFEST_FILENAME,
)

if t.TYPE_CHECKING:
    from singerlake.config import PartitionBy, PathConfig
    from singerlake.stream.file_writer import SingerFile


Partition = namedtuple("Partition", ["name", "value"])


class GenericPath:

    """Generic path class."""

    def __init__(self, segments: tuple[str, ...], relative: bool = False):
        self.segments = segments
        self.relative = relative

    def __str__(self):
        return "/".join(self.segments)

    def __repr__(self):
        return f"GenericPath({self.segments})"

    def __eq__(self, other):
        return (self.segments == other.segments) and (self.relative == other.relative)

    def __hash__(self):
        return hash(self.segments)

    def extend(self, *args: str) -> "GenericPath":
        """Extend the path."""
        return GenericPath(self.segments + args, relative=self.relative)

    @classmethod
    def from_dict(cls, data: t.Mapping[str, t.Any]) -> "GenericPath":
        """Extend the path with a dict."""
        return GenericPath(data["segments"], relative=data.get("relative", False))

    @classmethod
    def from_model(cls, model: t.Any) -> "GenericPath":
        """Extend the path with a dict."""
        return GenericPath(model.segments, relative=model.relative)


class BasePathTransformer:
    @staticmethod
    def transform(generic_path: GenericPath) -> t.Any:
        """Transform a path."""
        return generic_path


class BasePathManager:
    def __init__(self, config: "PathConfig"):
        self.config = config
        self._lake_root: GenericPath | None = None
        self._transformer = BasePathTransformer()

    @property
    def _generic_lake_root(self) -> GenericPath:
        """Compile the lake root path."""
        if self._lake_root is None:
            self._lake_root = GenericPath.from_model(self.config.lake_root)
        return self._lake_root

    @property
    def _generic_lake_manifest_path(self) -> GenericPath:
        """Compile the lake manifest path."""
        return self._generic_lake_root.extend(*("raw", LAKE_MANIFEST_FILENAME))

    def _generic_tap_path(self, tap_id: str) -> GenericPath:
        """Compile the tap path."""
        return self._generic_lake_root.extend(*("raw", tap_id))

    def _generic_tap_manifest_path(self, tap_id: str) -> GenericPath:
        """Compile the tap manifest path."""
        return self._generic_tap_path(tap_id=tap_id).extend(TAP_MANIFEST_FILENAME)

    def _generic_stream_path(self, tap_id: str, stream_id: str) -> GenericPath:
        """Compile the stream path."""
        return self._generic_tap_path(tap_id=tap_id).extend(stream_id)

    def _generic_stream_manifest_path(self, tap_id: str, stream_id: str) -> GenericPath:
        """Compile the stream manifest path."""
        return self._generic_stream_path(tap_id=tap_id, stream_id=stream_id).extend(
            STREAM_MANIFEST_FILENAME
        )

    def _generic_stream_file_path(self, stream_file: "SingerFile") -> GenericPath:
        """Compile the stream file path."""
        stream_path = self._generic_stream_path(
            tap_id=stream_file.tap_id, stream_id=stream_file.stream_id
        )
        stream_path = stream_path.extend(self.hash_stream_schema(stream_file.schema_))
        for partition in stream_file.partitions:
            stream_path = stream_path.extend(self.format_partition(partition))
        return stream_path.extend(stream_file.name)

    @property
    def lake_root(self) -> t.Any:
        """Get the lake root path."""
        return self.transform(self._generic_lake_root)

    @property
    def lake_manifest_path(self) -> t.Any:
        """Get the lake manifest path."""
        return self.transform(self._generic_lake_manifest_path)

    @property
    def transformer(self) -> BasePathTransformer:
        """Get the path transformer."""
        return self._transformer

    @transformer.setter
    def transformer(self, transformer: BasePathTransformer) -> None:
        """Set the path transformer."""
        self._transformer = transformer

    @property
    def file_partition_by(self) -> t.List["PartitionBy"]:
        """Get the file partition."""
        return self.config.partition_by or []

    @lru_cache
    def get_record_partitions(
        self, time_extracted: "datetime"
    ) -> t.Tuple[Partition, ...]:
        """Partition a record."""
        partitions = [
            Partition(
                name=partition_by.by, value=getattr(time_extracted, partition_by.by)
            )
            for partition_by in self.file_partition_by
        ]
        return tuple(partitions)

    def format_partition(self, partition: Partition) -> str:
        """Format a partition."""
        return str(partition.value)

    def hash_stream_schema(self, stream_schema: t.Mapping[str, t.Any]) -> str:
        """Calculate a unique short-hash for given schema."""
        data = json.dumps(stream_schema, sort_keys=True)
        int64_hash_bytes = (
            np.uint64(farmhash.fingerprint64(data)).astype("int64").tobytes()
        )
        return base58.b58encode(int64_hash_bytes).decode("utf-8")

    @t.final
    def get_tap_path(self, tap_id: str) -> t.Any:
        """Get the tap path."""
        return self.transform(self._generic_tap_path(tap_id=tap_id))

    @t.final
    def get_tap_manifest_path(self, tap_id: str) -> t.Any:
        """Get the tap manifest path."""
        return self.transform(self._generic_tap_manifest_path(tap_id=tap_id))

    @t.final
    def get_stream_path(self, tap_id: str, stream_id: str) -> t.Any:
        """Get the stream path."""
        return self.transform(
            self._generic_stream_path(tap_id=tap_id, stream_id=stream_id)
        )

    @t.final
    def get_stream_manifest_path(self, tap_id: str, stream_id: str) -> t.Any:
        """Get the stream manifest path."""
        return self.transform(self._generic_stream_manifest_path(tap_id, stream_id))

    @t.final
    def get_stream_file_path(self, stream_file: "SingerFile") -> t.Any:
        """Get the stream file path."""
        return self.transform(self._generic_stream_file_path(stream_file))

    def transform(self, path: GenericPath) -> t.Any:
        """Run before returning a path from get methods.

        Override to transform GenericPath to a different path type.
        """
        return self.transformer.transform(path)
