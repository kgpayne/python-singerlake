import json
import typing as t

import base58
import farmhash
import numpy as np

from .constant import (
    LAKE_MANIFEST_FILENAME,
    STREAM_MANIFEST_FILENAME,
    TAP_MANIFEST_FILENAME,
)

if t.TYPE_CHECKING:
    from singerlake.config import PathConfig


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


class BasePathManager:
    def __init__(self, config: "PathConfig"):
        self.config = config
        self.lake_root = GenericPath.from_model(self.config.lake_root)

    def hash_stream_schema(self, stream_schema: t.Mapping[str, t.Any]) -> str:
        """Calculate a unique short-hash for given schema."""
        data = json.dumps(stream_schema, sort_keys=True)
        int64_hash_bytes = (
            np.uint64(farmhash.fingerprint64(data)).astype("int64").tobytes()
        )
        return base58.b58encode(int64_hash_bytes).decode("utf-8")

    @property
    def lake_manifest_path(self) -> GenericPath:
        """Get the lake manifest path."""
        return self.lake_root.extend(*("raw", LAKE_MANIFEST_FILENAME))

    def get_tap_manifest_path(self, tap_id: str) -> GenericPath:
        """Get the tap manifest path."""
        return self.lake_root.extend(*("raw", tap_id, TAP_MANIFEST_FILENAME))

    def get_stream_manifest_path(self, tap_id: str, stream_id: str) -> GenericPath:
        """Get the stream manifest path."""
        return self.lake_root.extend(
            *("raw", tap_id, stream_id, STREAM_MANIFEST_FILENAME)
        )
