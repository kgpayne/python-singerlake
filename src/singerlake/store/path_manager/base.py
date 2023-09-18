import json
import typing as t

import base58
import farmhash
import numpy as np

if t.TYPE_CHECKING:
    from singerlake.config import PathConfig


class BasePathManager:
    def __init__(self, config: "PathConfig"):
        self.config = config

    def hash_stream_schema(self, stream_schema: t.Mapping[str, t.Any]) -> str:
        """Calculate a unique short-hash for given schema."""
        data = json.dumps(stream_schema, sort_keys=True)
        int64_hash_bytes = (
            np.uint64(farmhash.fingerprint64(data)).astype("int64").tobytes()
        )
        return base58.b58encode(int64_hash_bytes).decode("utf-8")

    @property
    def lake_root(self):
        """Get the lake root."""
        return self.config.lake_root

    @property
    def lake_manifest_path(self) -> tuple[str]:
        """Get the lake manifest path."""
        return (self.lake_root, "catalog.json")
