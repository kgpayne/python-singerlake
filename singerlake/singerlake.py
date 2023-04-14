from __future__ import annotations

import json
from pathlib import Path
from typing import Any, List, Mapping

import base58
import farmhash
import numpy as np

from singerlake.store import BaseStore


class SingerLake:
    """Singer Lake."""

    def __init__(self, store: BaseStore):
        self.store = store

    def hash_stream_schema(self, stream_schema: Mapping[str, Any]) -> str:
        """Calculate a unique short-hash for given schema."""
        data = json.dumps(stream_schema, sort_keys=True)
        int64_hash_bytes = (
            np.uint64(farmhash.fingerprint64(data)).astype("int64").tobytes()
        )
        return base58.b58encode(int64_hash_bytes).decode("utf-8")

    def write_files(
        self,
        tap_id: str,
        stream_id: str,
        stream_schema: Mapping[str, Any],
        files: List[Path],
    ) -> None:
        """Write files to the Lake."""
        stream_schema_hash = self.hash_stream_schema(stream_schema=stream_schema)
        self.store.add_files_to_stream(
            tap_id=tap_id,
            stream_id=stream_id,
            stream_schema_hash=stream_schema_hash,
            files=files,
        )
