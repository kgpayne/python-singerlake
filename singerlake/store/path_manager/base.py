import json
from typing import Any, List, Mapping

import base58
import farmhash
import numpy as np


class BasePathManager:
    def __init__(self):
        pass

    def hash_stream_schema(self, stream_schema: Mapping[str, Any]) -> str:
        """Calculate a unique short-hash for given schema."""
        data = json.dumps(stream_schema, sort_keys=True)
        int64_hash_bytes = (
            np.uint64(farmhash.fingerprint64(data)).astype("int64").tobytes()
        )
        return base58.b58encode(int64_hash_bytes).decode("utf-8")
