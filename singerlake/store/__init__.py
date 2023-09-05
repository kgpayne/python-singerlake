from .base import BaseStore
from .local import LocalStore

LAKE_MANIFEST_FILENAME = "manifest.json"
TAP_MANIFEST_FILENAME = "manifest.json"
STREAM_MANIFEST_FILENAME = "manifest.json"

__all__ = [
    "BaseStore",
    "LocalStore",
    "LAKE_MANIFEST_FILENAME",
    "TAP_MANIFEST_FILENAME",
    "STREAM_MANIFEST_FILENAME",
]
