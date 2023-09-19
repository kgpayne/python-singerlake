import hashlib
import json
import typing as t
from pathlib import Path

from .base import BaseStore

if t.TYPE_CHECKING:
    from singerlake.store.path_manager.base import GenericPath

    from .locker.base import BaseLocker
    from .path_manager.base import BasePathManager


class LocalStore(BaseStore):
    """Local directory store."""

    def __init__(self, locker: "BaseLocker", path_manager: "BasePathManager"):
        self.locker = locker
        self.path_manager = path_manager

        self._lake_manifest_checksum = None

    def _md5(self, file_path: Path):
        """Return the md5 checksum of a file."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _read_json(self, file_path: Path):
        """Read a JSON file."""
        with file_path.open("r", encoding="utf-8") as json_file:
            return json.load(json_file)

    def _to_path(self, generic_path: "GenericPath") -> Path:
        """Convert a GenericPath to a pathlib Path."""
        return Path(*generic_path.segments)

    def read_lake_manifest_checksum(self) -> str | None:
        """Read the Lake Manifest checksum."""
        lake_manifest_path = self._to_path(self.path_manager.lake_manifest_path)
        return self._md5(lake_manifest_path)

    def read_lake_manifest(self) -> tuple[dict, str] | None:
        """Read the Lake Manifest."""
        lake_manifest_path = self._to_path(self.path_manager.lake_manifest_path)
        lake_manifest = self._read_json(lake_manifest_path)
        if lake_manifest is not None:
            self._lake_manifest_checksum = self.read_lake_manifest_checksum()
            return lake_manifest

    @property
    def lake_manifest_has_changed(self) -> bool:
        """Return True if the Lake Manifest has changed."""
        return self.read_lake_manifest_checksum() != self._lake_manifest_checksum
