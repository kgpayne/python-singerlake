from __future__ import annotations

import hashlib
import json
import shutil
import typing as t
from pathlib import Path

from singerlake.store.manifest import TapManifest
from singerlake.store.path_manager.base import BasePathTransformer
from singerlake.tap import Tap

from .base import BaseStore

if t.TYPE_CHECKING:
    from singerlake import Singerlake
    from singerlake.store.locker.base import BaseLocker
    from singerlake.store.path_manager.base import BasePathManager, GenericPath
    from singerlake.stream.file_writer import SingerFile


class LocalPathTransformer(BasePathTransformer):
    @staticmethod
    def transform(generic_path: GenericPath) -> Path:
        """Transform a GenericPath to a pathlib Path."""
        if generic_path.relative:
            return Path.cwd() / Path(*generic_path.segments)
        return Path(*generic_path.segments)


class LocalStore(BaseStore):
    """Local directory store."""

    def __init__(
        self,
        singerlake: "Singerlake",
        locker: "BaseLocker",
        path_manager: "BasePathManager",
    ) -> None:
        super().__init__(
            singerlake=singerlake, locker=locker, path_manager=path_manager
        )
        self.path_manager.transformer = LocalPathTransformer()

    @property
    def lake_manifest_has_changed(self) -> bool:
        """Return True if the Lake Manifest has changed."""
        return self.read_lake_manifest_checksum() != self._lake_manifest_checksum

    def _md5(self, file_path: Path):
        """Return the md5 checksum of a file."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _read_json(self, file_path: Path):
        """Read a JSON file."""
        if not file_path.exists():
            return None
        with file_path.open("r", encoding="utf-8") as json_file:
            return json.load(json_file)

    def read_lake_manifest_checksum(self) -> str | None:
        """Read the Lake Manifest checksum."""
        return self._md5(self.path_manager.lake_manifest_path)

    def read_lake_manifest(self) -> dict | None:
        """Read the Lake Manifest."""
        lake_manifest = self._read_json(self.path_manager.lake_manifest_path)
        if lake_manifest is not None:
            self._lake_manifest_checksum = self.read_lake_manifest_checksum()
            return lake_manifest
        return None

    # Tap Manifest
    def read_tap_manifest(self, tap_id: str) -> dict | None:
        """Read a Tap Manifest."""
        return self._read_json(self.path_manager.get_tap_manifest_path(tap_id=tap_id))

    # Stream Manifest
    def read_stream_manifest(self, tap_id: str, stream_id: str) -> dict | None:
        """Read a Stream Manifest."""
        return self._read_json(
            self.path_manager.get_stream_manifest_path(
                tap_id=tap_id, stream_id=stream_id
            )
        )

    # Stream Files
    def _commit_stream_file(self, stream_file: "SingerFile") -> None:
        """Commit a singer file to storage."""
        file_path = self.path_manager.get_stream_file_path(stream_file=stream_file)
        if not file_path.parent.exists():
            file_path.parent.mkdir(parents=True)
        shutil.copy(stream_file.path, file_path)

    def commit_stream_files(self, stream_files: list["SingerFile"]) -> None:
        """Commit singer files to storage."""
        for stream_file in stream_files:
            self._commit_stream_file(stream_file=stream_file)

    def create_tap(self, tap_id: str) -> Tap:
        """Create a Tap."""
        file_path = self.path_manager.get_tap_path(tap_id=tap_id)
        file_path.mkdir(parents=True)
        tap_manifest = self.write_tap_manifest(tap_id=tap_id, manifest=TapManifest())
        return Tap(**tap_manifest.dict())

    def write_tap_manifest(self, tap_id: str, manifest: TapManifest) -> TapManifest:
        """Write a Tap Manifest."""
        file_path = self.path_manager.get_tap_manifest_path(tap_id=tap_id)
        with file_path.open("w", encoding="utf-8") as json_file:
            json.dump(manifest.dict(), json_file, indent=2)
        return manifest
