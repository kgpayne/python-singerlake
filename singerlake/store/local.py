import json
import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import List

from filelock import FileLock
from pydantic import BaseModel

from singerlake import (
    LAKE_MANIFEST_FILENAME,
    STREAM_MANIFEST_FILENAME,
    TAP_MANIFEST_FILENAME,
)
from singerlake.store import BaseStore


class LocalStore(BaseStore):
    """Local Disk SingerLake Store."""

    def __init__(self, lake_root: Path):
        self._lake_root = lake_root

    @property
    def lake_root(self) -> Path:
        """Lake root dir."""
        return self._lake_root

    @property
    def base_prefix(self) -> str:
        """Base prefix for Tap paths."""
        return "raw"

    def get_lake_path(self) -> Path:
        """Compose root path for this Lake."""
        return self.lake_root / self.base_prefix

    def get_tap_path(self, tap_id: str) -> Path:
        """Compose path for a given Tap by tap_id."""
        return self.get_lake_path() / tap_id

    def get_stream_path(
        self, tap_id: str, stream_id: str, stream_schema_hash: str | None = None
    ) -> Path:
        """Compose path for a given Stream by tap_id and stream_id."""
        base_stream_path = self.get_tap_path(tap_id=tap_id) / stream_id
        if stream_schema_hash:
            return base_stream_path / stream_schema_hash
        return base_stream_path

    def get_lake_manifest_path(self) -> Path:
        """Compose Lake manifest path."""
        return self.get_lake_path() / LAKE_MANIFEST_FILENAME

    def get_tap_manifest_path(self, tap_id: str) -> Path:
        """Compose Tap manifest path."""
        return self.get_tap_path(tap_id=tap_id) / TAP_MANIFEST_FILENAME

    def get_stream_manifest_path(self, tap_id: str, stream_id: str) -> Path:
        """Compose Stream manifest path."""
        return (
            self.get_stream_path(tap_id=tap_id, stream_id=stream_id)
            / STREAM_MANIFEST_FILENAME
        )

    def get_manifest(self, manifest_path: Path, model: BaseModel) -> BaseModel | None:
        """Read manifest to at path to given model."""
        if manifest_path.exists():
            with manifest_path.open() as manifest:
                data = json.load(manifest)
                return model(**data)

    def write_manifest(self, manifest_path: Path, manifest: BaseModel) -> None:
        """Write manifest to given path."""
        manifest_path.mkdir(parents=True, exist_ok=True)
        with manifest_path.open("w") as manifest_file:
            manifest_file.write(manifest.json())

    @contextmanager
    def lock(self, lockfile_path: Path, timeout):
        lock = FileLock(lockfile_path, timeout=timeout)
        with lock:
            yield

    @contextmanager
    def lock_lake(self, timeout: int | None):
        lockfile_path = Path(f"{self.get_lake_manifest_path()}.lock")
        yield from self.lock(lockfile_path=lockfile_path, timeout=timeout)

    @contextmanager
    def lock_tap(self, tap_id: str, timeout: int | None):
        lockfile_path = Path(f"{self.get_tap_manifest_path(tap_id=tap_id)}.lock")
        yield from self.lock(lockfile_path=lockfile_path, timeout=timeout)

    @contextmanager
    def lock_stream(self, tap_id: str, stream_id: str, timeout: int | None):
        lockfile_path = Path(
            f"{self.get_stream_manifest_path(tap_id=tap_id, stream_id=stream_id)}.lock"
        )
        yield from self.lock(lockfile_path=lockfile_path, timeout=timeout)

    def write_files_to_stream(self, stream_path: Path, files: List[Path]):
        for file in files:
            shutil.copy(file, stream_path / file.name)
