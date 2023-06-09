from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import List, final

from singerlake.models import BaseModel, LakeManifest, StreamManifest, TapManifest
from singerlake.store.const import (
    LAKE_MANIFEST_FILENAME,
    STREAM_MANIFEST_FILENAME,
    TAP_MANIFEST_FILENAME,
)


class BaseStore(ABC):
    """Base SingerLake storage interface."""

    def __init__(self, lake_root: str):
        self._lake_root = lake_root

    @property
    def lake_root(self) -> str:
        """Lake root dir."""
        return self._lake_root

    @property
    def base_prefix(self) -> str:
        """Base prefix for Tap paths."""
        return "raw/"

    def get_lake_path(self) -> str:
        """Compose root path for this Lake."""
        return self.lake_root + self.base_prefix

    def get_tap_path(self, tap_id: str) -> str:
        """Compose path for a given tap_id."""
        return self.get_lake_path() + tap_id

    def get_stream_path(
        self, tap_id: str, stream_id: str, stream_schema_hash: str | None = None
    ) -> str:
        """Compose path for a given Stream by tap_id and stream_id."""
        base_stream_path = self.get_tap_path(tap_id=tap_id) + stream_id
        if stream_schema_hash:
            return base_stream_path + stream_schema_hash
        return base_stream_path

    def get_lake_manifest_path(self) -> str:
        """Compose Lake manifest path."""
        return f"{self.get_lake_path()}/{LAKE_MANIFEST_FILENAME}"

    def get_tap_manifest_path(self, tap_id: str) -> str:
        """Compose Tap manifest path."""
        return f"{self.get_tap_path(tap_id=tap_id)}/{TAP_MANIFEST_FILENAME}"

    def get_stream_manifest_path(self, tap_id: str, stream_id: str) -> str:
        """Compose Stream manifest path."""
        return f"{self.get_stream_path(tap_id=tap_id, stream_id=stream_id)}/{STREAM_MANIFEST_FILENAME}"

    @abstractmethod
    def get_manifest(self, manifest_path: str, model: BaseModel) -> BaseModel:
        """Get Manifest from path."""

    @final
    def get_lake_manifest(self) -> LakeManifest:
        """Read Lake manifest."""
        return self.get_manifest(
            manifest_path=self.get_lake_manifest_path(), model=LakeManifest
        )

    @final
    def get_tap_manifest(self, tap_id: str) -> TapManifest:
        """Read Tap manifest by tap_id."""
        return self.get_manifest(
            manifest_path=self.get_tap_manifest_path(tap_id=tap_id), model=TapManifest
        )

    @final
    def get_stream_manifest(self, tap_id: str, stream_id: str) -> StreamManifest:
        """Read Stream manifest by tap_id and stream_id."""
        return self.get_manifest(
            manifest_path=self.get_stream_manifest_path(
                tap_id=tap_id, stream_id=stream_id
            ),
            model=StreamManifest,
        )

    @abstractmethod
    def write_manifest(self, manifest_path: str, manifest: BaseModel):
        """Write Manifest to given path."""

    @final
    def write_lake_manifest(self, manifest: LakeManifest) -> None:
        """Write Lake manifest."""
        self.write_manifest(
            manifest_path=self.get_lake_manifest_path(), manifest=manifest
        )

    @final
    def write_tap_manifest(self, tap_id: str, manifest: TapManifest) -> None:
        """Write Tap manifest."""
        return self.write_manifest(
            manifest_path=self.get_tap_manifest_path(tap_id=tap_id), manifest=manifest
        )

    @final
    def write_stream_manifest(
        self, tap_id: str, stream_id: str, manifest: StreamManifest
    ) -> None:
        """Write Stream manifest."""
        return self.write_manifest(
            manifest_path=self.get_stream_manifest_path(
                tap_id=tap_id, stream_id=stream_id
            ),
            manifest=manifest,
        )

    @contextmanager
    @abstractmethod
    def lock_lake(self):
        """Context manager to acquire and release lock on this SingerLake.

        Locking is required to update the Lake Manifest.
        """

    @contextmanager
    @abstractmethod
    def lock_tap(self, tap_id: str):
        """Context manager to acquire and release lock on a Tap by tap_id.

        Locking is required to update the Tap Manifest.
        """

    @contextmanager
    @abstractmethod
    def lock_stream(self, tap_id: str, stream_id: str):
        """Context manager to acquire and release lock on a Stream by tap_id and stream_id.

        Locking is required to update the Stream Manifest.
        """

    @abstractmethod
    def write_files_to_stream(self, stream_path: str, files: List[Path]):
        """Write files into Stream path."""

    @final
    def add_files_to_stream(
        self,
        tap_id: str,
        stream_id: str,
        stream_schema_hash: str,
        files: List[Path],
    ) -> None:
        """Write files to Lake."""
        stream_path = self.get_stream_path(
            tap_id=tap_id, stream_id=stream_id, stream_schema_hash=stream_schema_hash
        )
        self.write_files_to_stream(stream_path=stream_path, files=files)
        with self.lock_stream(tap_id=tap_id, stream_id=stream_id):
            manifest = self.get_stream_manifest(tap_id=tap_id, stream_id=stream_id)
            manifest.add_files(
                file_names=[file.name for file in files], schema_hash=stream_schema_hash
            )
            self.write_stream_manifest(
                tap_id=tap_id, stream_id=stream_id, manifest=manifest
            )

    @final
    def add_stream_to_tap(self, tap_id: str, stream_id: str) -> None:
        """Update Tap manifest with Stream.

        TODO: implement
        """
        raise NotImplementedError

    @final
    def add_tap_to_lake(self, tap_id: str) -> None:
        """Update Lake manifest with Tap.

        TODO: implement
        """
        raise NotImplementedError
