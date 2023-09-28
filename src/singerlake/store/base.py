from __future__ import annotations

import typing as t
from abc import ABC

from singerlake.store.manifest import LakeManifest, TapManifest

if t.TYPE_CHECKING:
    from singerlake import Singerlake
    from singerlake.store.locker.base import BaseLocker
    from singerlake.store.path_manager.base import BasePathManager
    from singerlake.stream.file_writer import SingerFile
    from singerlake.tap.tap import Tap


class BaseStore(ABC):
    """Base SingerLake storage interface."""

    def __init__(
        self,
        singerlake: "Singerlake",
        locker: "BaseLocker",
        path_manager: "BasePathManager",
    ) -> None:
        """Base SingerLake storage interface."""
        self.singerlake = singerlake
        self.locker = locker
        self.path_manager = path_manager

        self._lake_manifest: LakeManifest | None = None
        self._lake_manifest_checksum: str | None = None

    @property
    def lake_root(self) -> t.Any:
        """Return the Lake root path."""
        return self.path_manager.lake_root

    @property
    def lake_manifest(self) -> LakeManifest:
        """Return the Lake Manifest."""
        if self._lake_manifest is None:
            read_lake_manifest = self.read_lake_manifest()
            if read_lake_manifest is not None:
                self._lake_manifest = LakeManifest(**read_lake_manifest)
            else:
                raise ValueError("Lake Manifest not found.")
        return self._lake_manifest

    @t.final
    def get_tap_manifest(self, tap_id: str) -> TapManifest | None:
        """Get a Tap Manifest by ID."""
        read_tap_manifest = self.read_tap_manifest(tap_id=tap_id)
        return None if read_tap_manifest is None else TapManifest(**read_tap_manifest)

    # override these methods to implement a custom store
    def get_lake_root(self) -> t.Any:
        """Return the Lake root path."""
        raise NotImplementedError()

    def read_lake_manifest(self) -> dict | None:
        """Read the Lake Manifest."""
        raise NotImplementedError()

    def read_tap_manifest(self, tap_id: str) -> dict | None:
        """Read a Tap Manifest."""
        raise NotImplementedError()

    def read_stream_manifest(self, tap_id: str, stream_id: str) -> dict | None:
        """Read a Stream Manifest."""
        raise NotImplementedError()

    def create_tap(self, tap_id: str) -> "Tap":
        """Create a Tap."""
        raise NotImplementedError()

    def commit_stream_files(self, stream_files: list["SingerFile"]) -> None:
        """Commit stream files to storage."""
        raise NotImplementedError()

    def write_tap_manifest(self, tap_id: str, manifest: TapManifest) -> TapManifest:
        """Write a Tap Manifest."""
        raise NotImplementedError()
