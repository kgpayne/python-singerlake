from __future__ import annotations

import typing as t
from abc import ABC

if t.TYPE_CHECKING:
    from pathlib import Path

    from singerlake.store.locker.base import BaseLocker
    from singerlake.store.path_manager.base import BasePathManager
    from singerlake.stream.stream import Stream


class BaseStore(ABC):
    """Base SingerLake storage interface."""

    def __init__(self, locker: "BaseLocker", path_manager: "BasePathManager") -> None:
        """Base SingerLake storage interface."""
        self.locker = locker
        self.path_manager = path_manager

    def read_tap_manifest(self, tap_id: str) -> dict | None:
        """Read a Tap Manifest."""
        raise NotImplementedError()

    def read_stream_manifest(self, tap_id: str, stream_id: str) -> dict | None:
        """Read a Stream Manifest."""
        raise NotImplementedError()

    def commit_stream_files(self, stream: "Stream", stream_files: list["Path"]) -> None:
        """Commit stream files to storage."""
        raise NotImplementedError()
