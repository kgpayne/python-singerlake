from __future__ import annotations

import shutil
import typing as t
from pathlib import Path
from uuid import uuid4

from singerlake.config import SingerlakeConfig
from singerlake.discovery import DiscoveryService
from singerlake.store import BaseStore, StoreService

if t.TYPE_CHECKING:
    from singerlake.tap import Tap


class Singerlake:
    """Singer Lake."""

    def __init__(self, config: dict | None = None):
        """Initialize a Singer Lake instance."""
        config_dict = config or {}

        self.instance_id = str(uuid4())
        self.config = SingerlakeConfig(**config_dict)
        self.discovery_service = DiscoveryService(singerlake=self)

        self._store: BaseStore | None = None
        self._lake_id: str | None = None

    @property
    def lake_id(self) -> str:
        """Return the Lake ID."""
        if self._lake_id is None:
            self._lake_id = self.store.lake_manifest.lake_id
        return self._lake_id

    @property
    def working_dir(self) -> Path:
        """Return the local working directory."""
        if self.config.working_dir:
            working_dir = (
                Path.cwd() / Path(*self.config.working_dir.segments)
                if self.config.working_dir.relative
                else Path(*self.config.working_dir.segments)
            )
        else:
            working_dir = Path.cwd() / ".singerlake"

        working_dir.mkdir(parents=True, exist_ok=True)
        return working_dir

    @property
    def store(self) -> BaseStore:
        """Return the store instance."""
        if self._store is None:
            self._store = StoreService(
                singerlake=self, config=self.config.store
            ).get_store()
        return self._store

    def clean_working_dir(self) -> None:
        """Clean the local working directory."""
        shutil.rmtree(self.working_dir, ignore_errors=True)

    def list_taps(self) -> list[str]:
        """Return Taps stored in this Singerlake."""
        return self.discovery_service.list_taps()

    def get_tap(self, tap_id: str, create: bool = False) -> "Tap" | None:
        """Return a Tap stored in this Singerlake.

        Args:
            tap_id: Tap ID.
            create: If True, create a new Tap if it does not exist.
        """
        tap = self.discovery_service.get_tap(tap_id=tap_id)
        if tap is None and create:
            tap = self.store.create_tap(tap_id=tap_id)
        return tap
