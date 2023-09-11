from __future__ import annotations

from uuid import uuid4

from singerlake.config import SingerlakeConfig
from singerlake.discovery import DiscoveryService
from singerlake.manifest import ManifestService
from singerlake.store import StoreService


class Singerlake:
    """Singer Lake."""

    def __init__(self, config: dict | None = None):
        self.instance_id = str(uuid4())
        self.config = SingerlakeConfig(config or {})
        self.store = StoreService(config=self.config.store)
        self.manifest_service = ManifestService(
            singerlake=self,
            config=self.config.manifest,
        )
        self.discovery_service = DiscoveryService(
            singerlake=self, config=self.config.discovery
        )

    def list_taps(self):
        """Return Taps stored in this Singerlake."""
        return self.discovery_service.list_taps()

    def get_tap(self, tap_id: str):
        """Return a Tap stored in this Singerlake."""
        return self.discovery_service.get_tap(tap_id)
