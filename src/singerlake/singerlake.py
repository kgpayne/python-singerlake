from __future__ import annotations

from uuid import uuid4

from singerlake.config import SingerlakeConfig
from singerlake.discovery import DiscoveryService
from singerlake.manifest import ManifestService
from singerlake.store import StoreService


class Singerlake:
    """Singer Lake."""

    def __init__(self, config: dict | None = None):
        """Initialize a Singer Lake instance."""
        config_dict = config or {}

        self.instance_id = str(uuid4())
        self.config = SingerlakeConfig(**config_dict)
        self.manifest_service = ManifestService(singerlake=self)
        self.discovery_service = DiscoveryService(singerlake=self)
        self.store = StoreService(singerlake=self, config=self.config.store).get_store()

    def list_taps(self):
        """Return Taps stored in this Singerlake."""
        return self.discovery_service.list_taps()

    def get_tap(self, tap_id: str):
        """Return a Tap stored in this Singerlake."""
        return self.discovery_service.get_tap(tap_id)
