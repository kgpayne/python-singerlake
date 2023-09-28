from __future__ import annotations

import typing as t

from singerlake.tap import Tap

if t.TYPE_CHECKING:
    from singerlake import Singerlake


class DiscoveryService:
    """Discovery Service.

    This service is responsible for discovering Taps available in a given Singerlake.
    """

    def __init__(self, singerlake: "Singerlake"):
        self.singerlake = singerlake
        self._tap_cache: t.List[str] | None = None

    def list_taps(self) -> t.List[str]:
        """List available Taps."""
        if self._tap_cache is None:
            lake_manifest = self.singerlake.store.lake_manifest
            self._tap_cache = lake_manifest.taps

        return self._tap_cache

    def get_tap(self, tap_id) -> Tap | None:
        """Get a Tap by ID."""
        tap_manifest = self.singerlake.store.get_tap_manifest(tap_id=tap_id)
        if tap_manifest:
            return Tap(singerlake=self.singerlake, tap_manifest=tap_manifest)
        return None
