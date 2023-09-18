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
        self.tap_cache: dict | None = None

    def list_taps(self):
        """List available Taps."""
        if self.tap_cache is None:
            lake_manifest = self.singerlake.manifest_service.get_lake_manifest()
            self.tap_cache = {
                tap_definition["tap_id"]: Tap(**tap_definition)
                for tap_definition in lake_manifest.taps
            }

        yield iter(self.tap_cache.values())

    def get_tap(self, tap_id):
        """Get a Tap by ID."""
        lake_manifest = self.singerlake.manifest_service.get_lake_manifest()
        for tap_definition in lake_manifest.taps:
            if tap_definition["id"] == tap_id:
                return Tap(**tap_definition)
        raise ValueError(f"Tap {tap_id} not found.")
