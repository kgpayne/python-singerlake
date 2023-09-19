import typing as t

from .models import LakeManifest, TapManifest

if t.TYPE_CHECKING:
    from singerlake import Singerlake


class ManifestService:
    """Manifest Service."""

    def __init__(self, singerlake: "Singerlake"):
        self.singerlake = singerlake

        self._lake_manifest: LakeManifest | None = None

    @property
    def lake_manifest(self):
        """Get the Lake Manifest."""
        if self._lake_manifest is None:
            raw_lake_manifest = self.singerlake.store.read_lake_manifest()
            if raw_lake_manifest:
                self._lake_manifest = LakeManifest(**raw_lake_manifest)

        return self._lake_manifest

    def get_tap_manifest(self, tap_id: str):
        """Get a Tap Manifest by ID."""
        tap_manifest = self.singerlake.store.read_tap_manifest(tap_id=tap_id)
        if tap_manifest:
            return TapManifest(**tap_manifest)
