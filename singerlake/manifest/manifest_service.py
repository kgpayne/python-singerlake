from .models import LakeManifest


class ManifestService:
    """Manifest Service."""

    def __init__(self, singerlake, config):
        self.singerlake = singerlake
        self.config = config

        self._lake_manifest: LakeManifest | None = None

    @property
    def lake_manifest(self):
        """Get the Lake Manifest."""
        if self._lake_manifest is None:
            (
                raw_lake_manifest,
                lake_manifest_checksum,
            ) = self.singerlake.store.read_lake_manifest()
            if raw_lake_manifest:
                raw_lake_manifest["checksum"] = lake_manifest_checksum
                self._lake_manifest = LakeManifest.model_validate(raw_lake_manifest)

        return self._lake_manifest
