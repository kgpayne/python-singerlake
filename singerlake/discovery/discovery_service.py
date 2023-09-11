class DiscoveryService:
    def __init__(self, singerlake, config):
        self.singerlake = singerlake
        self.config = config

    def list_taps(self):
        """List available Taps."""
        lake_manifest = self.singerlake.manifest_service.get_lake_manifest()
        for tap_definition in lake_manifest.taps:
            yield Tap(**tap_definition)
