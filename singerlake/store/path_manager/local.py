from singerlake.config import PathConfig


class LocalPathManager:
    """LocalPathManager manages local paths."""

    def __init__(self, config: PathConfig):
        self.config = config

    @property
    def lake_root(self):
        """Get the lake root."""
        return self.config.lake_root

    @property
    def lake_manifest_path(self) -> tuple[str]:
        """Get the lake manifest path."""
        return (self.lake_root, "catalog.json")
