from singerlake.config import PathConfig

from .base import BasePathManager
from .hive import HivePathManager


class PathService:
    """PathService is a factory class that returns a PathManager based on the
    config provided.
    """

    def __init__(self, config: PathConfig):
        self.config = config

    def get_path_manager(self) -> BasePathManager:
        """Return a path manager instance."""
        if self.config.path_type == "hive":
            return HivePathManager(config=self.config)

        raise ValueError(f"Unknown path type: {self.config.path_type}")
