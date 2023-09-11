from singerlake.store.base import BaseStore
from singerlake.store.local import LocalStore


class StoreService:
    def __init__(self, config):
        self.config = config

    def get_store(self) -> BaseStore:
        """Return a store instance."""
        if self.config.type == "local":
            return LocalStore(lake_root=self.config.lake_root)

        raise ValueError(f"Unknown store type: {self.config.type}")
