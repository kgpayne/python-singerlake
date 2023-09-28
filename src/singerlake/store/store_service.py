import typing as t

from .local import LocalStore
from .locker import LockService
from .path_manager import PathService

if t.TYPE_CHECKING:
    from singerlake import Singerlake
    from singerlake.config import StoreConfig
    from singerlake.store.base import BaseStore


class StoreService:
    def __init__(self, singerlake: "Singerlake", config: "StoreConfig"):
        self.singerlake = singerlake
        self.config = config

    def get_store(self) -> "BaseStore":
        """Return a store instance."""
        locker = LockService(config=self.config.lock).get_locker(
            singerlake=self.singerlake
        )
        path_manager = PathService(config=self.config.path).get_path_manager()
        if self.config.store_type == "local":
            return LocalStore(
                singerlake=self.singerlake, locker=locker, path_manager=path_manager
            )
        raise ValueError(f"Unknown store type: {self.config.store_type}")
