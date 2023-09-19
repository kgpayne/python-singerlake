from __future__ import annotations

import typing as t
from abc import ABC

if t.TYPE_CHECKING:
    from singerlake.config import StoreConfig


class BaseStore(ABC):
    """Base SingerLake storage interface."""

    def __init__(self, locker: BaseLocker, path_manager: BasePathManager) -> None:
        """Base SingerLake storage interface."""
        self.locker = locker
        self.path_manager = path_manager
