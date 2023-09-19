import typing as t

from .base import BaseLocker

if t.TYPE_CHECKING:
    from singerlake import Singerlake
    from singerlake.config import LockConfig


class LockService:
    def __init__(self, config: "LockConfig"):
        self.config = config

    def get_locker(self, singerlake: "Singerlake"):
        return BaseLocker(singerlake=singerlake)
