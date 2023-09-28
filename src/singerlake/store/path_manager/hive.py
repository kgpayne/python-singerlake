import typing as t

from .base import BasePathManager

if t.TYPE_CHECKING:
    from singerlake.store.path_manager import Partition


class HivePathManager(BasePathManager):
    """HivePathManager is a path manager for Hive paths."""

    def format_partition(self, partition: "Partition") -> str:
        """Format a partition."""
        return f"{partition.name}={partition.value}"
