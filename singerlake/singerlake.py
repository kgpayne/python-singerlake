from __future__ import annotations

from uuid import uuid4

from singerlake.store import StoreService


class Singerlake:
    """Singer Lake."""

    def __init__(self, config: dict | None = None):
        self.instance_id = str(uuid4())
        self.config = SingerlakeConfig(config or {})
        self.store = StoreService(
            store_config=config.store,
            lock_config=config.lock,
            paths_config=config.paths,
        )
