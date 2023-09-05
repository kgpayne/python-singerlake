from singerlake.locking.base import BaseLock
from singerlake.store.base import BaseStore
from singerlake.stream.config import StreamConfig


class Stream:
    def __init__(
        self, tap: Tap, stream_id: str, store: BaseStore, lock: BaseLock
    ) -> None:
        self.tap = tap
        self.store = store
        self.lock = lock
