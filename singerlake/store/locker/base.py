from singerlake import SingerLake
from singerlake.store import BaseStore
from singerlake.stream import Stream


class BaseLocker:
    """Base Lock."""

    def __init__(self, store: BaseStore, singerlake: SingerLake, stream: Stream = None):
        """Base Lock.

        Args:
            store: SingerLake Store.
            stream: SingerLake Stream.
        """
        self.store = store
        self.stream_id = stream.stream_id if stream else None
        self.instance_id = singerlake.instance_id

    def acquire(self):
        """Acquire lock.

        If a stream is provided, acquire a lock on that stream.
        Otherwise, acquire a lock on the lake.
        """
        raise NotImplementedError()

    def release(self):
        """Release lock.

        If a stream is provided, release the lock on that stream.
        Otherwise, release the lock on the lake.
        """
        raise NotImplementedError()

    def refresh(self):
        """Refresh lock.

        If a stream is provided, refresh the lock on that stream.
        Otherwise, refresh the lock on the lake.
        """
        raise NotImplementedError()

    def __enter__(self):
        """Enter context."""
        self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit context."""
        self.release()


class LocalFileLock(BaseLock):
    """Local File Lock."""

    def acquire(self):
        return super().acquire()

    def release(self):
        return super().release()

    def refresh(self):
        return super().refresh()
