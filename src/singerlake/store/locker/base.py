import typing as t

if t.TYPE_CHECKING:
    from singerlake import Singerlake


class BaseLocker:
    """Base Lock."""

    def __init__(self, singerlake: "Singerlake"):
        """Base Lock.

        Args:
            store: SingerLake Store.
            stream: SingerLake Stream.
        """
        self.singerlake = singerlake

    @property
    def instance_id(self):
        """Instance ID."""
        return self.singerlake.instance_id

    @property
    def store(self):
        """Store."""
        return self.singerlake.store

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


class LocalFileLock(BaseLocker):
    """Local File Lock."""

    def acquire(self):
        return super().acquire()

    def release(self):
        return super().release()

    def refresh(self):
        return super().refresh()
