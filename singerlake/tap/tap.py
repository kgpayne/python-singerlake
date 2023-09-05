from singerlake.manifest import ManifestService
from singerlake.store.base import BaseStore
from singerlake.stream.stream import Stream


class Tap:
    """Tap."""

    def __init__(self, tap_id: str, store: BaseStore) -> None:
        """Tap."""
        self.tap_id = tap_id
        self.store = store
        self.manifest = ManifestService.get_tap_manifest(tap_id=tap_id, store=store)

    def get_stream(self, stream_id: str) -> Stream:
        """Get Stream."""
        return Stream(tap=self, stream_id=stream_id, store=self.store)
