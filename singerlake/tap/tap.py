from singerlake.manifest import ManifestService
from singerlake.store.base import BaseStore
from singerlake.stream.stream import Stream


class Tap:
    """Tap."""

    def __init__(self, singerlake: Singerlake, tap_id: str) -> None:
        """Tap."""
        self.tap_id = tap_id
        self.store = singerlake.store
        self.lock = singerlake.lock
        self.manifest = singerlake.manifest

    def list_streams(self) -> list[str]:
        """List streams."""
        return self.manifest.get_tap_manifest(self.tap_id).streams

    def get_stream(self, stream_id: str) -> Stream:
        """Get Stream."""
        return Stream(singerlake=self.singerlake, tap=self, stream_id=stream_id)
