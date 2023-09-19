from __future__ import annotations

import typing as t

from singerlake.stream import Stream

if t.TYPE_CHECKING:
    from singerlake import Singerlake
    from singerlake.manifest.models import TapManifest


class Tap:
    """Tap."""

    def __init__(self, singerlake: "Singerlake", tap_manifest: "TapManifest") -> None:
        """Tap."""
        self.singerlake = singerlake
        self.tap_manifest = tap_manifest
        self._stream_cache = None

    @property
    def tap_id(self) -> str:
        """Tap ID."""
        return self.tap_manifest.tap_id

    @property
    def streams(self) -> t.Mapping[str, Stream]:
        """Streams."""
        if self._stream_cache is None:
            self._stream_cache = {}
            for stream_id in self.tap_manifest.streams:
                stream_manifest = self.singerlake.manifest_service.get_stream_manifest(
                    tap_id=self.tap_id, stream_id=stream_id
                )
                self._stream_cache[stream_id] = Stream(
                    singerlake=self.singerlake,
                    tap=self,
                    stream_manifest=stream_manifest,
                )
        return self._stream_cache

    @property
    def stream_ids(self) -> list[str]:
        """Stream IDs."""
        return self.tap_manifest.streams

    def get_stream(self, stream_id: str) -> Stream:
        """Get Stream."""
        return self.streams.get(stream_id)
