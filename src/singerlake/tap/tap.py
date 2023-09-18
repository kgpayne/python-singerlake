import typing as t

from singerlake.stream import Stream

if t.TYPE_CHECKING:
    from singerlake import Singerlake
    from singerlake.manifest.models import TapDefinition


class Tap:
    """Tap."""

    def __init__(
        self, singerlake: "Singerlake", tap_definition: "TapDefinition"
    ) -> None:
        """Tap."""
        self.singerlake = singerlake
        self.tap_definition = tap_definition
        self.stream_cache = None

    @property
    def tap_id(self) -> str:
        """Tap ID."""
        return self.tap_definition.tap_id

    @property
    def streams(self) -> list[Stream]:
        """Streams."""
        if self.stream_cache is None:
            self.stream_cache = {
                stream_definition.stream_id: Stream(
                    singerlake=self.singerlake,
                    tap=self,
                    stream_definition=stream_definition,
                )
                for stream_definition in self.tap_definition.streams
            }
        return [self.stream_cache[stream_id] for stream_id in self.stream_cache]

    def get_stream(self, stream_id: str) -> Stream:
        """Get Stream."""
        return self.stream_cache.get(stream_id)
