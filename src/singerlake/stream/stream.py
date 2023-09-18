import typing as t

from singerlake.manifest.models import StreamDefinition

if t.TYPE_CHECKING:
    from singerlake import Singerlake
    from singerlake.tap import Tap


class Stream:
    def __init__(
        self,
        singerlake: "Singerlake",
        tap: "Tap",
        stream_definition: StreamDefinition,
    ) -> None:
        self.singerlake = singerlake
        self.tap = tap
        self.stream_definition = stream_definition

    @property
    def stream_id(self) -> str:
        return self.stream_definition.stream_id
