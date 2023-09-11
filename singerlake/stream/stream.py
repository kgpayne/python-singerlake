from singerlake.manifest.models import StreamDefinition
from singerlake.singerlake import Singerlake
from singerlake.tap.tap import Tap


class Stream:
    def __init__(
        self,
        singerlake: Singerlake,
        tap: Tap,
        stream_definition: StreamDefinition,
    ) -> None:
        self.singerlake = singerlake
        self.tap = tap
        self.stream_definition = stream_definition

    @property
    def stream_id(self) -> str:
        return self.stream_definition.stream_id
