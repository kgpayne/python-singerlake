import typing as t

from singerlake.manifest.models import StreamManifest

if t.TYPE_CHECKING:
    from singerlake import Singerlake
    from singerlake.tap import Tap


class Stream:
    def __init__(
        self,
        singerlake: "Singerlake",
        tap: "Tap",
        stream_manifest: StreamManifest,
    ) -> None:
        self.singerlake = singerlake
        self.tap = tap
        self.stream_manifest = stream_manifest

    @property
    def stream_id(self) -> str:
        return self.stream_manifest.stream_id
