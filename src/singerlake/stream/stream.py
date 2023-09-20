import typing as t
from contextlib import contextmanager
from pathlib import Path

from singerlake.manifest.models import StreamManifest

from .record_writer import RecordWriter

if t.TYPE_CHECKING:
    from singerlake import Singerlake
    from singerlake.tap import Tap


class Stream:
    """Singerlake Stream object.

    Responsible for:
    - writing records to files on disk
    - committing files to storage
    """

    def __init__(self, singerlake: "Singerlake", tap: "Tap", stream_id: str) -> None:
        self.singerlake = singerlake
        self.tap = tap
        self.stream_id = stream_id

        self.files: list[Path] = []

    @contextmanager
    def record_writer(self):
        """Create a record writer for this stream."""
        writer = RecordWriter(stream=self, output_dir=self.singerlake.working_dir)
        try:
            writer.open()
            yield writer
        finally:
            writer.close()
            self.files.extend(writer.files)

    def commit(self):
        """Commit stream files to storage."""
        raise NotImplementedError
        # self.singerlake.store.commit_stream_files(stream=self, stream_files=self.files)
