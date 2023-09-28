import typing as t
from contextlib import contextmanager

from .file_writer import SingerFile
from .record_writer import RecordWriter

if t.TYPE_CHECKING:
    from datetime import datetime

    from singerlake import Singerlake
    from singerlake.store.path_manager.base import Partition
    from singerlake.tap import Tap


class Stream:
    """Singerlake Stream object.

    Responsible for:
    - writing records to files on disk
    - committing files to storage
    """

    def __init__(
        self,
        singerlake: "Singerlake",
        tap: "Tap",
        stream_id: str,
    ) -> None:
        self.singerlake = singerlake
        self.tap = tap
        self.stream_id = stream_id

        self.files: list[SingerFile] = []

    def partition_record(self, time_extracted: "datetime") -> t.Tuple["Partition", ...]:
        """Partition a record."""
        return self.singerlake.store.path_manager.get_record_partitions(time_extracted)

    @contextmanager
    def record_writer(self):
        """Create a record writer for this stream."""
        writer = RecordWriter(
            stream=self,
            output_dir=self.singerlake.working_dir,
        )
        try:
            yield writer
        finally:
            writer.finalize()
            self.files.extend(writer.singer_files)

    def commit(self):
        """Commit stream files to storage."""
        self.singerlake.store.commit_stream_files(stream_files=self.files)
