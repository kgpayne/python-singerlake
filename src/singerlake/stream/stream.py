import typing as t
from contextlib import contextmanager

from .record_writer import RecordWriter

if t.TYPE_CHECKING:
    from datetime import datetime
    from pathlib import Path

    from singerlake import Singerlake
    from singerlake.config import Partition
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
        self.partitions: t.List["Partition"] = (
            self.singerlake.config.store.path.partition_by or []
        )

        self.files: list[Path] = []

    def partition_record(self, time_extracted: "datetime") -> t.Tuple[str, ...]:
        """Partition a record."""
        partitions = [
            getattr(time_extracted, partition.by) for partition in self.partitions
        ]
        return tuple(partitions)

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
            self.files.extend(writer.files)

    def commit(self):
        """Commit stream files to storage."""
        self.singerlake.store.commit_stream_files(stream=self, stream_files=self.files)
