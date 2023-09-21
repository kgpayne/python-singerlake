from __future__ import annotations

import typing as t
from pathlib import Path

from flexdict import FlexDict

import singerlake.singer.utils as su

from .file_writer import SingerFileWriter

if t.TYPE_CHECKING:
    from .stream import Stream


MAX_RECORD_COUNT = 10000


class RecordWriter:
    """Write records to a stream file."""

    def __init__(self, stream: Stream, output_dir: Path) -> None:
        self.stream = stream
        self.output_dir = output_dir

        self.singer_files: list[Path] = []
        self.is_finalized = False
        self._open_files: FlexDict = FlexDict()

    def _finalize_file(
        self, file: SingerFileWriter, partition: t.Tuple[t.Any, ...]
    ) -> None:
        singer_file = file.close(output_dir=self.output_dir, partition=partition)
        self.singer_files.append(singer_file)

    def _new_file(self, partition: t.Tuple[t.Any, ...]) -> SingerFileWriter:
        """Return a new file."""
        open_file = SingerFileWriter(stream=self.stream).open()
        self._open_files.set(keys=partition, value=open_file)
        return open_file

    def write(self, schema: dict, record: dict) -> None:
        """Write a record to the stream."""

        # partition the record
        time_extracted = su.get_time_extracted(record)
        partition = self.stream.partition_record(time_extracted) or ("default",)
        open_file = self._open_files.get(partition)

        if not open_file or open_file.closed:
            open_file = self._new_file(partition)

        if open_file.records_written == MAX_RECORD_COUNT:
            self._finalize_file(file=open_file, partition=partition)
            # open a new file
            open_file = self._new_file(partition)

        if open_file.records_written == 0:
            # write the stream schema
            open_file.write_schema(schema)

        open_file.write_record(record)

    def finalize(self) -> None:
        """Finalize the stream."""
        for partition, open_file in self._open_files.flatten():
            self._finalize_file(file=open_file, partition=tuple(partition))
        self.is_finalized = True
