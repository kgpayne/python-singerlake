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

        self.files: list[Path] = []
        self.is_finalized = False
        self._open_files: FlexDict = FlexDict()

    def _finalize_file(self, file: SingerFileWriter) -> None:
        finalized_file_path = file.close(output_dir=self.output_dir)
        self.files.append(finalized_file_path)

    def _new_file(self, partition: t.Tuple[t.Any, ...]) -> SingerFileWriter:
        """Return a new file."""
        file = SingerFileWriter(stream=self.stream).open()
        self._open_files.set(keys=partition, value=file)
        return file

    def write(self, schema: dict, record: dict) -> None:
        """Write a record to the stream."""

        # partition the record
        time_extracted = su.get_time_extracted(record)
        partition = self.stream.partition_record(time_extracted) or ("default",)
        file = self._open_files.get(partition)

        if not file or file.closed:
            file = self._new_file(partition)

        if file.records_written == MAX_RECORD_COUNT:
            self._finalize_file(file)
            # open a new file
            file = self._new_file(partition)

        if file.records_written == 0:
            # write the stream schema
            file.write_schema(schema)

        file.write_record(record)

    def finalize(self) -> None:
        """Finalize the stream."""
        for file in self._open_files.values(nested=True):
            self._finalize_file(file)
        self.is_finalized = True
