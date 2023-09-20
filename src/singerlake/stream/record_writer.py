from __future__ import annotations

import typing as t
from pathlib import Path

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
        self._current_file: SingerFileWriter | None = None
        self._record_count = 0

    @property
    def current_file(self) -> SingerFileWriter:
        """Return the current file."""
        if self._current_file is None:
            raise ValueError("File not open.")

        return self._current_file

    @current_file.setter
    def current_file(self, value: SingerFileWriter) -> None:
        """Set the current file."""
        self._current_file = value

    def open(self) -> RecordWriter:
        self.current_file = SingerFileWriter(stream=self.stream).open()
        return self

    def close(self):
        """Finalize the last file."""
        if self._current_file is None:
            raise ValueError("File not open.")

        self._finalize_current_file()

    def _finalize_current_file(self):
        finalized_file_path = self.current_file.close(output_dir=self.output_dir)
        self._current_file = None
        self.files.append(finalized_file_path)

    def write(self, schema: dict, record: dict) -> None:
        """Write a record to the stream."""

        if self._record_count == MAX_RECORD_COUNT:
            self._finalize_current_file()
            # open a new file
            self.current_file = SingerFileWriter(stream=self.stream).open()
            self._record_count = 0

        if self._record_count == 0:
            # write the stream schema
            self.current_file.write_schema(schema)

        self.current_file.write_record(record)
        self._record_count += 1
