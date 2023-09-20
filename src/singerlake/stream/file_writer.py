from __future__ import annotations

import json
import shutil
import tempfile
import typing as t
from datetime import datetime
from io import TextIOWrapper
from pathlib import Path
from uuid import uuid4

if t.TYPE_CHECKING:
    from .stream import Stream


class SingerFileWriter:
    """Base class for writing singer files to disk via temporary directories."""

    def __init__(self, stream: "Stream") -> None:
        self.stream = stream

        self._tmp_dir: Path | None = None
        self._file: TextIOWrapper | None = None
        self._file_path: Path | None = None
        self._min_time_extracted: datetime | None = None
        self._max_time_extracted: datetime | None = None

    @property
    def file_path(self) -> Path:
        """Return the file path."""
        if self._file_path is None:
            raise ValueError("File has not been written to.")

        return self._file_path

    @file_path.setter
    def file_path(self, value: Path) -> None:
        """Set the file path."""
        self._file_path = value

    @property
    def file(self) -> TextIOWrapper:
        """Return the file."""
        if self._file is None:
            raise ValueError("File has not been opened.")

        return self._file

    @file.setter
    def file(self, value: TextIOWrapper) -> None:
        """Set the file."""
        self._file = value

    @property
    def tmp_dir(self) -> Path:
        """Return the temporary directory."""
        if self._tmp_dir is None:
            raise ValueError("Temporary directory has not been created.")

        return self._tmp_dir

    @tmp_dir.setter
    def tmp_dir(self, value: Path) -> None:
        """Set the temporary directory."""
        self._tmp_dir = value

    def _get_time_extracted(self, record: dict) -> datetime:
        """Return the time extracted from a record."""
        time_extracted = record.get("time_extracted") or record.get("record", {}).get(
            "_sdc_extracted_at"
        )
        if not time_extracted:
            raise ValueError("Record does not contain time_extracted")

        return datetime.fromisoformat(time_extracted)

    def _open_file(self, tmp_dir: Path) -> TextIOWrapper:
        """Open a file for writing."""
        self.file_path = tmp_dir / f"{uuid4()}.jsonl"
        self.file = self.file_path.open("w", encoding="utf-8")
        return self.file

    @property
    def file_name(self) -> str:
        """Return the file name."""
        if not self._min_time_extracted or not self._max_time_extracted:
            raise ValueError("File has not been written to.")

        file_start_time = self._min_time_extracted.strftime("%Y%m%dT%H%M%SZ")
        file_stop_time = self._max_time_extracted.strftime("%Y%m%dT%H%M%SZ")
        return f"{self.stream.stream_id}-{file_start_time}-{file_stop_time}.singer"

    def open(self) -> SingerFileWriter:
        """Create a temporary directory and new file to write records to."""
        if self._tmp_dir is None:
            self.tmp_dir = Path(tempfile.mkdtemp())
        self._open_file(self.tmp_dir)
        return self

    def close(self, output_dir: Path) -> Path:
        """Remove the temporary directory."""
        if self._file is None:
            raise ValueError("File not open")

        if not self.file.closed:
            self.file.close()

        output_file_path = output_dir / self.file_name
        shutil.move(self.file_path, output_file_path)
        self._file = None

        if self.tmp_dir.exists():
            shutil.rmtree(self.tmp_dir, ignore_errors=True)

        return output_file_path

    def write_record(self, record: dict) -> None:
        """Write a record to the file."""
        if self._file is None:
            raise ValueError("File not open")

        time_extracted = self._get_time_extracted(record)

        if self._min_time_extracted is None:
            self._min_time_extracted = time_extracted

        if self._max_time_extracted is None:
            self._max_time_extracted = time_extracted

        if time_extracted < self._min_time_extracted:
            self._min_time_extracted = time_extracted

        if time_extracted > self._max_time_extracted:
            self._max_time_extracted = time_extracted

        payload = json.dumps(record, ensure_ascii=False)
        self.file.write(f"{payload}\n")

    def write_schema(self, schema: dict) -> None:
        """Write a schema to the file."""
        if self._file is None:
            raise ValueError("File not open")

        self.file.write(f"{json.dumps(schema, ensure_ascii=False)}\n")
