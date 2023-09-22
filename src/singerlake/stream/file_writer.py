from __future__ import annotations

import json
import shutil
import tempfile
import typing as t
from datetime import datetime
from io import TextIOWrapper
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel, Field

import singerlake.singer.utils as su

if t.TYPE_CHECKING:
    from .stream import Stream


class SingerFile(BaseModel):
    """Singer file object."""

    tap_id: str
    schema_: dict = Field(alias="schema")
    parent_dir: Path
    partition: tuple[int, ...]
    min_time_extracted: datetime
    max_time_extracted: datetime
    encryption: t.Literal["none", "bz2", "gz"] = "none"

    @property
    def name(self):
        """Return the filename."""
        file_start_time = self.min_time_extracted.strftime("%Y%m%dT%H%M%SZ")
        file_stop_time = self.max_time_extracted.strftime("%Y%m%dT%H%M%SZ")
        file_name = f"{self.stream_id}-{file_start_time}-{file_stop_time}.singer"
        if self.encryption != "none":
            file_name += f".{self.encryption}"
        return file_name

    @property
    def stream_id(self):
        """Return the stream ID."""
        return self.schema_["stream"]

    @property
    def path(self):
        """Return the file path."""
        return self.parent_dir / self.name

    def __repr__(self) -> str:
        """Return a string representation of the object."""
        return f"{self.__class__.__name__}({self.path})"


class SingerFileWriter:
    """Base class for writing singer files to disk via temporary directories."""

    def __init__(self, stream: "Stream") -> None:
        self.stream = stream
        self.records_written = 0

        self._schema: dict | None = None
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

    @property
    def closed(self) -> bool:
        """Return True if the file is closed."""
        return self._file is None

    def _open_file(self, tmp_dir: Path) -> TextIOWrapper:
        """Open a file for writing."""
        self.file_path = tmp_dir / f"{uuid4()}.jsonl"
        self.file = self.file_path.open("w", encoding="utf-8")
        return self.file

    def open(self) -> SingerFileWriter:
        """Create a temporary directory and new file to write records to."""
        if self._tmp_dir is None:
            self.tmp_dir = Path(tempfile.mkdtemp())
        self._open_file(self.tmp_dir)
        return self

    def close(self, output_dir: Path, partition: t.Tuple[t.Any, ...]) -> SingerFile:
        """Remove the temporary directory."""
        if self._file is None:
            raise ValueError("File not open")

        if not self.file.closed:
            self.file.close()

        singer_file = SingerFile(
            tap_id=self.stream.tap.tap_id,
            schema=self._schema,
            parent_dir=output_dir,
            partition=partition,
            min_time_extracted=self._min_time_extracted,
            max_time_extracted=self._max_time_extracted,
        )
        shutil.move(self.file_path, singer_file.path)
        self._file = None

        if self.tmp_dir.exists():
            shutil.rmtree(self.tmp_dir, ignore_errors=True)

        return singer_file

    def write_record(self, record: dict) -> None:
        """Write a record to the file."""
        if self._file is None:
            raise ValueError("File not open")

        time_extracted = su.get_time_extracted(record)

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
        self.records_written += 1

    def write_schema(self, schema: dict) -> None:
        """Write a schema to the file."""
        if self._file is None:
            raise ValueError("File not open")

        self._schema = schema

        self.file.write(f"{json.dumps(schema, ensure_ascii=False)}\n")
