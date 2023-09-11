import datetime
from typing import List, Mapping

from pydantic import BaseModel


class StreamDefinition(BaseModel):
    """Stream Manifest."""

    stream_id: str

    # files: List[str] = []
    # versions: Mapping[str, str] = {}

    # def add_files(self, file_names: List[str], schema_hash: str):
    #     """Add files to this Stream Manifest."""
    #     self.files.extend([f"{schema_hash}/{file_name}" for file_name in file_names])
    #     if schema_hash not in self.versions:
    #         self.versions[schema_hash] = datetime.datetime.utcnow().strftime(
    #             "%Y%m%dT%H%M%SZ"
    #         )
    #     return self


class TapDefinition(BaseModel):
    """Tap Manifest."""

    tap_id: str
    streams: List[StreamDefinition] = []


class LakeManifest(BaseModel):
    """Lake Manifest."""

    lake_id: str
    taps: List[TapDefinition] = []
