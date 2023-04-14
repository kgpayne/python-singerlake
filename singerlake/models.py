import datetime
from typing import List, Mapping

from pydantic import BaseModel


class LakeManifest(BaseModel):
    """Lake Manifest."""


class TapManifest(BaseModel):
    """Tap Manifest."""


class StreamManifest(BaseModel):
    """Stream Manifest."""

    files: List[str]
    versions: Mapping[str, str]

    def add_files(self, file_names: List[str], schema_hash: str):
        """Add files to this Stream Manifest."""
        self.files.extend([f"{schema_hash}/{file_name}" for file_name in file_names])
        if schema_hash not in self.versions:
            self.versions[schema_hash] = datetime.datetime.utcnow().strftime(
                "%Y%m%dT%H%M%SZ"
            )
        return self
