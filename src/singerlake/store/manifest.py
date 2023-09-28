import typing as t

from pydantic import BaseModel


class SchemaVersion(BaseModel):
    """Schema Version."""

    first_observed: str
    schema_hash: str


class StreamManifest(BaseModel):
    """Stream Manifest."""

    stream_id: str

    files: t.List[str] = []
    versions: t.List[SchemaVersion] = []


class TapManifest(BaseModel):
    """Tap Manifest."""

    tap_id: str
    streams: t.List[str] = []


class LakeManifest(BaseModel):
    """Lake Manifest."""

    lake_id: str
    taps: t.List[str] = []
