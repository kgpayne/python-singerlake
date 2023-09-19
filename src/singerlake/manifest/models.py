import datetime
import typing as t

from pydantic import BaseModel


class StreamManifest(BaseModel):
    """Stream Manifest."""

    stream_id: str

    files: t.List[str] = []
    versions: t.Mapping[str, str] = {}


class TapManifest(BaseModel):
    """Tap Manifest."""

    tap_id: str
    streams: t.List[str] = []


class LakeManifest(BaseModel):
    """Lake Manifest."""

    lake_id: str
    taps: t.List[str] = []
