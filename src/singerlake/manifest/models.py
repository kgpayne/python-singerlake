import datetime
import typing as t

from pydantic import BaseModel


class StreamDefinition(BaseModel):
    """Stream Manifest."""

    stream_id: str

    files: t.List[str] = []
    versions: t.Mapping[str, str] = {}


class TapDefinition(BaseModel):
    """Tap Manifest."""

    tap_id: str
    streams: t.List[StreamDefinition] = []


class LakeManifest(BaseModel):
    """Lake Manifest."""

    lake_id: str
    taps: t.List[TapDefinition] = []
