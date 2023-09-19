import typing as t

from pydantic import BaseModel


class GenericPathModel(BaseModel):
    """Generic Path Model."""

    segments: t.Tuple[str, ...]
    relative: bool = False


class PathConfig(BaseModel):
    """Singer Lake Path Config."""

    path_type: str = "hive"
    lake_root: GenericPathModel


class LockConfig(BaseModel):
    """Singer Lake Lock Config."""

    lock_type: str = "local"


class StoreConfig(BaseModel):
    """Singer Lake Store Config."""

    store_type: str = "local"
    path: PathConfig
    lock: LockConfig


class SingerlakeConfig(BaseModel):
    """Singer Lake Config."""

    store: StoreConfig
