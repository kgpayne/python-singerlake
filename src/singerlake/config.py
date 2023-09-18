from pydantic import BaseModel


class PathConfig(BaseModel):
    """Singer Lake Path Config."""

    path_type: str = "hive"
    lake_root: tuple[str, ...]


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
