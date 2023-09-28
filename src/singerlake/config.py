import typing as t

from pydantic import BaseModel


class PartitionBy(BaseModel):
    """Partition Model."""

    by: t.Literal["year", "month", "day", "hour", "minute", "second"]


class GenericPathModel(BaseModel):
    """Generic Path Model."""

    segments: t.Tuple[str, ...]
    relative: bool = False


class PathConfig(BaseModel):
    """Singer Lake Path Config."""

    path_type: str = "hive"
    lake_root: GenericPathModel
    partition_by: t.Optional[t.List[PartitionBy]] = [
        PartitionBy(by="year"),
        PartitionBy(by="month"),
        PartitionBy(by="day"),
        PartitionBy(by="hour"),
    ]


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
    working_dir: t.Optional[GenericPathModel] = None
