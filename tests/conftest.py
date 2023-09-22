import json
import shutil
from pathlib import Path

import pytest

from singerlake import Singerlake


@pytest.fixture(scope="session")
def read_singerlake_config():
    return {
        "store": {
            "store_type": "local",
            "path": {
                "path_type": "hive",
                "lake_root": {
                    "segments": ("tests", "data", "read_lake"),
                    "relative": True,
                },
            },
            "lock": {
                "lock_type": "local",
            },
        }
    }


@pytest.fixture(scope="session")
def read_singerlake(read_singerlake_config: dict):
    singerlake = Singerlake(config=read_singerlake_config)
    singerlake.clean_working_dir()
    yield singerlake
    singerlake.clean_working_dir()


@pytest.fixture(scope="session")
def write_singerlake_config():
    return {
        "store": {
            "store_type": "local",
            "path": {
                "path_type": "hive",
                "lake_root": {
                    "segments": ("tests", "data", "write_lake"),
                    "relative": True,
                },
            },
            "lock": {
                "lock_type": "local",
            },
        }
    }


def _clean_lake_dir(lake_root: Path, lake_manifest: dict):
    shutil.rmtree(lake_root)
    lake_root.mkdir(parents=True, exist_ok=True)
    json.dump(
        lake_manifest,
        open(lake_root / "manifest.json", "w", encoding="utf-8"),
    )
    (lake_root / "raw").mkdir(parents=True, exist_ok=True)
    (lake_root / "raw" / "tap-carbon-intensity").mkdir(parents=True, exist_ok=True)
    json.dump(
        {
            "tap_id": "tap-carbon-intensity",
            "streams": ["entry", "generationmix", "region"],
        },
        open(
            lake_root / "raw" / "tap-carbon-intensity" / "manifest.json",
            "w",
            encoding="utf-8",
        ),
    )


@pytest.fixture(scope="session")
def write_singerlake(write_singerlake_config: dict):
    singerlake = Singerlake(config=write_singerlake_config)
    singerlake.clean_working_dir()
    lake_root = singerlake.store.get_lake_root()
    if lake_root.exists():
        _clean_lake_dir(lake_root, {"lake_id": "merry-lizard"})

    yield singerlake
    singerlake.clean_working_dir()
    _clean_lake_dir(lake_root, {"lake_id": "merry-lizard"})
