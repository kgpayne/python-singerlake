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
def read_singerlake(singerlake_config: dict):
    singerlake = Singerlake(config=singerlake_config)
    singerlake.clean_working_dir()
    yield singerlake
    singerlake.clean_working_dir()
