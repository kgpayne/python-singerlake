import pytest

from singerlake import Singerlake


@pytest.fixture(scope="session")
def singerlake_config():
    return {
        "store": {
            "store_type": "local",
            "path": {
                "path_type": "hive",
                "lake_root": ("tests", "data", "lake"),
            },
            "lock": {
                "lock_type": "local",
            },
        }
    }


@pytest.fixture(scope="session")
def singerlake(singerlake_config: dict):
    yield Singerlake(config=singerlake_config)
