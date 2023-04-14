import shutil
from pathlib import Path

import pytest

from singerlake.store import LocalStore

TEST_DIR = Path(__file__).parent / "local_singerlake" / "test_local_store"


class TestLocalStore:
    """Test Local Store."""

    @pytest.fixture
    def test_dir(self):
        TEST_DIR.mkdir(exist_ok=True, parents=True)
        yield TEST_DIR
        shutil.rmtree(TEST_DIR)

    @pytest.fixture
    def local_store(self, test_dir):
        yield LocalStore(lake_root=test_dir)

    def test_dir_generators(self, local_store):
        assert local_store.lake_root == TEST_DIR
        assert local_store.get_lake_path() == TEST_DIR / "raw"
        assert (
            local_store.get_tap_path(tap_id="tap-sample--meltano")
            == TEST_DIR / "raw" / "tap-sample--meltano"
        )
        assert (
            local_store.get_stream_path(
                tap_id="tap-sample--meltano", stream_id="example-stream-1"
            )
            == TEST_DIR / "raw" / "tap-sample--meltano" / "example-stream-1"
        )
