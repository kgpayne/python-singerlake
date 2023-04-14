import json
import shutil
from pathlib import Path

import pytest

from singerlake import SingerLake
from singerlake.store import LocalStore

EXAMPLE_FILES_DIR = Path(__file__).parent / "example_files"
TEST_DIR = Path(__file__).parent / "local_singerlake" / "test_local_singerlake"
LAKE_PATH = TEST_DIR / "raw"
EXAMPLE_TAP_SYNC = {
    "tap-carbon-intensity": {
        "entry": {
            "files": [
                EXAMPLE_FILES_DIR / "entry-20230414T155444-20230414T155444.singer.gz"
            ],
            "schema": EXAMPLE_FILES_DIR / "entry.schema.json",
            "schema_hash": "Y8Mjkb4i9yM",
        },
        "generationmix": {
            "files": [
                EXAMPLE_FILES_DIR
                / "generationmix-20230414T155444-20230414T155444.singer.gz"
            ],
            "schema": EXAMPLE_FILES_DIR / "generationmix.schema.json",
            "schema_hash": "XcxC17T7smW",
        },
        "region": {
            "files": [
                EXAMPLE_FILES_DIR / "region-20230414T155444-20230414T155444.singer.gz"
            ],
            "schema": EXAMPLE_FILES_DIR / "region.schema.json",
            "schema_hash": "3v36Q5QKNZM",
        },
    }
}


class TestLocalSingerlake:
    @pytest.fixture
    def test_dir(self):
        TEST_DIR.mkdir(exist_ok=True, parents=True)
        yield TEST_DIR
        shutil.rmtree(TEST_DIR)

    @pytest.fixture
    def local_store(self, test_dir):
        yield LocalStore(lake_root=test_dir)

    @pytest.fixture
    def singerlake(self, local_store):
        yield SingerLake(store=local_store)

    def test_write(self, singerlake):
        for tap_id, streams in EXAMPLE_TAP_SYNC.items():
            for stream_id, stream in streams.items():
                stream_schema = {}
                with stream["schema"].open() as schema_file:
                    stream_schema = json.load(schema_file)
                singerlake.write_files(
                    tap_id=tap_id,
                    stream_id=stream_id,
                    stream_schema=stream_schema,
                    files=stream["files"],
                )
                # check tap dir created
                assert (LAKE_PATH / tap_id).exists()
                # check stream dir created
                assert (LAKE_PATH / tap_id / stream_id).exists()
                # check manifest file created
                assert (LAKE_PATH / tap_id / stream_id / "manifest.json").exists()
                # check stream version dir created
                assert (LAKE_PATH / tap_id / stream_id / stream["schema_hash"]).exists()
                # check data file copied
                for file in stream["files"]:
                    assert (
                        LAKE_PATH
                        / tap_id
                        / stream_id
                        / stream["schema_hash"]
                        / file.name
                    ).exists()
