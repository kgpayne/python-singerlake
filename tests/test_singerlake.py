from pathlib import Path

import pytest  # noqa: F401

from tests.utils import TestStreamWriter


def test_singerlake(read_singerlake):
    assert read_singerlake.instance_id is not None
    assert read_singerlake.config is not None
    assert read_singerlake.store is not None
    assert read_singerlake.discovery_service is not None


def test_discovery(read_singerlake):
    assert read_singerlake.lake_id == "sound-oryx"
    assert read_singerlake.list_taps() == ["tap-carbon-intensity"]

    tap = read_singerlake.get_tap("tap-carbon-intensity")

    assert tap.stream_ids == [
        "entry",
        "generationmix",
        "region",
    ]


def test_stream_writer(read_singerlake):
    input_file_path = Path.cwd() / "tests" / "data" / "test_inputs" / "entry.jsonl"
    tap = read_singerlake.get_tap("tap-carbon-intensity")
    stream = tap.get_stream("entry")
    stream_writer = TestStreamWriter(input_stream_path=input_file_path)
    stream = stream_writer.write_messages_to_stream(stream=stream)
    assert len(stream.files) == 2
    assert [file.name for file in stream.files] == [
        "entry-20200819T130156Z-20200819T130156Z.singer",
        "entry-20230920T140156Z-20230920T140156Z.singer",
    ]


def test_stream_commit(write_singerlake):
    input_file_path = Path.cwd() / "tests" / "data" / "test_inputs" / "entry.jsonl"
    tap = write_singerlake.get_tap("tap-carbon-intensity")
    stream = tap.get_stream("entry")
    stream_writer = TestStreamWriter(input_stream_path=input_file_path)
    stream = stream_writer.write_messages_to_stream(stream=stream)
    stream.commit()

    # stream directory exists
    stream_files_path = (
        Path.cwd()
        / "tests"
        / "data"
        / "write_lake"
        / "raw"
        / "tap-carbon-intensity"
        / "entry"
        / "Y8Mjkb4i9yM"
    )
    assert stream_files_path.exists()

    # stream files exist
    stream_file_1_path = (
        stream_files_path
        / "year=2020"
        / "month=8"
        / "day=19"
        / "hour=13"
        / "entry-20200819T130156Z-20200819T130156Z.singer"
    )
    assert stream_file_1_path.exists()

    stream_file_2_path = (
        stream_files_path
        / "year=2023"
        / "month=9"
        / "day=20"
        / "hour=14"
        / "entry-20230920T140156Z-20230920T140156Z.singer"
    )
    assert stream_file_2_path.exists()
