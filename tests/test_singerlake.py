from pathlib import Path

import pytest  # noqa: F401

from tests.utils import TestStreamWriter


def test_singerlake(read_singerlake):
    assert read_singerlake.instance_id is not None
    assert read_singerlake.config is not None
    assert read_singerlake.store is not None
    assert read_singerlake.manifest_service is not None
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
    assert len(stream.files) == 1
    assert stream.files[0].name == "entry-20230920T140156Z-20230920T140156Z.singer"
