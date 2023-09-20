from pathlib import Path

import pytest  # noqa: F401

from tests.utils import TestStreamWriter


def test_singerlake(singerlake):
    assert singerlake.instance_id is not None
    assert singerlake.config is not None
    assert singerlake.store is not None
    assert singerlake.manifest_service is not None
    assert singerlake.discovery_service is not None


def test_discovery(singerlake):
    assert singerlake.lake_id == "sound-oryx"
    assert singerlake.list_taps() == ["tap-carbon-intensity"]

    tap = singerlake.get_tap("tap-carbon-intensity")

    assert tap.stream_ids == [
        "entry",
        "generationmix",
        "region",
    ]


def test_stream_writer(singerlake):
    input_file_path = Path.cwd() / "tests" / "data" / "test_inputs" / "entry.jsonl"
    tap = singerlake.get_tap("tap-carbon-intensity")
    stream = tap.get_stream("entry")
    stream_writer = TestStreamWriter(input_stream_path=input_file_path)
    stream = stream_writer.write_messages_to_stream(stream=stream)
    assert len(stream.files) == 1
    assert stream.files[0].name == "entry-20230920T140156Z-20230920T140156Z.singer"
