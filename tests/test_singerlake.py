import pytest  # noqa: F401


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
