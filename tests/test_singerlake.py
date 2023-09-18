import pytest  # noqa: F401


def test_singerlake(singerlake):
    assert singerlake.instance_id is not None
    assert singerlake.config is not None
    assert singerlake.store is not None
    assert singerlake.manifest_service is not None
    assert singerlake.discovery_service is not None
