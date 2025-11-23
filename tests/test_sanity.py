import pytest
from nats.aio.client import Client as NATS


@pytest.mark.asyncio
async def test_container_ok(nats_client: NATS):
    sub = await nats_client.subscribe("test.subject")
    await nats_client.publish("test.subject", b"Hello, NATS!")
    msg = await sub.next_msg(timeout=1)
    assert msg.data == b"Hello, NATS!"


@pytest.mark.asyncio
async def test_container_jetstream(nats_client: NATS):
    js = nats_client.jetstream()
    await js.add_stream(name="TEST_STREAM", subjects=["test.stream.*"])
    info = await js.stream_info("TEST_STREAM")
    assert info.config.name == "TEST_STREAM"


@pytest.mark.asyncio
async def test_container_version(nats_client: NATS, nats_version: str | None):
    if nats_version is None:
        pytest.skip("NATS_CONTAINER_VERSION not set; skipping version check")
    actual_version = nats_client.connected_server_version

    assert nats_version == f"{actual_version.major}.{actual_version.minor}"


@pytest.mark.asyncio
async def test_python_version(expected_python_version: str | None):
    import sys

    if expected_python_version is None:
        pytest.skip("EXPECTED_PYTHON_VERSION not set; skipping version check")

    actual_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    assert expected_python_version == actual_version
