import pytest
from nats.aio.client import Client as NATS


@pytest.mark.asyncio
async def test_container_ok(nats_client: NATS):
    sub = await nats_client.subscribe("test.subject")
    await nats_client.publish("test.subject", b"Hello, NATS!")
    msg = await sub.next_msg(timeout=1)
    assert msg.data == b"Hello, NATS!"
