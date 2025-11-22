from typing import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from dotenv import load_dotenv
from nats import connect
from nats.aio.client import Client as NATS
from testcontainers.nats import NatsContainer

load_dotenv()


@pytest.fixture(scope="session")
def nats_server() -> Generator[str, None, None]:
    """Starts a NATS server for testing."""
    with NatsContainer("nats:latest").with_command("-js") as nats:
        yield nats.nats_uri()


@pytest_asyncio.fixture(scope="function")
async def nats_client(nats_server: str) -> AsyncGenerator[NATS, None]:
    """Provides a connected NATS client for testing."""
    nc = await connect(servers=[nats_server])
    js = nc.jetstream()
    # clean up any existing streams
    streams = await js.streams_info()
    for stream in streams:
        if stream.config.name is not None:
            await js.delete_stream(stream.config.name)
    try:
        yield nc
    finally:
        await nc.close()
