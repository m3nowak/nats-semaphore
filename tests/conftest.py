import os
from typing import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from dotenv import load_dotenv
from nats import connect
from nats.aio.client import Client as NATS
from testcontainers.nats import NatsContainer

load_dotenv()


@pytest.fixture(scope="session")
def nats_version() -> str | None:
    """Provides the NATS server version for testing."""
    return os.getenv("NATS_CONTAINER_VERSION", None)


@pytest.fixture(scope="session")
def expected_python_version() -> str | None:
    """Provides the expected Python version for testing."""
    return os.getenv("EXPECTED_PYTHON_VERSION", None)


@pytest.fixture(scope="session")
def nats_server(nats_version: str | None) -> Generator[str, None, None]:
    """Starts a NATS server for testing."""
    env_var = os.getenv("NATS_SERVER_URL")
    if env_var:
        yield env_var
    else:
        version = nats_version if nats_version is not None else "2.7"
        with NatsContainer(f"docker.io/library/nats:{version}").with_command("-js") as nats:
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
