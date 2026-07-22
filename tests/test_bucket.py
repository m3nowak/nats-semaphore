import asyncio
import inspect
import warnings
from dataclasses import FrozenInstanceError

import pytest
from nats import connect
from nats.aio.client import Client as NATS
from nats.js.api import KeyValueConfig, StorageType
from nats.js.errors import APIError, BucketNotFoundError

from nats_semaphore import (
    NatsSemaphoreContext,
    SemaphoreBucketConfig,
    SemaphoreBucketConfigMismatchWarning,
)


def test_bucket_config_defaults_and_immutability():
    config = SemaphoreBucketConfig()

    assert config.bucket == "SEMAPHORES"
    assert config.description == "Semaphore Bucket"
    assert config.ttl == 10
    assert config.max_bytes == 1024 * 1024
    assert config.storage == StorageType.MEMORY
    assert config.replicas == 1
    with pytest.raises(FrozenInstanceError):
        config.ttl = 20  # type: ignore[misc]


@pytest.mark.parametrize("bucket", ["", "has space", "has.dot", "has>token"])
def test_bucket_config_rejects_invalid_bucket_names(bucket: str):
    with pytest.raises(ValueError, match="bucket"):
        SemaphoreBucketConfig(bucket=bucket)


@pytest.mark.parametrize(
    ("field", "value"),
    [("ttl", -0.1), ("max_bytes", 0), ("max_bytes", -1), ("replicas", 0), ("replicas", -1)],
)
def test_bucket_config_rejects_invalid_numeric_values(field: str, value: float):
    with pytest.raises(ValueError, match=field):
        SemaphoreBucketConfig(**{field: value})  # type: ignore[arg-type]


@pytest.mark.parametrize("ttl", [None, 0])
def test_bucket_config_accepts_disabled_ttl(ttl: float | None):
    assert SemaphoreBucketConfig(ttl=ttl).ttl == ttl


def test_context_requires_new_bucket_contract():
    parameters = inspect.signature(NatsSemaphoreContext).parameters

    assert "bucket" in parameters
    assert "kv" not in parameters
    assert parameters["bucket"].default is inspect.Parameter.empty
    assert KeyValueConfig not in parameters["bucket"].annotation.__args__


@pytest.mark.asyncio
async def test_provisioning_is_lazy_and_uses_all_defaults(nats_client: NATS):
    context = NatsSemaphoreContext(nats_client, bucket=SemaphoreBucketConfig())
    js = nats_client.jetstream()

    with pytest.raises(BucketNotFoundError):
        await js.key_value("SEMAPHORES")

    assert await context.semaphore("work", 1).current_free_count() == 1
    status = await (await js.key_value("SEMAPHORES")).status()
    stream_config = status.stream_info.config
    assert status.bucket == "SEMAPHORES"
    assert stream_config.description == "Semaphore Bucket"
    assert stream_config.max_age == 10
    assert stream_config.max_bytes == 1024 * 1024
    assert stream_config.storage == StorageType.MEMORY
    assert stream_config.num_replicas == 1
    assert stream_config.max_msgs_per_subject == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("ttl", [None, 0, 2.5])
async def test_provisions_supported_custom_values(nats_client: NATS, ttl: float | None):
    config = SemaphoreBucketConfig(
        bucket="CUSTOM",
        description="Custom semaphore state",
        ttl=ttl,
        max_bytes=2048,
        storage=StorageType.FILE,
        replicas=1,
    )
    context = NatsSemaphoreContext(nats_client, bucket=config)

    assert await context.semaphore("work", 1).current_free_count() == 1
    status = await (await nats_client.jetstream().key_value("CUSTOM")).status()
    stream_config = status.stream_info.config
    assert stream_config.description == "Custom semaphore state"
    if ttl:
        assert stream_config.max_age == ttl
    else:
        assert stream_config.max_age in (None, 0)
    assert stream_config.max_bytes == 2048
    assert stream_config.storage == StorageType.FILE


@pytest.mark.asyncio
async def test_string_binds_only(nats_client: NATS):
    js = nats_client.jetstream()
    context = NatsSemaphoreContext(nats_client, bucket="EXISTING")

    with pytest.raises(BucketNotFoundError):
        await context.semaphore("work", 1).current_free_count()

    await js.create_key_value(KeyValueConfig(bucket="EXISTING", ttl=10))
    bound_context = NatsSemaphoreContext(nats_client, bucket="EXISTING")
    assert await bound_context.semaphore("work", 1).current_free_count() == 1


@pytest.mark.asyncio
async def test_matching_existing_bucket_is_reused_without_warning(nats_client: NATS):
    config = SemaphoreBucketConfig(bucket="SHARED")
    first = NatsSemaphoreContext(nats_client, bucket=config)
    second = NatsSemaphoreContext(nats_client, bucket=config)
    await first.semaphore("one", 1).current_free_count()

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always", SemaphoreBucketConfigMismatchWarning)
        assert await second.semaphore("two", 1).current_free_count() == 1
    assert not [warning for warning in captured if warning.category is SemaphoreBucketConfigMismatchWarning]


@pytest.mark.asyncio
async def test_description_only_drift_does_not_warn(nats_client: NATS):
    config = SemaphoreBucketConfig(bucket="DESCRIPTION", description="First description")
    await NatsSemaphoreContext(nats_client, config).semaphore("one", 1).current_free_count()

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always", SemaphoreBucketConfigMismatchWarning)
        context = NatsSemaphoreContext(
            nats_client,
            SemaphoreBucketConfig(bucket="DESCRIPTION", description="Second description"),
        )
        assert await context.semaphore("two", 1).current_free_count() == 1
    assert not [warning for warning in captured if warning.category is SemaphoreBucketConfigMismatchWarning]


@pytest.mark.asyncio
async def test_operational_drift_warns_but_description_drift_does_not(nats_client: NATS):
    js = nats_client.jetstream()
    await js.create_key_value(
        KeyValueConfig(
            bucket="DRIFTED",
            description="Managed elsewhere",
            ttl=3,
            max_bytes=4096,
            storage=StorageType.FILE,
            replicas=1,
        )
    )
    requested = SemaphoreBucketConfig(
        bucket="DRIFTED",
        description="Ignored cosmetic drift",
        ttl=10,
        max_bytes=1024,
        storage=StorageType.MEMORY,
        replicas=2,
    )

    with pytest.warns(SemaphoreBucketConfigMismatchWarning) as captured:
        assert await NatsSemaphoreContext(nats_client, requested).semaphore("work", 1).current_free_count() == 1

    message = str(captured[0].message)
    assert "DRIFTED" in message
    assert "ttl" in message and "requested=10" in message and "effective=3" in message
    assert "max_bytes" in message
    assert "storage" in message
    assert "replicas" in message
    assert "description" not in message


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "requested_value"),
    [
        ("ttl", 4),
        ("max_bytes", 8192),
        ("storage", StorageType.MEMORY),
        ("replicas", 2),
    ],
)
async def test_each_operational_drift_field_warns(
    nats_client: NATS,
    field: str,
    requested_value: float | int | StorageType,
):
    bucket = f"DRIFT_{field.upper()}"
    await nats_client.jetstream().create_key_value(
        KeyValueConfig(
            bucket=bucket,
            ttl=3,
            max_bytes=4096,
            storage=StorageType.FILE,
            replicas=1,
        )
    )
    requested = {
        "bucket": bucket,
        "ttl": 3,
        "max_bytes": 4096,
        "storage": StorageType.FILE,
        "replicas": 1,
        field: requested_value,
    }

    with pytest.warns(SemaphoreBucketConfigMismatchWarning, match=field):
        context = NatsSemaphoreContext(nats_client, SemaphoreBucketConfig(**requested))  # type: ignore[arg-type]
        assert await context.semaphore("work", 1).current_free_count() == 1


@pytest.mark.asyncio
async def test_non_race_provisioning_failure_is_propagated(nats_client: NATS):
    context = NatsSemaphoreContext(
        nats_client,
        SemaphoreBucketConfig(bucket="UNAVAILABLE", replicas=2),
    )

    with pytest.raises(APIError):
        await context.semaphore("work", 1).current_free_count()
    with pytest.raises(BucketNotFoundError):
        await nats_client.jetstream().key_value("UNAVAILABLE")


@pytest.mark.asyncio
async def test_matching_requests_converge_across_independent_connections(nats_server: str):
    clients = await asyncio.gather(*(connect(nats_server) for _ in range(8)))
    contexts = [NatsSemaphoreContext(client, SemaphoreBucketConfig(bucket="RACE")) for client in clients]
    try:
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always", SemaphoreBucketConfigMismatchWarning)
            counts = await asyncio.gather(
                *(context.semaphore(f"work-{i}", 1).current_free_count() for i, context in enumerate(contexts))
            )
        assert counts == [1] * len(contexts)
        assert not [warning for warning in captured if warning.category is SemaphoreBucketConfigMismatchWarning]
    finally:
        await asyncio.gather(*(client.close() for client in clients))


@pytest.mark.asyncio
async def test_different_requests_converge_and_report_losers(nats_server: str):
    clients = await asyncio.gather(*(connect(nats_server) for _ in range(8)))
    configs = [
        SemaphoreBucketConfig(bucket="DRIFT_RACE", ttl=2 + i % 2, max_bytes=2048 + 1024 * (i % 2)) for i in range(8)
    ]
    contexts = [NatsSemaphoreContext(client, config) for client, config in zip(clients, configs)]
    try:
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            counts = await asyncio.gather(
                *(context.semaphore(f"work-{i}", 1).current_free_count() for i, context in enumerate(contexts))
            )
        assert counts == [1] * len(contexts)
        effective_kv = await clients[0].jetstream().key_value("DRIFT_RACE")
        effective = (await effective_kv.status()).stream_info.config
        expected_warning_count = sum(
            config.ttl != effective.max_age or config.max_bytes != effective.max_bytes for config in configs
        )
        assert (
            len([warning for warning in captured if warning.category is SemaphoreBucketConfigMismatchWarning])
            == expected_warning_count
        )
    finally:
        await asyncio.gather(*(client.close() for client in clients))


@pytest.mark.asyncio
async def test_renew_interval_uses_effective_bucket_ttl(nats_client: NATS):
    await nats_client.jetstream().create_key_value(KeyValueConfig(bucket="TTL_DRIFT", ttl=1))
    context = NatsSemaphoreContext(nats_client, SemaphoreBucketConfig(bucket="TTL_DRIFT", ttl=10))
    semaphore = context.semaphore("work", 1)

    with pytest.warns(SemaphoreBucketConfigMismatchWarning):
        with pytest.raises(ValueError, match="renew_interval"):
            await semaphore.acquire(renew_interval=1)
    with pytest.raises(ValueError, match="renew_interval"):
        await semaphore.acquire(renew_interval=2)
    lock = await semaphore.acquire(renew_interval=0.5)
    await lock.release()


@pytest.mark.asyncio
async def test_zero_renew_interval_is_valid_without_ttl(nats_client: NATS):
    context = NatsSemaphoreContext(nats_client, SemaphoreBucketConfig(bucket="NO_TTL", ttl=None))
    lock = await context.semaphore("work", 1).acquire(renew_interval=0)
    await lock.release()


@pytest.mark.asyncio
async def test_stale_release_does_not_delete_newer_lock_or_mask_body_error(nats_client: NATS):
    context = NatsSemaphoreContext(nats_client, SemaphoreBucketConfig(bucket="STALE", ttl=0.5))
    semaphore = context.semaphore("work", 1)
    stale = await semaphore.acquire(renew_interval=0)
    await asyncio.sleep(0.6)
    current = await semaphore.acquire(renew_interval=0)

    await stale.release()
    with pytest.raises(asyncio.TimeoutError):
        await semaphore.acquire(timeout=0.1, renew_interval=0)

    await asyncio.sleep(0.6)
    replacement = None
    with pytest.raises(RuntimeError, match="protected work"):
        async with semaphore.lock(renew_interval=0):
            await asyncio.sleep(0.6)
            replacement = await semaphore.acquire(renew_interval=0)
            raise RuntimeError("protected work")
    assert replacement is not None
    with pytest.raises(asyncio.TimeoutError):
        await semaphore.acquire(timeout=0.1, renew_interval=0)
    await replacement.release()
    await current.release()
