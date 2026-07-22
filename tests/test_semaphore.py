import asyncio

import pytest
from nats.aio.client import Client as NATS

from nats_semaphore import NatsSemaphoreContext, SemaphoreBucketConfig


@pytest.mark.asyncio
async def test_basic_setup_0(nats_client: NATS):
    context = NatsSemaphoreContext(nats_client, bucket=SemaphoreBucketConfig(bucket="TEST_KV_BUCKET"))
    with pytest.raises(ValueError):
        context.semaphore(name="test_semaphore", slot_count=0)


@pytest.mark.asyncio
async def test_basic_setup_1(nats_client: NATS):
    context = NatsSemaphoreContext(nats_client, bucket=SemaphoreBucketConfig(bucket="TEST_KV_BUCKET"))
    semaphore = context.semaphore(name="test_semaphore", slot_count=1)

    lock = await semaphore.acquire(timeout=5.0, renew_interval=0)
    assert lock is not None
    with pytest.raises(asyncio.TimeoutError):
        await semaphore.acquire(timeout=1.0)
    await lock.release()


@pytest.mark.asyncio
async def test_basic_setup_2(nats_client: NATS):
    context = NatsSemaphoreContext(nats_client, bucket=SemaphoreBucketConfig(bucket="TEST_KV_BUCKET"))
    semaphore = context.semaphore(name="test_semaphore", slot_count=2)

    lock1 = await semaphore.acquire(timeout=5.0)
    assert lock1 is not None
    lock2 = await semaphore.acquire(timeout=5.0)
    assert lock2 is not None
    with pytest.raises(asyncio.TimeoutError):
        await semaphore.acquire(timeout=1.0)
    await lock1.release()
    await lock2.release()


@pytest.mark.asyncio
async def test_acquire_release(nats_client: NATS):
    context = NatsSemaphoreContext(nats_client, bucket=SemaphoreBucketConfig(bucket="TEST_KV_BUCKET"))
    semaphore = context.semaphore(name="test_semaphore", slot_count=1)

    lock = await semaphore.acquire(timeout=5.0, renew_interval=0)
    assert lock is not None
    await lock.release()

    lock2 = await semaphore.acquire(timeout=5.0)
    assert lock2 is not None
    await lock2.release()


@pytest.mark.asyncio
async def test_context_manager(nats_client: NATS):
    context = NatsSemaphoreContext(nats_client, bucket=SemaphoreBucketConfig(bucket="TEST_KV_BUCKET"))
    semaphore = context.semaphore(name="test_semaphore", slot_count=1)

    async with semaphore.lock(timeout=5.0) as lock:
        with pytest.raises(asyncio.TimeoutError):
            await semaphore.acquire(timeout=1.0)
        assert lock is not None


@pytest.mark.asyncio
async def test_lock_expiration(nats_client: NATS):
    config = SemaphoreBucketConfig(
        bucket="TEST_KV_BUCKET",
        ttl=1,  # Set a TTL for the keys
    )

    context = NatsSemaphoreContext(nats_client, bucket=config)
    semaphore = context.semaphore(name="test_semaphore", slot_count=1)

    lock = await semaphore.acquire(timeout=5.0, renew_interval=0)
    assert lock is not None

    # Wait for the lock to expire
    await asyncio.sleep(1.1)

    # Now we should be able to acquire the lock again
    lock2 = await semaphore.acquire(timeout=5.0, renew_interval=0)
    assert lock2 is not None

    await lock2.release()


@pytest.mark.asyncio
async def test_multiple_semaphores(nats_client: NATS):
    context = NatsSemaphoreContext(nats_client, bucket=SemaphoreBucketConfig(bucket="TEST_KV_BUCKET"))

    semaphore1 = context.semaphore(name="semaphore_1", slot_count=1)
    semaphore2 = context.semaphore(name="semaphore_2", slot_count=1)

    lock1 = await semaphore1.acquire(timeout=5.0)
    assert lock1 is not None

    lock2 = await semaphore2.acquire(timeout=5.0)
    assert lock2 is not None

    await lock1.release()
    await lock2.release()


@pytest.mark.asyncio
async def test_semaphore_reuse(nats_client: NATS):
    context = NatsSemaphoreContext(nats_client, bucket=SemaphoreBucketConfig(bucket="TEST_KV_BUCKET"))
    semaphore = context.semaphore(name="test_semaphore", slot_count=1)

    for _ in range(3):
        lock = await semaphore.acquire(timeout=5.0)
        assert lock is not None
        await lock.release()


@pytest.mark.asyncio
async def test_acquire_timeout(nats_client: NATS):
    context = NatsSemaphoreContext(nats_client, bucket=SemaphoreBucketConfig(bucket="TEST_KV_BUCKET"))
    semaphore = context.semaphore(name="test_semaphore", slot_count=1)

    lock = await semaphore.acquire(timeout=5.0, renew_interval=0)
    assert lock is not None

    with pytest.raises(asyncio.TimeoutError):
        await semaphore.acquire(timeout=1.0)

    await lock.release()


@pytest.mark.asyncio
async def test_free_count(nats_client: NATS):
    context = NatsSemaphoreContext(nats_client, bucket=SemaphoreBucketConfig(bucket="TEST_KV_BUCKET"))
    semaphore = context.semaphore(name="test_semaphore", slot_count=3)

    free_count = await semaphore.current_free_count()
    assert free_count == 3

    lock1 = await semaphore.acquire(timeout=5.0)
    free_count = await semaphore.current_free_count()
    assert free_count == 2

    lock2 = await semaphore.acquire(timeout=5.0)
    free_count = await semaphore.current_free_count()
    assert free_count == 1

    await lock1.release()
    free_count = await semaphore.current_free_count()
    assert free_count == 2

    await lock2.release()
    free_count = await semaphore.current_free_count()
    assert free_count == 3


@pytest.mark.asyncio
async def test_renew_lock(nats_client: NATS):
    config = SemaphoreBucketConfig(
        bucket="TEST_KV_BUCKET",
        ttl=2,
    )

    context = NatsSemaphoreContext(nats_client, bucket=config)
    semaphore = context.semaphore(name="test_semaphore", slot_count=1)

    lock = await semaphore.acquire(timeout=5.0, renew_interval=0)
    assert lock is not None

    await asyncio.sleep(1.2)

    # Renew
    await lock.renew()

    # The original entry would now be expired, but the renewed entry is current.
    await asyncio.sleep(1.2)

    # Should still be locked.
    with pytest.raises(asyncio.TimeoutError):
        await semaphore.acquire(timeout=0.5, renew_interval=0)

    await lock.release()


@pytest.mark.asyncio
async def test_auto_renew_lock(nats_client: NATS):
    config = SemaphoreBucketConfig(
        bucket="TEST_KV_BUCKET",
        ttl=1,
    )

    context = NatsSemaphoreContext(nats_client, bucket=config)
    semaphore = context.semaphore(name="test_semaphore", slot_count=1)

    lock = await semaphore.acquire(timeout=5.0, renew_interval=0.4)
    assert lock is not None

    await asyncio.sleep(1.3)

    with pytest.raises(asyncio.TimeoutError):
        await semaphore.acquire(timeout=0.5, renew_interval=0)

    await lock.release()


@pytest.mark.asyncio
async def test_auto_renew_disabled(nats_client: NATS):
    config = SemaphoreBucketConfig(
        bucket="TEST_KV_BUCKET",
        ttl=1,
    )

    context = NatsSemaphoreContext(nats_client, bucket=config)
    semaphore = context.semaphore(name="test_semaphore", slot_count=1)

    lock = await semaphore.acquire(timeout=5.0, renew_interval=0)
    assert lock is not None

    await asyncio.sleep(1.1)

    lock2 = await semaphore.acquire(timeout=5.0, renew_interval=0)
    assert lock2 is not None
    await lock2.release()


@pytest.mark.asyncio
async def test_auto_renew_interval_validation(nats_client: NATS):
    context = NatsSemaphoreContext(nats_client, bucket=SemaphoreBucketConfig(bucket="TEST_KV_BUCKET"))
    semaphore = context.semaphore(name="test_semaphore", slot_count=1)

    with pytest.raises(ValueError):
        await semaphore.acquire(renew_interval=-1)


@pytest.mark.asyncio
async def test_renew_lost_lock(nats_client: NATS):
    from nats.js.errors import KeyWrongLastSequenceError

    config = SemaphoreBucketConfig(
        bucket="TEST_KV_BUCKET",
        ttl=1,  # 1 second TTL
    )

    context = NatsSemaphoreContext(nats_client, bucket=config)
    semaphore = context.semaphore(name="test_semaphore", slot_count=1)

    lock = await semaphore.acquire(timeout=5.0, renew_interval=0)

    # Wait for expiration
    await asyncio.sleep(2.0)

    # Renew should fail
    with pytest.raises(KeyWrongLastSequenceError):
        await lock.renew()


@pytest.mark.asyncio
async def test_auto_renew_lost_lock_release(nats_client: NATS):
    config = SemaphoreBucketConfig(
        bucket="TEST_KV_BUCKET",
        ttl=1,
    )

    context = NatsSemaphoreContext(nats_client, bucket=config)
    semaphore = context.semaphore(name="test_semaphore", slot_count=1)

    lock1 = await semaphore.acquire(timeout=5.0, renew_interval=0)
    assert lock1 is not None

    # Let the first lock expire before another holder claims its slot.
    await asyncio.sleep(1.1)

    # Another lock should now be acquirable
    lock2 = await semaphore.acquire(timeout=5.0, renew_interval=0)
    assert lock2 is not None

    # Releasing the lost lock should not delete the current lock
    await lock1.release()

    # A third lock should fail because lock2 is still held
    with pytest.raises(asyncio.TimeoutError):
        await semaphore.acquire(timeout=0.5, renew_interval=0)

    await lock2.release()


@pytest.mark.asyncio
async def test_separate_contexts(nats_client: NATS):
    config = SemaphoreBucketConfig(bucket="TEST_KV_BUCKET")
    context1 = NatsSemaphoreContext(nats_client, bucket=config)
    context2 = NatsSemaphoreContext(nats_client, bucket=config)

    semaphore1 = context1.semaphore(name="test_semaphore", slot_count=1)
    semaphore2 = context2.semaphore(name="test_semaphore", slot_count=1)

    lock1 = await semaphore1.acquire(timeout=5.0)
    assert lock1 is not None
    with pytest.raises(asyncio.TimeoutError):
        await semaphore2.acquire(timeout=1.0)
    await lock1.release()
