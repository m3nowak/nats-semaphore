import asyncio

import pytest
from nats.aio.client import Client as NATS


@pytest.mark.asyncio
async def test_basic_setup_0(nats_client: NATS):
    from nats_semaphore import NatsSemaphoreDispatcher

    dispatcher = NatsSemaphoreDispatcher(nats_client, kv="TEST_KV_BUCKET")
    with pytest.raises(ValueError):
        dispatcher.semaphore(name="test_semaphore", slot_count=0)


@pytest.mark.asyncio
async def test_basic_setup_1(nats_client: NATS):
    from nats_semaphore import NatsSemaphoreDispatcher

    dispatcher = NatsSemaphoreDispatcher(nats_client, kv="TEST_KV_BUCKET")
    semaphore = dispatcher.semaphore(name="test_semaphore", slot_count=1)

    lock = await semaphore.acquire(timeout=5.0)
    assert lock is not None
    with pytest.raises(asyncio.TimeoutError):
        await semaphore.acquire(timeout=1.0)
    await lock.release()


@pytest.mark.asyncio
async def test_basic_setup_2(nats_client: NATS):
    from nats_semaphore import NatsSemaphoreDispatcher

    dispatcher = NatsSemaphoreDispatcher(nats_client, kv="TEST_KV_BUCKET")
    semaphore = dispatcher.semaphore(name="test_semaphore", slot_count=2)

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
    from nats_semaphore import NatsSemaphoreDispatcher

    dispatcher = NatsSemaphoreDispatcher(nats_client, kv="TEST_KV_BUCKET")
    semaphore = dispatcher.semaphore(name="test_semaphore", slot_count=1)

    lock = await semaphore.acquire(timeout=5.0)
    assert lock is not None
    await lock.release()

    lock2 = await semaphore.acquire(timeout=5.0)
    assert lock2 is not None
    await lock2.release()


@pytest.mark.asyncio
async def test_context_manager(nats_client: NATS):
    from nats_semaphore import NatsSemaphoreDispatcher

    dispatcher = NatsSemaphoreDispatcher(nats_client, kv="TEST_KV_BUCKET")
    semaphore = dispatcher.semaphore(name="test_semaphore", slot_count=1)

    async with semaphore.lock(timeout=5.0) as lock:
        with pytest.raises(asyncio.TimeoutError):
            await semaphore.acquire(timeout=1.0)
        assert lock is not None


@pytest.mark.asyncio
async def test_lock_expiration(nats_client: NATS):
    from nats.js.api import KeyValueConfig

    from nats_semaphore import NatsSemaphoreDispatcher

    kvc = KeyValueConfig(
        bucket="TEST_KV_BUCKET",
        ttl=1,  # Set a TTL for the keys
    )

    dispatcher = NatsSemaphoreDispatcher(nats_client, kv=kvc)
    semaphore = dispatcher.semaphore(name="test_semaphore", slot_count=1)

    lock = await semaphore.acquire(timeout=5.0)
    assert lock is not None

    # Wait for the lock to expire
    await asyncio.sleep(1.1)

    # Now we should be able to acquire the lock again
    lock2 = await semaphore.acquire(timeout=5.0)
    assert lock2 is not None

    await lock2.release()


@pytest.mark.asyncio
async def test_multiple_semaphores(nats_client: NATS):
    from nats_semaphore import NatsSemaphoreDispatcher

    dispatcher = NatsSemaphoreDispatcher(nats_client, kv="TEST_KV_BUCKET")

    semaphore1 = dispatcher.semaphore(name="semaphore_1", slot_count=1)
    semaphore2 = dispatcher.semaphore(name="semaphore_2", slot_count=1)

    lock1 = await semaphore1.acquire(timeout=5.0)
    assert lock1 is not None

    lock2 = await semaphore2.acquire(timeout=5.0)
    assert lock2 is not None

    await lock1.release()
    await lock2.release()


@pytest.mark.asyncio
async def test_semaphore_reuse(nats_client: NATS):
    from nats_semaphore import NatsSemaphoreDispatcher

    dispatcher = NatsSemaphoreDispatcher(nats_client, kv="TEST_KV_BUCKET")
    semaphore = dispatcher.semaphore(name="test_semaphore", slot_count=1)

    for _ in range(3):
        lock = await semaphore.acquire(timeout=5.0)
        assert lock is not None
        await lock.release()


@pytest.mark.asyncio
async def test_acquire_timeout(nats_client: NATS):
    from nats_semaphore import NatsSemaphoreDispatcher

    dispatcher = NatsSemaphoreDispatcher(nats_client, kv="TEST_KV_BUCKET")
    semaphore = dispatcher.semaphore(name="test_semaphore", slot_count=1)

    lock = await semaphore.acquire(timeout=5.0)
    assert lock is not None

    with pytest.raises(asyncio.TimeoutError):
        await semaphore.acquire(timeout=1.0)

    await lock.release()


@pytest.mark.asyncio
async def test_free_count(nats_client: NATS):
    from nats_semaphore import NatsSemaphoreDispatcher

    dispatcher = NatsSemaphoreDispatcher(nats_client, kv="TEST_KV_BUCKET")
    semaphore = dispatcher.semaphore(name="test_semaphore", slot_count=3)

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
