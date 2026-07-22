# nats-semaphore

A distributed semaphore implementation for Python using NATS JetStream KeyValue stores. This library allows you to coordinate access to shared resources across multiple processes or services with ease.

## Features

- **Distributed Locking**: Leverages NATS JetStream KeyValue stores for reliable distributed coordination.
- **Configurable Concurrency**: Define the number of slots (concurrency limit) for each semaphore.
- **Asyncio Support**: Built from the ground up for Python's `asyncio`.
- **Context Manager**: Easy-to-use `async with` syntax for automatic lock acquisition and release.
- **Timeout Handling**: Support for acquisition timeouts.
- **Automatic Renewal**: Locks are renewed periodically while held, so the default 10 second KV TTL does not expire during longer work.

## Installation

`pip install nats-semaphore`

## Usage

Here is a simple example of how to use `nats-semaphore`:

```python
import asyncio
import nats
from nats_semaphore import NatsSemaphoreContext, SemaphoreBucketConfig

async def main():
    # 1. Connect to NATS
    nc = await nats.connect("nats://localhost:4222")

    # 2. Explicitly opt into lazy create-or-bind provisioning.
    semaphore_context = NatsSemaphoreContext(nc, bucket=SemaphoreBucketConfig())

    # To bind only to infrastructure provisioned elsewhere, pass its bucket name.
    # This raises BucketNotFoundError on first use if the bucket does not exist.
    # semaphore_context = NatsSemaphoreContext(nc, bucket="SEMAPHORES")

    # 3. Define a semaphore
    # 'name' identifies the resource.
    # 'slot_count' is the maximum number of concurrent locks allowed.
    semaphore = semaphore_context.semaphore(name="my-shared-resource", slot_count=3)

    # 4. Acquire a lock using a context manager
    try:
        # Try to acquire a lock, waiting up to 5 seconds.
        # By default, the lock is renewed every 5 seconds.
        async with semaphore.lock(timeout=5.0) as lock:
            print("Lock acquired! Doing work...")
            await asyncio.sleep(1)
            print("Work done.")
    except asyncio.TimeoutError:
        print("Failed to acquire lock within timeout.")

    # Alternative: Manual acquire/release
    try:
        lock = await semaphore.acquire(timeout=5.0)
        print("Manually acquired lock.")
        # ... do work ...
    finally:
        await lock.release()
        print("Manually released lock.")

    # Disable automatic renewal by setting renew_interval to 0.
    lock = await semaphore.acquire(timeout=5.0, renew_interval=0)
    await lock.release()

    await nc.close()

if __name__ == "__main__":
    asyncio.run(main())
```

`SemaphoreBucketConfig()` provisions the `SEMAPHORES` bucket lazily with a 10 second TTL, a 1 MiB limit, memory storage, and one replica. Acquired locks are renewed every 5 seconds unless `renew_interval=0` is passed to `acquire()` or `lock()`.

Custom provisioning remains semaphore-specific:

```python
from nats.js.api import StorageType
from nats_semaphore import NatsSemaphoreContext, SemaphoreBucketConfig

context = NatsSemaphoreContext(
    nc,
    bucket=SemaphoreBucketConfig(
        bucket="MY_SEMAPHORES",
        ttl=30,
        max_bytes=2 * 1024 * 1024,
        storage=StorageType.FILE,
        replicas=3,
    ),
)
```

## Migrating To 0.0.3

The context API intentionally no longer accepts `kv=` or native NATS `KeyValueConfig` values. Replace create-or-bind calls with `bucket=SemaphoreBucketConfig(...)`. Replace `kv="NAME"` with `bucket="NAME"` only when the bucket is provisioned externally and should be bind-only.

## Requirements

- Python >= 3.10
- [nats-py](https://github.com/nats-io/nats.py) >= 2.12

## Compatibility

Compatible with nats-server versions:

- 2.7
- 2.8
- 2.9
- 2.10
- 2.11
- 2.12
- 2.14

## License

Apache-2.0
