# nats-semaphore

A distributed semaphore implementation for Python using NATS JetStream KeyValue stores. This library allows you to coordinate access to shared resources across multiple processes or services with ease.

## Features

- **Distributed Locking**: Leverages NATS JetStream KeyValue stores for reliable distributed coordination.
- **Configurable Concurrency**: Define the number of slots (concurrency limit) for each semaphore.
- **Asyncio Support**: Built from the ground up for Python's `asyncio`.
- **Context Manager**: Easy-to-use `async with` syntax for automatic lock acquisition and release.
- **Timeout Handling**: Support for acquisition timeouts.

## Installation

```bash
pip install nats-semaphore
```

## Usage

Here is a simple example of how to use `nats-semaphore`:

```python
import asyncio
import nats
from nats_semaphore import NatsSemaphoreDispatcher

async def main():
    # 1. Connect to NATS
    nc = await nats.connect("nats://localhost:4222")

    # 2. Initialize the dispatcher
    # 'kv' is the name of the NATS KeyValue bucket to use for storing locks.
    # It will be created if it doesn't exist.
    dispatcher = NatsSemaphoreDispatcher(nc, kv="SEMAPHORE_BUCKET")

    # 3. Define a semaphore
    # 'name' identifies the resource.
    # 'slot_count' is the maximum number of concurrent locks allowed.
    semaphore = dispatcher.semaphore(name="my-shared-resource", slot_count=3)

    # 4. Acquire a lock using a context manager
    try:
        # Try to acquire a lock, waiting up to 5 seconds
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

    await nc.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## Requirements

- Python >= 3.10
- [nats-py](https://github.com/nats-io/nats.py) >= 2.12

## License

Apache-2.0
