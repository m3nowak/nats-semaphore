import sys
from asyncio import Semaphore, TimeoutError
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncGenerator

from nats.aio.client import Client as NATS
from nats.js import JetStreamContext
from nats.js.api import KeyValueConfig
from nats.js.errors import BucketNotFoundError, KeyWrongLastSequenceError, NoKeysError
from nats.js.kv import KeyValue

if sys.version_info >= (3, 11):
    from asyncio import timeout as asyncio_timeout
else:
    from async_timeout import timeout as asyncio_timeout

_MAX_BYTES_DEFAULT = 1 * 1024 * 1024  # 1 MB
_DESCRIPTION_DEFAULT = "Semaphore Bucket"
_LOCK_TIMEOUT_DEFAULT = 60.0  # seconds
_LOCK_TTL_DEFAULT = 60.0  # seconds


class NatsSemaphoreDispatcher:
    _js_ctx: JetStreamContext
    _kv_config: KeyValueConfig
    _setup_semaphore: Semaphore
    _kv: KeyValue | None

    def __init__(self, js_manager: NATS | JetStreamContext, kv: str | KeyValueConfig):
        if isinstance(js_manager, NATS):
            js_manager = js_manager.jetstream()

        self._js_ctx = js_manager

        if isinstance(kv, str):
            kv = KeyValueConfig(
                bucket=kv,
                max_bytes=_MAX_BYTES_DEFAULT,
                description=_DESCRIPTION_DEFAULT,
                ttl=_LOCK_TTL_DEFAULT,
            )

        self._kv_config = kv
        self._setup_semaphore = Semaphore(1)
        self._kv = None

    async def _ensure_bucket(self):
        if self._kv is None:
            async with self._setup_semaphore:
                try:
                    self._kv = await self._js_ctx.key_value(self._kv_config.bucket)
                except BucketNotFoundError:
                    self._kv = await self._js_ctx.create_key_value(self._kv_config)

    async def _get_kv(self) -> KeyValue:
        await self._ensure_bucket()
        assert self._kv is not None
        return self._kv

    def semaphore(self, name: str, slot_count: int) -> "NatsSemaphore":
        return NatsSemaphore(self, name=name, slot_count=slot_count)


class NatsSemaphoreLock:
    _name: str
    _slot_no: int
    _semaphore: "NatsSemaphore"

    def __init__(self, name: str, slot_no: int, semaphore: "NatsSemaphore"):
        self._name = name
        self._slot_no = slot_no
        self._semaphore = semaphore

    async def release(self):
        kv = await self._semaphore._dispatcher._get_kv()
        await kv.delete(f"{self._name}-{self._slot_no}")


class NatsSemaphore:
    _dispatcher: NatsSemaphoreDispatcher
    _name: str
    _slot_count: int
    _slots: set[str]

    def __init__(self, dispatcher: NatsSemaphoreDispatcher, name: str, slot_count: int):
        if slot_count < 1:
            raise ValueError("slot_count must be at least 1")

        self._dispatcher = dispatcher
        self._name = name
        self._slot_count = slot_count
        self._slots = set([f"{name}-{i}" for i in range(slot_count)])

    async def _get_free_slots(self) -> set[str]:
        kv = await self._dispatcher._get_kv()
        try:
            keys = await kv.keys()
        except NoKeysError:
            keys = []
        return self._slots - set(keys)

    async def current_free_count(self) -> int:
        free_slots = await self._get_free_slots()
        return len(free_slots)

    async def acquire(self, timeout: float = _LOCK_TIMEOUT_DEFAULT) -> NatsSemaphoreLock:
        kv = await self._dispatcher._get_kv()

        status = await kv.status()
        ttl_nanos = status.stream_info.config.max_age
        if ttl_nanos is None:
            ttl_nanos = 0
        ttl_seconds = ttl_nanos / 1e9 if ttl_nanos > 0 else float("inf")

        watcher = await kv.watchall()
        taken_slots: dict[str, float] = {}

        try:
            async with asyncio_timeout(timeout):
                while True:
                    now = datetime.now(timezone.utc).timestamp()

                    candidates = []
                    active_taken_slots = {}
                    for slot in self._slots:
                        expiry = taken_slots.get(slot)
                        if expiry is None or now >= expiry:
                            candidates.append(slot)
                        else:
                            active_taken_slots[slot] = expiry
                    taken_slots = active_taken_slots

                    for candidate in candidates:
                        try:
                            await kv.create(candidate, b"LOCKED")
                            await watcher.stop()
                            return NatsSemaphoreLock(
                                name=self._name,
                                slot_no=int(candidate.split("-")[-1]),
                                semaphore=self,
                            )
                        except KeyWrongLastSequenceError:
                            pass

                    next_expiry = min(taken_slots.values()) if taken_slots else float("inf")
                    wait_time = max(0.0, next_expiry - now)

                    if wait_time == float("inf"):
                        wait_time = 1.0

                    try:
                        ud = await watcher.updates(timeout=wait_time)
                        if ud:
                            if ud.key in self._slots:
                                if ud.operation == "DEL" or ud.operation == "PURGE":
                                    taken_slots.pop(ud.key, None)
                                else:
                                    created = 0.0
                                    if isinstance(ud.created, int):
                                        created = ud.created / 1e9
                                    elif hasattr(ud.created, "timestamp"):
                                        created = ud.created.timestamp()  # type: ignore
                                    taken_slots[ud.key] = created + ttl_seconds
                    except TimeoutError:
                        pass
        except TimeoutError:
            await watcher.stop()
            raise TimeoutError(f"Timeout while acquiring semaphore '{self._name}'")

    @asynccontextmanager
    async def lock(self, timeout: float = _LOCK_TIMEOUT_DEFAULT) -> AsyncGenerator[NatsSemaphoreLock, None]:
        lock = await self.acquire(timeout=timeout)
        try:
            yield lock
        finally:
            await lock.release()
