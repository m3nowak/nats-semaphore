import asyncio
import contextlib
import logging
import re
import sys
import warnings
from asyncio import Semaphore, TimeoutError
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncGenerator

from nats.aio.client import Client as NATS
from nats.js import JetStreamContext
from nats.js.api import KeyValueConfig, StorageType
from nats.js.errors import APIError, BucketNotFoundError, KeyWrongLastSequenceError, NoKeysError
from nats.js.kv import KeyValue

if sys.version_info >= (3, 11):
    from asyncio import timeout as asyncio_timeout
else:
    from async_timeout import timeout as asyncio_timeout

_MAX_BYTES_DEFAULT = 1 * 1024 * 1024  # 1 MB
_DESCRIPTION_DEFAULT = "Semaphore Bucket"
_LOCK_TIMEOUT_DEFAULT = 10.0  # seconds
_LOCK_TTL_DEFAULT = 10.0  # seconds
_LOCK_RENEW_INTERVAL_DEFAULT = 5.0  # seconds
_WRONG_LAST_SEQUENCE_ERROR = 10071
_VALID_BUCKET_RE = re.compile(r"^[a-zA-Z0-9_-]+$")

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SemaphoreBucketConfig:
    bucket: str = "SEMAPHORES"
    description: str = _DESCRIPTION_DEFAULT
    ttl: float | None = _LOCK_TTL_DEFAULT
    max_bytes: int = _MAX_BYTES_DEFAULT
    storage: StorageType = StorageType.MEMORY
    replicas: int = 1

    def __post_init__(self):
        if _VALID_BUCKET_RE.fullmatch(self.bucket) is None:
            raise ValueError("bucket must contain only letters, digits, underscores, or hyphens")
        if self.ttl is not None and self.ttl < 0:
            raise ValueError("ttl must be greater than or equal to 0")
        if self.max_bytes <= 0:
            raise ValueError("max_bytes must be greater than 0")
        if self.replicas < 1:
            raise ValueError("replicas must be at least 1")
        if not isinstance(self.storage, StorageType):
            raise ValueError("storage must be a StorageType")


class SemaphoreBucketConfigMismatchWarning(UserWarning):
    """Requested semaphore bucket settings differ from the effective settings."""


class NatsSemaphoreContext:
    _js_ctx: JetStreamContext
    _bucket: str | SemaphoreBucketConfig
    _setup_semaphore: Semaphore
    _kv: KeyValue | None

    def __init__(self, js_manager: NATS | JetStreamContext, bucket: str | SemaphoreBucketConfig):
        if isinstance(js_manager, NATS):
            js_manager = js_manager.jetstream()

        self._js_ctx = js_manager
        self._bucket = bucket
        self._setup_semaphore = Semaphore(1)
        self._kv = None

    async def _ensure_bucket(self):
        if self._kv is None:
            async with self._setup_semaphore:
                if self._kv is not None:
                    return

                if isinstance(self._bucket, str):
                    self._kv = await self._js_ctx.key_value(self._bucket)
                    return

                try:
                    self._kv = await self._js_ctx.key_value(self._bucket.bucket)
                except BucketNotFoundError:
                    config = KeyValueConfig(
                        bucket=self._bucket.bucket,
                        description=self._bucket.description,
                        ttl=self._bucket.ttl,
                        max_bytes=self._bucket.max_bytes,
                        storage=self._bucket.storage,
                        replicas=self._bucket.replicas,
                        history=1,
                    )
                    try:
                        self._kv = await self._js_ctx.create_key_value(config)
                    except Exception as provisioning_error:
                        try:
                            self._kv = await self._js_ctx.key_value(self._bucket.bucket)
                        except Exception:
                            raise provisioning_error

                await self._warn_on_config_mismatch(self._kv, self._bucket)

    async def _warn_on_config_mismatch(self, kv: KeyValue, requested: SemaphoreBucketConfig):
        stream_config = (await kv.status()).stream_info.config
        requested_ttl = requested.ttl or 0
        effective_ttl = stream_config.max_age or 0
        fields = {
            "ttl": (requested_ttl, effective_ttl),
            "max_bytes": (requested.max_bytes, stream_config.max_bytes),
            "storage": (requested.storage, stream_config.storage),
            "replicas": (requested.replicas, stream_config.num_replicas),
        }
        differences = [
            f"{field}(requested={requested_value!r}, effective={effective_value!r})"
            for field, (requested_value, effective_value) in fields.items()
            if requested_value != effective_value
        ]
        if differences:
            warnings.warn(
                f"Semaphore bucket {requested.bucket!r} configuration differs: {', '.join(differences)}",
                SemaphoreBucketConfigMismatchWarning,
                stacklevel=5,
            )

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
    _revision: int
    _renew_task: asyncio.Task[None] | None
    _is_lost: bool

    def __init__(
        self,
        name: str,
        slot_no: int,
        semaphore: "NatsSemaphore",
        revision: int,
        renew_interval: float,
    ):
        self._name = name
        self._slot_no = slot_no
        self._semaphore = semaphore
        self._revision = revision
        self._renew_task = None
        self._is_lost = False

        if renew_interval > 0:
            self._renew_task = asyncio.create_task(self._auto_renew(renew_interval))

    async def _auto_renew(self, renew_interval: float):
        try:
            while True:
                await asyncio.sleep(renew_interval)
                await self.renew()
        except KeyWrongLastSequenceError:
            self._is_lost = True
        except Exception:
            logger.exception("Auto-renew failed for lock %s-%s", self._name, self._slot_no)

    async def release(self):
        if self._renew_task is not None:
            self._renew_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._renew_task
            self._renew_task = None

        if self._is_lost:
            return

        kv = await self._semaphore._context._get_kv()
        try:
            await kv.delete(f"{self._name}-{self._slot_no}", last=self._revision)
        except APIError as error:
            if error.err_code != _WRONG_LAST_SEQUENCE_ERROR:
                raise
            self._is_lost = True

    async def renew(self):
        kv = await self._semaphore._context._get_kv()
        try:
            self._revision = await kv.update(f"{self._name}-{self._slot_no}", b"LOCKED", last=self._revision)
        except KeyWrongLastSequenceError as e:
            raise KeyWrongLastSequenceError(
                f"Lock for slot '{self._name}-{self._slot_no}' was lost and cannot be renewed."
            ) from e


class NatsSemaphore:
    _context: NatsSemaphoreContext
    _name: str
    _slot_count: int
    _slots: set[str]

    def __init__(self, context: NatsSemaphoreContext, name: str, slot_count: int):
        if slot_count < 1:
            raise ValueError("slot_count must be at least 1")

        self._context = context
        self._name = name
        self._slot_count = slot_count
        self._slots = set([f"{name}-{i}" for i in range(slot_count)])

    async def _get_free_slots(self) -> set[str]:
        kv = await self._context._get_kv()
        try:
            keys = await kv.keys()
        except NoKeysError:
            keys = []
        return self._slots - set(keys)

    async def current_free_count(self) -> int:
        free_slots = await self._get_free_slots()
        return len(free_slots)

    async def acquire(
        self,
        timeout: float = _LOCK_TIMEOUT_DEFAULT,
        renew_interval: float = _LOCK_RENEW_INTERVAL_DEFAULT,
    ) -> NatsSemaphoreLock:
        if renew_interval < 0:
            raise ValueError("renew_interval must be greater than or equal to 0")

        kv = await self._context._get_kv()

        status = await kv.status()
        effective_ttl = status.stream_info.config.max_age or 0
        ttl_seconds = effective_ttl if effective_ttl > 0 else float("inf")
        if renew_interval > 0 and renew_interval >= ttl_seconds:
            raise ValueError("renew_interval must be shorter than the semaphore bucket TTL")

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
                            revision = await kv.create(candidate, b"LOCKED")
                            await watcher.stop()
                            return NatsSemaphoreLock(
                                name=self._name,
                                slot_no=int(candidate.split("-")[-1]),
                                semaphore=self,
                                revision=revision,
                                renew_interval=renew_interval,
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
    async def lock(
        self,
        timeout: float = _LOCK_TIMEOUT_DEFAULT,
        renew_interval: float = _LOCK_RENEW_INTERVAL_DEFAULT,
    ) -> AsyncGenerator[NatsSemaphoreLock, None]:
        lock = await self.acquire(timeout=timeout, renew_interval=renew_interval)
        try:
            yield lock
        finally:
            await lock.release()
