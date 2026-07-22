# NATS Semaphore

The language of compatibility and distributed concurrency guarantees provided by nats-semaphore.

## Language

**Supported NATS version**:
A NATS Server release line with which nats-semaphore publicly guarantees compatibility.
_Avoid_: Tested version

**Semaphore bucket**:
A NATS KeyValue bucket dedicated to semaphore coordination state.
_Avoid_: Semaphore database

**Semaphore bucket configuration**:
A description of a NATS KeyValue bucket intended to store semaphore coordination state and eligible to be provisioned by nats-semaphore.
_Avoid_: Semaphore database, database configuration

**Semaphore slot**:
One unit of the concurrency capacity of a semaphore.

**Semaphore lock**:
A time-bounded claim on one semaphore slot that may be renewed while its holder remains active.
_Avoid_: Permanent lock
