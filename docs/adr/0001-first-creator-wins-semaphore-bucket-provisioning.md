# First creator wins semaphore bucket provisioning

When multiple application instances concurrently request provisioning of the same semaphore bucket, nats-semaphore creates it if absent and otherwise binds to the bucket that already exists. The first successful creator determines the effective configuration; later instances neither update nor reject a bucket whose operational settings differ from their request, but emit a configuration-mismatch warning so deployments remain available without silently hiding configuration drift.

## Considered Options

- Reject an existing bucket whose configuration differs from the request.
- Update an existing bucket to match each request.
- Bind to the existing bucket and report differences.

## Consequences

Concurrent requests with different configurations have a nondeterministic winner. All instances converge on one bucket, and operators are responsible for resolving any reported drift.
