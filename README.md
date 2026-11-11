# Supplier Capture

Local, crash-safe order ingestion service. Python 3.12+, FastAPI, httpx,
SQLite. No external services: the "supplier" is a local simulator
(`fixtures/source_server.py`) and all state lives in one SQLite database.

## What it does

`capture/api.py` accepts capture jobs over HTTP. `capture/worker.py` runs a
job under a SQLite-backed lease, `capture/source.py` pages the upstream
protocol, and `capture/store.py` makes every page durable in one atomic
SQLite transaction.

The pipeline is safe against the failure modes that produced missing orders,
stale versions overwriting newer ones, and cursors advancing without data:

- **Atomic page commits.** Each page's order changes, the page checkpoint
  (`cursor` / `change_seq`) and the job counters are written in a single
  `BEGIN IMMEDIATE` ... `COMMIT`. A crash either has the whole page or none
  of it; a checkpoint can never be ahead of the data.
- **Monotonic version gating.** Every order upsert only wins when the new
  `version` is strictly greater. Duplicate, replayed and out-of-order events
  become no-ops, an older version never overwrites a newer one, and a deleted
  order is never resurrected by an older snapshot.
- **Staged full snapshot.** A full run writes into `snapshot_staging` while
  the currently visible `orders` stay untouched. Only after every page is
  durable does one swap transaction promote the frozen view, so a half
  finished snapshot can never replace good visible data.
- **Fencing leases.** Each lease has an owner and a monotonic
  `lease_generation`. Every guarded write checks owner + generation +
  expiry inside its transaction. A worker whose lease expired and was taken
  over keeps its stale generation and is fenced off: even if it returns late,
  its commits roll back and it cannot overwrite the new owner's progress.
  Coordination is entirely in SQLite rows — no in-process or cross-process
  lock is required.
- **Retry vs. protocol split.** Transport errors, 429 (honoring
  `Retry-After`), transient 5xx and malformed JSON are
  `RetryableSourceError` and get bounded exponential backoff. Non-retryable
  statuses (e.g. 410 snapshot expired) and schema violations are
  `ProtocolError`: the job stops immediately and the checkpoint is not moved.
- **Cancellation.** A cancel flag is checked before every new request and
  during backoff sleeps; the job stops without issuing further requests and
  leaves a resumable checkpoint.

## Data model

- `jobs` — one row per job: mode, checkpoint (`snapshot_id`, `cursor`,
  `change_seq`), counters (`received`, `applied`, `retries`), last `error`,
  lease columns (`lease_owner`, `lease_until`, `lease_generation`) and
  `cancel_requested`.
- `orders` — visible order state keyed by `(source_id, order_id)` with the
  highest applied `version`; `deleted=1` rows are tombstones.
- `effects` — every applied version transition once, keyed by
  `(source_id, order_id, version)`, proving no duplicate side effects.
- `snapshot_staging` — per-job holding area for an in-progress full snapshot.
- `source_checkpoints` — durable per-source resume position.

`received` counts records seen upstream (duplicates included); `applied`
counts version transitions that actually changed local data.

## Running

```bash
python -m pip install -r requirements.txt

# terminal 1: local supplier simulator
python -m uvicorn fixtures.source_server:app --host 127.0.0.1 --port 8127

# terminal 2: capture API (BackgroundTasks run the worker in-process)
python -m uvicorn capture.api:app --host 127.0.0.1 --port 8227
```

`CAPTURE_DB` selects the SQLite file (default `var/capture.db`). A worker can
also run standalone, which is how cross-process tests drive it:

```bash
python -m capture.worker JOB_ID
```

## HTTP interface

| Method & path | Purpose |
| --- | --- |
| `POST /jobs` | Create a job (`full` or `incremental`). 409 if the source already has an active job. |
| `GET /jobs/{id}` | Full job state: counters, retries, last error, lease status. |
| `POST /jobs/{id}/cancel` | Request cancellation; the worker stops before the next request. |
| `POST /jobs/{id}/resume` | Requeue a terminal/expired job and continue from its checkpoint. Optional `{"change_seq": n}` for incremental jobs. |
| `GET /sources/{id}/checkpoint` | Durable snapshot id / change_seq position. |
| `GET /sources/{id}/orders` | Visible (non-deleted) orders. |

```bash
curl -X POST localhost:8227/jobs -H 'content-type: application/json' \
  -d '{"source_id":"supplier-a","source_url":"http://127.0.0.1:8127","mode":"full"}'
curl localhost:8227/jobs/$JOB_ID
curl -X POST localhost:8227/jobs/$JOB_ID/cancel
curl -X POST localhost:8227/jobs/$JOB_ID/resume
curl localhost:8227/sources/supplier-a/checkpoint
curl localhost:8227/sources/supplier-a/orders
```

## Failure injection and reproduction

`CAPTURE_FAILPOINT` makes a worker exit (exit code 77) at a precise point,
firing once per process so a resumed worker runs through:

| Value | Injection point |
| --- | --- |
| `after_read` | After the HTTP page is read, before any transaction. |
| `before_commit` | Inside the page transaction, genuinely before `COMMIT` (SQLite rolls it back). |
| `after_commit` | After `COMMIT`, before the next request / response. |
| `expire_lease` | Release an expired lease and linger, letting a second worker take over. |

Crash-recovery reproduction (full snapshot, three pages of two orders):

```bash
# 1. queue a job, then run a worker that dies after reading the first page
CAPTURE_FAILPOINT=after_read CAPTURE_DB=var/c.db python -m capture.worker JOB_ID
# 2. clear the stale lease (or wait for CAPTURE_LEASE_TTL), requeue, resume
python -m capture.worker JOB_ID   # after POST /jobs/JOB_ID/resume
# verify: six orders, six effects, no duplicates
python -m pytest tests/test_integration.py -q
```

Cross-process lease takeover:

```bash
CAPTURE_FAILPOINT=expire_lease CAPTURE_LEASE_TTL=1 python -m capture.worker JOB_ID &
# once the lease is expired, a second process takes over and completes it
python -m capture.worker JOB_ID
```

Tuning knobs (all environment variables, read per worker):
`CAPTURE_LEASE_TTL`, `CAPTURE_MAX_ATTEMPTS`, `CAPTURE_BACKOFF_BASE`,
`CAPTURE_BACKOFF_CAP`, `CAPTURE_SLEEP_SLICE`. `CAPTURE_AUTO_START=0` keeps
newly created/resumed jobs queued (used by tests that own the worker
processes).

The simulator is controlled by `POST /control` with `scenario` (`normal`,
`transient`, `malformed`, `replay`), `delay` seconds and `page_size`.
`transient` injects a 429 (with `Retry-After`) then a 503 per page key;
`malformed` returns broken JSON; `replay` repeats the previous page's last
row. All records are synthetic and local only.

## Tests

```bash
python -m pytest -q
```

The suite starts a real uvicorn simulator per test and asserts against the
actual SQLite contents (visible orders, tombstones, `effects`, checkpoints
and job counters), not just HTTP statuses. It covers:

- full → incremental happy path, counters and checkpoints;
- 429/503 retries with backoff, malformed-JSON retry budget, protocol errors
  that must not advance the checkpoint;
- cancellation during work and during backoff, with a resumable checkpoint;
- crashes at all three injection points, in full and incremental modes, with
  no data loss and no duplicate effects;
- cross-process contention (two workers, one lease), lease expiry and
  generation fencing, stale-writer rollback;
- page replay, out-of-order/stale versions, deletions, the second full
  snapshot not resurrecting deleted orders, and partial snapshots never
  replacing visible data.

## Known limitations

- A single SQLite database is the coordination and storage layer; it is
  durable and correct for processes sharing that file, but it is not a
  horizontally scalable multi-node design.
- There is no automatic supervisor: after a crash a job must be resumed via
  `POST /jobs/{id}/resume` (or an external scheduler that calls it once the
  lease has expired). Workers do not self-restart.
- Lease expiry is wall-clock based; large clock skew between hosts sharing
  the database can cause premature takeover. All processes are expected to
  share a reasonably synchronized clock.
- The full-snapshot swap is version-gated rather than a wholesale replace:
  orders absent from a new snapshot but already at an equal/higher local
  version are retained rather than deleted, which favors never losing data
  over mirroring an exact upstream set.
- The bundled simulator is a test fixture, not a production client.
