"""SQLite persistence for the capture service.

All multi-statement writes go through :func:`transaction`, which opens one
deferred transaction and commits atomically. Checkpoint movement therefore can
never be observed without the order changes of the same page being durable.
"""
import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA_VERSION = 2

#: Job states. Only the four ACTIVE_STATES hold a lease that blocks a
#: competing worker / a second job for the same source.
ACTIVE_STATES = ('queued', 'running', 'retrying', 'cancelling')


@contextmanager
def read_connection():
    """Read-only helper that always closes its connection (the bare sqlite3
    context manager commits but does not close)."""
    db = connect()
    try:
        yield db
    finally:
        db.close()
ACTIVE_STATES = ('queued', 'running', 'retrying', 'cancelling')


def connect():
    path = Path(os.environ.get('CAPTURE_DB', 'var/capture.db'))
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA busy_timeout=30000')
    db.execute('PRAGMA foreign_keys=ON')
    db.execute('PRAGMA user_version')
    _create_schema(db)
    return db


def _create_schema(db):
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS jobs(
            id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            source_url TEXT NOT NULL,
            mode TEXT NOT NULL,
            state TEXT NOT NULL,
            snapshot_id TEXT,
            cursor TEXT,
            change_seq INTEGER NOT NULL DEFAULT 0,
            received INTEGER NOT NULL DEFAULT 0,
            applied INTEGER NOT NULL DEFAULT 0,
            retries INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            lease_owner TEXT,
            lease_until REAL NOT NULL DEFAULT 0,
            lease_generation INTEGER NOT NULL DEFAULT 0,
            cancel_requested INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS orders(
            source_id TEXT NOT NULL,
            order_id TEXT NOT NULL,
            version INTEGER NOT NULL,
            deleted INTEGER NOT NULL,
            payload TEXT NOT NULL,
            PRIMARY KEY(source_id, order_id)
        );
        -- Every applied version transition is recorded exactly once, even if
        -- the same page is replayed after a crash.
        CREATE TABLE IF NOT EXISTS effects(
            source_id TEXT NOT NULL,
            order_id TEXT NOT NULL,
            version INTEGER NOT NULL,
            PRIMARY KEY(source_id, order_id, version)
        );
        -- Staging area for a full snapshot; visible orders are only replaced
        -- inside the atomic swap at the end of the full job.
        CREATE TABLE IF NOT EXISTS snapshot_staging(
            job_id TEXT NOT NULL,
            order_id TEXT NOT NULL,
            version INTEGER NOT NULL,
            deleted INTEGER NOT NULL,
            payload TEXT NOT NULL,
            PRIMARY KEY(job_id, order_id)
        );
        -- Durable per-source checkpoint used by incremental jobs.
        CREATE TABLE IF NOT EXISTS source_checkpoints(
            source_id TEXT PRIMARY KEY,
            snapshot_id TEXT,
            change_seq INTEGER NOT NULL DEFAULT 0,
            last_job_id TEXT,
            updated_at REAL NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source_id, state);
        """
    )
    # Additive migration for databases created by the initial schema: every
    # column introduced later is created if missing, so an existing database
    # opens without losing data.
    cols = {r['name'] for r in db.execute('PRAGMA table_info(jobs)')}
    added_columns = {
        'lease_owner': 'TEXT',
        'lease_until': 'REAL NOT NULL DEFAULT 0',
        'lease_generation': 'INTEGER NOT NULL DEFAULT 0',
        'cancel_requested': 'INTEGER NOT NULL DEFAULT 0',
        'created_at': 'REAL NOT NULL DEFAULT 0',
        'updated_at': 'REAL NOT NULL DEFAULT 0',
    }
    for name, decl in added_columns.items():
        if name not in cols:
            db.execute('ALTER TABLE jobs ADD COLUMN %s %s' % (name, decl))
    db.execute('PRAGMA user_version = %d' % SCHEMA_VERSION)


@contextmanager
def transaction():
    """Single BEGIN/COMMIT (or ROLLBACK) around a unit of work."""
    db = connect()
    try:
        db.execute('BEGIN IMMEDIATE')
        yield db
        _failpoint_before_commit()
        db.execute('COMMIT')
    except BaseException:
        db.execute('ROLLBACK')
        raise
    finally:
        db.close()


_CRASH_FIRED = set()
# Armed only around page-apply transactions so lease bookkeeping commits
# never trigger the before-commit crash hook.
_before_commit_armed = False


def arm_before_commit_crash():
    global _before_commit_armed
    _before_commit_armed = True


def disarm_before_commit_crash():
    global _before_commit_armed
    _before_commit_armed = False


def _failpoint_before_commit():
    """Crash hook fired genuinely before COMMIT, while the transaction is
    still open. SQLite rolls the uncommitted transaction back on recovery."""
    if not _before_commit_armed:
        return
    if os.environ.get('CAPTURE_FAILPOINT') != 'before_commit':
        return
    if 'before_commit' in _CRASH_FIRED:
        return
    _CRASH_FIRED.add('before_commit')
    os._exit(77)


def job(job_id):
    with read_connection() as db:
        row = db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
        return dict(row) if row else None


def checkpoint(source_id):
    with read_connection() as db:
        row = db.execute(
            'SELECT * FROM source_checkpoints WHERE source_id=?', (source_id,)
        ).fetchone()
        return dict(row) if row else None


def active_job_for_source(source_id, now):
    """Return an active (queued/running/retrying/cancelling) job of a source.

    Active state alone blocks a second trigger, even before the lease has
    been acquired: lease expiry governs who may *execute*, not whether a new
    job for the same source may be created.
    """
    with read_connection() as db:
        row = db.execute(
            "SELECT * FROM jobs WHERE source_id=? AND state IN (%s) "
            "ORDER BY created_at DESC LIMIT 1"
            % ','.join('?' for _ in ACTIVE_STATES),
            (source_id, *ACTIVE_STATES),
        ).fetchone()
        return dict(row) if row else None


def create_job(job_id, source_id, source_url, mode, change_seq, now):
    with transaction() as db:
        db.execute(
            'INSERT INTO jobs(id,source_id,source_url,mode,state,change_seq,'
            'created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)',
            (job_id, source_id, source_url, mode, 'queued', change_seq, now, now),
        )


def acquire_lease(job_id, owner, now, ttl):
    """Atomically claim (or take over) a job lease.

    Ownership passes only when the current lease is free or expired, so two
    processes can never hold the same lease generation at the same time.
    Every takeover increments ``lease_generation`` (a fencing token), so a
    worker that returns late after losing its lease is fenced off by every
    guarded write and can never reclaim the job behind the new owner's back.
    Returns the new generation on success, or None if a live lease exists.
    """
    with transaction() as db:
        cur = db.execute(
            'UPDATE jobs SET lease_owner=?, lease_until=?, state=?, '
            'lease_generation=lease_generation+1, updated_at=? '
            'WHERE id=? AND lease_until<=?',
            (owner, now + ttl, 'running', now, job_id, now),
        )
        if cur.rowcount != 1:
            return None
        return db.execute(
            'SELECT lease_generation AS g FROM jobs WHERE id=?', (job_id,)
        ).fetchone()['g']


def renew_lease(job_id, owner, generation, now, ttl):
    """Extend the lease; fails if it was lost or fenced by a takeover."""
    with transaction() as db:
        cur = db.execute(
            'UPDATE jobs SET lease_until=?, updated_at=? '
            'WHERE id=? AND lease_owner=? AND lease_generation=? AND lease_until>?',
            (now + ttl, now, job_id, owner, generation, now),
        )
        return cur.rowcount == 1


def lease_live(job_id, owner, generation, now):
    with read_connection() as db:
        row = db.execute(
            'SELECT lease_owner, lease_until, lease_generation FROM jobs WHERE id=?',
            (job_id,),
        ).fetchone()
        return bool(row and row['lease_owner'] == owner
                    and row['lease_generation'] == generation
                    and row['lease_until'] > now)


def attach_snapshot(job_id, owner, generation, now, ttl, snapshot_id, change_seq):
    """Persist the freshly created snapshot view before any page is read,
    guarded by the lease generation so a taken-over worker cannot mutate
    the job."""
    with transaction() as db:
        cur = db.execute(
            'UPDATE jobs SET snapshot_id=?, cursor=?, change_seq=?, '
            'lease_until=?, updated_at=? '
            'WHERE id=? AND lease_owner=? AND lease_generation=? AND lease_until>?',
            (snapshot_id, '0', change_seq, now + ttl, now, job_id, owner,
             generation, now),
        )
        if cur.rowcount != 1:
            raise LeaseLost(job_id)


def request_cancel(job_id, now):
    """Mark the job for cancellation. Running workers notice before the
    next request and before the next transactional commit."""
    with transaction() as db:
        cur = db.execute(
            "UPDATE jobs SET cancel_requested=1, state=CASE WHEN state='running' "
            "THEN 'cancelling' ELSE state END, updated_at=? "
            "WHERE id=? AND state IN (%s)" % ','.join('?' for _ in ACTIVE_STATES),
            (now, job_id, *ACTIVE_STATES),
        )
        return cur.rowcount == 1


def mark_resumable(job_id, generation, state, error, now):
    """Terminal/resumable transition allowed only for the current lease
    generation; a stale worker cannot overwrite a newer owner's progress."""
    with transaction() as db:
        cur = db.execute(
            'UPDATE jobs SET state=?, error=?, lease_owner=NULL, lease_until=0, '
            'updated_at=? WHERE id=? AND lease_generation=?',
            (state, error, now, job_id, generation),
        )
        return cur.rowcount == 1


def requeue(job_id, now):
    """Make a job resumable.

    Terminal jobs can always be requeued. An active job may only be requeued
    once its lease has expired (crashed/stale worker); a still-live lease is
    rejected so a running worker cannot be hijacked. Returns False otherwise.
    """
    with transaction() as db:
        cur = db.execute(
            "UPDATE jobs SET state='queued', error=NULL, lease_owner=NULL, "
            'lease_until=0, lease_generation=lease_generation+1, '
            'cancel_requested=0, updated_at=? WHERE id=? AND ('
            "state IN ('failed','cancelled','completed') OR lease_until<=?)",
            (now, job_id, now),
        )
        return cur.rowcount == 1


def increment_retries(job_id, owner, generation, now):
    """Count one retry; returns False if the lease was lost/fenced meanwhile."""
    with transaction() as db:
        cur = db.execute(
            'UPDATE jobs SET retries=retries+1, state=?, updated_at=? '
            'WHERE id=? AND lease_owner=? AND lease_generation=? AND lease_until>?',
            ('retrying', now, job_id, owner, generation, now),
        )
        return cur.rowcount == 1


def commit_full_page(job_id, owner, generation, now, ttl, next_cursor,
                     change_seq, items):
    """Atomically stage one snapshot page and move the page checkpoint.

    The staging upsert, the cursor/change_seq update and the received counter
    are committed in one transaction guarded by the lease, so a crash can
    never leave the checkpoint ahead of the staged data (or vice versa).
    Replayed rows are version-gated. Version transitions only become
    "applied" when the snapshot is promoted by :func:`swap_snapshot`; until
    then nothing is visible, so ``applied`` is deliberately untouched here.
    Returns (received, staging_writes).
    """
    with transaction() as db:
        staged = 0
        for item in items:
            cur = db.execute(
                'INSERT INTO snapshot_staging(job_id,order_id,version,deleted,payload) '
                'VALUES(?,?,?,?,?) ON CONFLICT(job_id,order_id) DO UPDATE SET '
                'version=excluded.version, deleted=excluded.deleted, '
                'payload=excluded.payload WHERE excluded.version>snapshot_staging.version',
                (job_id, item['order_id'], item['version'],
                 int(bool(item.get('deleted', False))), json.dumps(item)),
            )
            staged += cur.rowcount
        cur = db.execute(
            'UPDATE jobs SET cursor=?, change_seq=?, '
            'received=received+?, lease_until=?, updated_at=? '
            'WHERE id=? AND lease_owner=? AND lease_generation=? AND lease_until>?',
            (next_cursor, change_seq, len(items), now + ttl, now,
             job_id, owner, generation, now),
        )
        if cur.rowcount != 1:
            raise LeaseLost(job_id)
        return len(items), staged


def swap_snapshot(job_id, source_id, snapshot_id, change_seq, owner,
                  generation, now, ttl):
    """Promote staged rows to visible orders in one transaction.

    Every staged row is applied with a monotonic version guard, so the swap
    cannot downgrade an order, and a tombstone is never written over a newer
    live version (deleted orders stay deleted instead of being resurrected).
    Returns the number of version transitions actually applied.
    """
    with transaction() as db:
        applied = 0
        rows = db.execute(
            'SELECT order_id, version, deleted, payload FROM snapshot_staging '
            'WHERE job_id=?', (job_id,),
        ).fetchall()
        for row in rows:
            cur = db.execute(
                'INSERT INTO orders(source_id,order_id,version,deleted,payload) '
                'VALUES(?,?,?,?,?) ON CONFLICT(source_id,order_id) DO UPDATE SET '
                'version=excluded.version, deleted=excluded.deleted, '
                'payload=excluded.payload WHERE excluded.version>orders.version',
                (source_id, row['order_id'], row['version'], row['deleted'],
                 row['payload']),
            )
            if cur.rowcount:
                applied += 1
                db.execute(
                    'INSERT OR IGNORE INTO effects VALUES(?,?,?)',
                    (source_id, row['order_id'], row['version']),
                )
        db.execute('DELETE FROM snapshot_staging WHERE job_id=?', (job_id,))
        db.execute(
            'INSERT INTO source_checkpoints(source_id,snapshot_id,change_seq,'
            'last_job_id,updated_at) VALUES(?,?,?,?,?) '
            'ON CONFLICT(source_id) DO UPDATE SET snapshot_id=excluded.snapshot_id, '
            'change_seq=excluded.change_seq, last_job_id=excluded.last_job_id, '
            'updated_at=excluded.updated_at',
            (source_id, snapshot_id, change_seq, job_id, now),
        )
        cur = db.execute(
            'UPDATE jobs SET applied=applied+?, lease_until=?, updated_at=? '
            'WHERE id=? AND lease_owner=? AND lease_generation=? AND lease_until>?',
            (applied, now + ttl, now, job_id, owner, generation, now),
        )
        if cur.rowcount != 1:
            raise LeaseLost(job_id)
        return applied


def commit_change_page(job_id, source_id, owner, generation, now, ttl,
                       next_seq, items):
    """Apply one incremental page atomically.

    Version-gated upserts make stale/out-of-order/replayed events no-ops;
    the checkpoint advances to ``next_seq`` in the same transaction, so a
    crash can never move the cursor without the data being durable.
    Returns (received, applied).
    """
    with transaction() as db:
        applied = 0
        for item in items:
            cur = db.execute(
                'INSERT INTO orders(source_id,order_id,version,deleted,payload) '
                'VALUES(?,?,?,?,?) ON CONFLICT(source_id,order_id) DO UPDATE SET '
                'version=excluded.version, deleted=excluded.deleted, '
                'payload=excluded.payload WHERE excluded.version>orders.version',
                (source_id, item['order_id'], item['version'],
                 int(bool(item.get('deleted', False))), json.dumps(item)),
            )
            if cur.rowcount:
                applied += 1
                db.execute(
                    'INSERT OR IGNORE INTO effects VALUES(?,?,?)',
                    (source_id, item['order_id'], item['version']),
                )
        db.execute(
            'INSERT INTO source_checkpoints(source_id,change_seq,last_job_id,'
            'updated_at) VALUES(?,?,?,?) ON CONFLICT(source_id) DO UPDATE SET '
            'change_seq=excluded.change_seq, last_job_id=excluded.last_job_id, '
            'updated_at=excluded.updated_at',
            (source_id, next_seq, job_id, now),
        )
        cur = db.execute(
            'UPDATE jobs SET change_seq=?, received=received+?, '
            'applied=applied+?, lease_until=?, updated_at=? '
            'WHERE id=? AND lease_owner=? AND lease_generation=? AND lease_until>?',
            (next_seq, len(items), applied, now + ttl, now, job_id, owner,
             generation, now),
        )
        if cur.rowcount != 1:
            raise LeaseLost(job_id)
        return len(items), applied


def orders(source_id):
    with read_connection() as db:
        return [json.loads(x['payload']) for x in db.execute(
            'SELECT payload FROM orders WHERE source_id=? AND deleted=0 '
            'ORDER BY order_id', (source_id,))]


def order_rows(source_id):
    """Full internal view (including tombstones) for integration tests."""
    with read_connection() as db:
        return [dict(x) for x in db.execute(
            'SELECT order_id, version, deleted FROM orders WHERE source_id=? '
            'ORDER BY order_id', (source_id,))]


def effects(source_id):
    with read_connection() as db:
        return [tuple(x) for x in db.execute(
            'SELECT order_id, version FROM effects WHERE source_id=? '
            'ORDER BY order_id, version', (source_id,))]


def staging_rows(job_id):
    with read_connection() as db:
        return [dict(x) for x in db.execute(
            'SELECT order_id, version, deleted FROM snapshot_staging '
            'WHERE job_id=? ORDER BY order_id', (job_id,))]


class LeaseLost(RuntimeError):
    """Raised inside a write transaction when the lease guard fails; the
    transaction is rolled back by the ``transaction`` context manager."""


# Backwards-compatible alias used by older imports.
_LeaseLost = LeaseLost


# Backwards-compatible thin wrapper kept for the original unit test.
def apply(source_id, items):
    with transaction() as db:
        for item in items:
            db.execute(
                'INSERT OR REPLACE INTO orders VALUES(?,?,?,?,?)',
                (source_id, item['order_id'], item['version'],
                 int(bool(item.get('deleted', False))), json.dumps(item)),
            )
            db.execute('INSERT OR IGNORE INTO effects VALUES(?,?,?)',
                       (source_id, item['order_id'], item['version']))
