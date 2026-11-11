"""Capture worker.

Runs a job to completion under a SQLite-backed lease. Every page is applied
in one atomic transaction (order changes + checkpoint + counters), so any
process exit leaves a consistent, resumable state: the next worker simply
re-reads the page at the stored cursor / change_seq and the version-gated
writes turn replays into no-ops.

Fault injection (process exits, see README):
    CAPTURE_FAILPOINT=after_read     - after the HTTP page is read
    CAPTURE_FAILPOINT=before_commit  - inside the page transaction, pre-COMMIT
    CAPTURE_FAILPOINT=after_commit   - after COMMIT, before the next request
    CAPTURE_FAILPOINT=expire_lease   - drop this worker's lease and sleep so a
                                       second worker can take over (one-shot)
"""
import os
import sys
import time
import uuid

from . import store
from .source import ProtocolError, RetryableSourceError, Source


def _env_float(name, default):
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return float(default)


# Defaults; each Worker reads the environment at construction so tests (and
# operators) can tune timing without re-importing the module.
DEFAULT_LEASE_TTL = _env_float('CAPTURE_LEASE_TTL', 5.0)
DEFAULT_MAX_ATTEMPTS = int(_env_float('CAPTURE_MAX_ATTEMPTS', 6))
DEFAULT_BACKOFF_BASE = _env_float('CAPTURE_BACKOFF_BASE', 0.2)
DEFAULT_BACKOFF_CAP = _env_float('CAPTURE_BACKOFF_CAP', 5.0)
DEFAULT_SLEEP_SLICE = _env_float('CAPTURE_SLEEP_SLICE', 0.05)

_FAILPOINTS_FIRED = set()


class JobCancelled(RuntimeError):
    pass


def failpoint(name, job_id=None, owner=None, generation=None):
    """Exit the process (or simulate lease loss) at a named injection point.

    Crash points fire once per process so a resumed worker runs through.
    """
    if os.environ.get('CAPTURE_FAILPOINT') != name or name in _FAILPOINTS_FIRED:
        return
    _FAILPOINTS_FIRED.add(name)
    if name == 'expire_lease' and job_id is not None:
        # Simulate a stalled worker whose lease has expired. Real expiry only
        # clears the lease window; the generation moves when the takeover
        # worker acquires it. Linger so the takeover happens while this
        # process is still alive.
        with store.transaction() as db:
            # lease_until in the past models genuine expiry, so another
            # worker's acquire_lease (lease_until <= now) succeeds at once.
            db.execute(
                'UPDATE jobs SET lease_owner=NULL, lease_until=? WHERE id=?',
                (time.time() - 1.0, job_id),
            )
        ttl = _env_float('CAPTURE_LEASE_TTL', DEFAULT_LEASE_TTL)
        time.sleep(max(ttl * 2, 2.0))
        # On waking, a takeover owner now holds a newer generation. This
        # worker must stop rather than re-acquire the lease.
        current = store.job(job_id)
        if (current is None or current['lease_owner'] != owner
                or current['lease_generation'] != generation):
            return
        os._exit(77)
    os._exit(77)


class Worker:
    def __init__(self, job_id, owner=None, clock=time.time, sleep=time.sleep):
        self.job_id = job_id
        self.owner = owner or ('worker-%s-%s' % (os.getpid(), uuid.uuid4().hex[:8]))
        self.now = clock
        self.sleep = sleep
        # Fencing token of the lease this worker currently holds; None until
        # the lease is acquired. A stale worker keeps its old generation and
        # is rejected by every guarded write after a takeover.
        self.generation = None
        self.lease_ttl = _env_float('CAPTURE_LEASE_TTL', DEFAULT_LEASE_TTL)
        self.max_attempts = int(_env_float('CAPTURE_MAX_ATTEMPTS',
                                           DEFAULT_MAX_ATTEMPTS))
        self.backoff_base = _env_float('CAPTURE_BACKOFF_BASE',
                                       DEFAULT_BACKOFF_BASE)
        self.backoff_cap = _env_float('CAPTURE_BACKOFF_CAP', DEFAULT_BACKOFF_CAP)
        self.sleep_slice = _env_float('CAPTURE_SLEEP_SLICE',
                                      DEFAULT_SLEEP_SLICE)

    # -- helpers -----------------------------------------------------------

    def _job(self):
        return store.job(self.job_id)

    def _check_cancel(self):
        job = self._job()
        if job is None:
            raise RuntimeError('job vanished')
        if job['cancel_requested']:
            raise JobCancelled('cancellation requested')
        if not store.lease_live(self.job_id, self.owner, self.generation,
                                self.now()):
            raise store.LeaseLost(self.job_id)

    def _interruptible_sleep(self, seconds):
        """Sleep in short slices, remaining responsive to cancellation and
        to lease takeover."""
        deadline = self.now() + max(0.0, seconds)
        while self.now() < deadline:
            self._check_cancel()
            remaining = deadline - self.now()
            if remaining <= 0:
                break
            self.sleep(min(self.sleep_slice, remaining))
        self._check_cancel()

    def _with_retries(self, operation, label):
        """Call ``operation`` with bounded exponential backoff.

        Only :class:`RetryableSourceError` is retried; protocol errors escape
        immediately and therefore can never advance a checkpoint.
        """
        attempt = 0
        while True:
            try:
                return operation()
            except RetryableSourceError as exc:
                attempt += 1
                if attempt >= self.max_attempts:
                    raise RetryableSourceError(
                        '%s failed after %d attempts: %s'
                        % (label, attempt, exc),
                    ) from exc
                if not store.increment_retries(self.job_id, self.owner,
                                               self.generation, self.now()):
                    raise store.LeaseLost(self.job_id)
                if exc.retry_after is not None:
                    # Never sleep on a (near-)zero/negative Retry-After:
                    # scheduling overhead can leave no time budget at all.
                    delay = max(self.sleep_slice, exc.retry_after)
                else:
                    delay = min(self.backoff_cap,
                                self.backoff_base * (2 ** (attempt - 1)))
                self._interruptible_sleep(delay)
                if not store.renew_lease(self.job_id, self.owner,
                                         self.generation, self.now(),
                                         self.lease_ttl):
                    raise store.LeaseLost(self.job_id)

    # -- main loop ----------------------------------------------------------

    def run(self):
        job = self._job()
        if job is None:
            raise ValueError('unknown job')
        generation = store.acquire_lease(
            self.job_id, self.owner, self.now(), self.lease_ttl)
        if generation is None:
            # Another live worker owns the job; do not touch its progress.
            return self.job_id, 'lease_held'
        self.generation = generation
        # A cancel may have been requested while the job was queued, before
        # any worker held the lease; honor it before the first upstream call.
        if self._job().get('cancel_requested'):
            if store.mark_resumable(self.job_id, self.generation, 'cancelled',
                                    'cancelled by client; checkpoint retained',
                                    self.now()):
                return self.job_id, 'cancelled'
            return self.job_id, 'lease_lost'
        try:
            if job['mode'] == 'full':
                self._run_full()
            else:
                self._run_incremental()
        except JobCancelled:
            if store.mark_resumable(self.job_id, self.generation, 'cancelled',
                                    'cancelled by client; checkpoint retained',
                                    self.now()):
                return self.job_id, 'cancelled'
            return self.job_id, 'lease_lost'
        except store.LeaseLost:
            # Lease expired and was taken over. Never overwrite new progress.
            return self.job_id, 'lease_lost'
        except ProtocolError as exc:
            # Contract violation: stop without moving the checkpoint.
            if store.mark_resumable(self.job_id, self.generation, 'failed',
                                    'protocol error: %s' % exc, self.now()):
                return self.job_id, 'failed'
            return self.job_id, 'lease_lost'
        except RetryableSourceError as exc:
            if store.mark_resumable(self.job_id, self.generation, 'failed',
                                    'retry budget exhausted: %s' % exc,
                                    self.now()):
                return self.job_id, 'failed'
            return self.job_id, 'lease_lost'
        except Exception as exc:  # defensive: keep a resumable checkpoint
            if store.mark_resumable(self.job_id, self.generation, 'failed',
                                    repr(exc), self.now()):
                return self.job_id, 'failed'
            return self.job_id, 'lease_lost'
        else:
            store.mark_resumable(self.job_id, self.generation, 'completed',
                                 None, self.now())
            return self.job_id, 'completed'

    def _run_full(self):
        job = self._job()
        with Source(job['source_url']) as source:
            if job['snapshot_id']:
                # Resuming a crashed full run: reuse the frozen view.
                snapshot_id = job['snapshot_id']
                change_seq = job['change_seq']
                # NULL cursor means the final page was already committed;
                # only the atomic swap remains.
                cursor = job['cursor']
            else:
                self._check_cancel()
                snapshot = self._with_retries(source.snapshot, 'snapshot')
                snapshot_id = snapshot['snapshot_id']
                change_seq = snapshot['change_seq']
                store.attach_snapshot(self.job_id, self.owner, self.generation,
                                      self.now(), self.lease_ttl,
                                      snapshot_id, change_seq)
                cursor = '0'
            while cursor is not None:
                self._check_cancel()
                page = self._with_retries(
                    lambda: source.page(snapshot_id, cursor),
                    'page cursor=%s' % cursor,
                )
                failpoint('after_read', self.job_id)
                next_cursor = page['next_cursor']
                # Staging + checkpoint commit atomically; the before_commit
                # crash hook fires inside the store transaction itself.
                store.arm_before_commit_crash()
                try:
                    store.commit_full_page(
                        self.job_id, self.owner, self.generation, self.now(),
                        self.lease_ttl, next_cursor, change_seq, page['items'],
                    )
                finally:
                    store.disarm_before_commit_crash()
                failpoint('after_commit', self.job_id)
                cursor = next_cursor
            failpoint('expire_lease', self.job_id, self.owner,
                      self.generation)
            # All pages durable: promote the frozen view atomically. Until
            # this transaction commits, existing visible orders stay intact.
            store.swap_snapshot(self.job_id, job['source_id'], snapshot_id,
                                change_seq, self.owner, self.generation,
                                self.now(), self.lease_ttl)

    def _run_incremental(self):
        job = self._job()
        with Source(job['source_url']) as source:
            # Prefer the source checkpoint; the job value is the resume point.
            checkpoint = store.checkpoint(job['source_id'])
            seq = job['change_seq']
            if checkpoint and checkpoint['change_seq'] > seq:
                seq = checkpoint['change_seq']
            while True:
                self._check_cancel()
                page = self._with_retries(
                    lambda: source.changes(seq), 'changes after=%s' % seq,
                )
                failpoint('after_read', self.job_id)
                next_seq = page['next_seq']
                store.arm_before_commit_crash()
                try:
                    store.commit_change_page(
                        self.job_id, job['source_id'], self.owner,
                        self.generation, self.now(), self.lease_ttl,
                        next_seq, page['items'],
                    )
                finally:
                    store.disarm_before_commit_crash()
                failpoint('after_commit', self.job_id)
                seq = next_seq
                if not page.get('has_more', False):
                    break


def run(job_id):
    """Module-level entry used by the API background task and the CLI."""
    return Worker(job_id).run()


if __name__ == '__main__':
    run(sys.argv[1])