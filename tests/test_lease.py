"""Cross-process lease and contention tests.

Two real OS processes compete for the same job; coordination happens solely
through the SQLite lease columns, never through in-process locks.
"""
import subprocess
import time

from capture import store

from .conftest import (control, create_job, force_release_lease, queue_job,
                       wait_for_exit, wait_for_state, wait_until,
                       worker_subprocess)


def test_two_workers_same_job_only_one_commits(source_server, db_path, env):
    control(source_server, delay=0.05)
    job_id = queue_job(source_server, db_path)
    # Two processes start near-simultaneously and fight for the same lease.
    first = worker_subprocess(job_id, db_path)
    second = worker_subprocess(job_id, db_path)
    code1, _, err1 = wait_for_exit(first, timeout=40)
    code2, _, err2 = wait_for_exit(second, timeout=40)
    assert code1 == 0, err1
    assert code2 == 0, err2

    job = store.job(job_id)
    assert job['state'] == 'completed'
    assert len(store.orders('supplier-a')) == 6
    # No duplicated effects even though two workers read the source.
    assert len(store.effects('supplier-a')) == 6
    assert job['applied'] == 6


def test_expired_lease_takeover_completes_the_job(client, source_server,
                                                  db_path, env):
    # The first worker loses its lease right before the final swap and stays
    # alive long enough for a second worker to take over. The fencing
    # generation must stop the returning stale worker from touching progress.
    control(source_server, delay=0.05)
    job_id = queue_job(source_server, db_path)
    loser = worker_subprocess(
        job_id, db_path, failpoint='expire_lease',
        extra_env={'CAPTURE_LEASE_TTL': '1'})
    # Wait until the failpoint has released a genuinely expired lease.
    def lease_expired():
        job = store.job(job_id)
        return bool(job and job['lease_owner'] is None
                    and 0 < job['lease_until'] <= time.time())
    assert wait_until(lease_expired)
    gen_before = store.job(job_id)['lease_generation']

    # A fresh worker takes over the expired lease and finishes.
    takeover = worker_subprocess(job_id, db_path,
                                 extra_env={'CAPTURE_LEASE_TTL': '30'})
    code, out, err = wait_for_exit(takeover, timeout=40)
    assert code == 0, err or out
    code_l, _, err_l = wait_for_exit(loser, timeout=40)
    assert code_l == 0, err_l

    job = store.job(job_id)
    assert job['state'] == 'completed'
    # The takeover advanced the fencing generation exactly once.
    assert job['lease_generation'] == gen_before + 1
    assert len(store.orders('supplier-a')) == 6
    assert len(store.effects('supplier-a')) == 6
    # The stale worker's late LeaseLost path must not have re-marked the job.
    assert store.job(job_id)['state'] == 'completed'


def test_stale_worker_cannot_commit_after_takeover(db_path, source_server, env):
    """Proof that the commit-time lease guard rolls back a late writer even
    if it holds stale in-memory state."""
    job_id = 'job-guard'
    store.create_job(job_id, 'supplier-guard', source_server, 'full', 0,
                     time.time())
    gen_a = store.acquire_lease(job_id, 'owner-A', time.time(), 30)
    assert gen_a == 1
    # Owner A stages a page while still holding the lease.
    store.commit_full_page(job_id, 'owner-A', gen_a, time.time(), 30, '2', 6,
                           [{'order_id': 'o1', 'version': 1}])
    # Lease expires and owner B takes over (generation is fenced upward).
    force_release_lease(job_id)
    gen_b = store.acquire_lease(job_id, 'owner-B', time.time(), 30)
    assert gen_b == 2

    # A's late commit attempt with the stale generation must fail and roll
    # back atomically.
    try:
        store.commit_full_page(job_id, 'owner-A', gen_a, time.time(), 30, '4',
                               6, [{'order_id': 'o2', 'version': 1}])
    except store.LeaseLost:
        pass
    else:  # pragma: no cover
        raise AssertionError('stale owner was allowed to commit')

    job = store.job(job_id)
    assert job['cursor'] == '2'  # A's earlier committed page, B's baseline
    rows = store.staging_rows(job_id)
    assert [r['order_id'] for r in rows] == ['o1']  # o2 was rolled back

    # Incremental path is fenced the same way.
    try:
        store.commit_change_page(job_id, 'supplier-guard', 'owner-A', gen_a,
                                 time.time(), 30, 99,
                                 [{'order_id': 'o3', 'version': 1}])
    except store.LeaseLost:
        pass
    else:  # pragma: no cover
        raise AssertionError('stale owner was allowed to commit changes')
    assert store.checkpoint('supplier-guard') is None
    assert store.order_rows('supplier-guard') == []

    # The current generation can still commit normally.
    store.commit_change_page(job_id, 'supplier-guard', 'owner-B', gen_b,
                             time.time(), 30, 7,
                             [{'order_id': 'o1', 'version': 1}])
    assert store.checkpoint('supplier-guard')['change_seq'] == 7


def test_second_active_job_for_same_source_is_rejected(client, source_server,
                                                       db_path, env,
                                                       no_auto_start):
    control(source_server, delay=0.3)
    first = create_job(client, source_server)
    # The job stays queued because auto-start is off; active state alone
    # must make the second trigger a 409.
    assert store.job(first)['state'] == 'queued'
    resp = client.post('/jobs', json={
        'source_id': 'supplier-a', 'source_url': source_server, 'mode': 'full'})
    assert resp.status_code == 409
    assert resp.json()['detail']['job_id'] == first

    # The queued job still runs cleanly once a worker picks it up.
    proc = worker_subprocess(first, db_path)
    code, _, err = wait_for_exit(proc, timeout=40)
    assert code == 0, err
    assert store.job(first)['state'] == 'completed'


def test_resume_then_cancel_via_http_during_retry_backoff(client,
                                                          source_server,
                                                          db_path, env,
                                                          no_auto_start):
    # Every request fails malformed; the worker parks in backoff sleeps,
    # where cancellation must interrupt it.
    control(source_server, scenario='malformed')
    job_id = create_job(client, source_server)
    proc = worker_subprocess(job_id, db_path)
    assert wait_until(lambda: (store.job(job_id) or {}).get('retries', 0) >= 1)
    assert client.post('/jobs/%s/cancel' % job_id).status_code == 202
    code, _, err = wait_for_exit(proc, timeout=40)
    assert code == 0, err
    job = store.job(job_id)
    assert job['state'] == 'cancelled'
    # Checkpoint retained, nothing applied; resume after the fixup completes.
    control(source_server, scenario='normal')
    assert client.post('/jobs/%s/resume' % job_id).status_code == 202
    proc2 = worker_subprocess(job_id, db_path)
    code, _, err = wait_for_exit(proc2, timeout=40)
    assert code == 0, err
    assert store.job(job_id)['state'] == 'completed'
    assert len(store.orders('supplier-a')) == 6


def test_resume_rejects_live_lease_but_accepts_expired_one(client,
                                                           source_server,
                                                           db_path, env,
                                                           no_auto_start):
    job_id = queue_job(source_server, db_path)
    proc = worker_subprocess(job_id, db_path)
    # While the lease is live, resume must be refused.
    assert wait_until(lambda: (store.job(job_id) or {}).get('lease_owner'))
    resp = client.post('/jobs/%s/resume' % job_id)
    assert resp.status_code == 409
    wait_for_exit(proc, timeout=40)
    wait_for_state(client, job_id)

    # After completion the same job id can be requeued and run again by a
    # fresh worker process (auto-start is disabled).
    resp = client.post('/jobs/%s/resume' % job_id)
    assert resp.status_code == 202
    assert store.job(job_id)['state'] == 'queued'
    proc2 = worker_subprocess(job_id, db_path)
    code, _, err = wait_for_exit(proc2, timeout=40)
    assert code == 0, err
    job = store.job(job_id)
    assert job['state'] == 'completed'
    assert len(store.orders('supplier-a')) == 6
