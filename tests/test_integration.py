"""End-to-end integration tests.

These assert against the actual SQLite contents (orders, tombstones, effects,
checkpoints and job counters), not just HTTP status codes.
"""
import json
import sqlite3
import time

from capture import store

from .conftest import (control, create_job, force_release_lease, queue_job,
                       wait_for_exit, wait_for_state, wait_until,
                       worker_subprocess)


def open_raw(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def test_full_then_incremental_happy_path(client, source_server, db_path, env):
    job_id = create_job(client, source_server)
    job = wait_for_state(client, job_id)
    assert job['state'] == 'completed'
    assert job['received'] == 6 and job['applied'] == 6
    assert job['retries'] == 0 and job['error'] is None
    assert job['change_seq'] == 6

    rows = store.order_rows('supplier-a')
    assert [(r['order_id'], r['version'], r['deleted']) for r in rows] == [
        ('order-001', 1, 0), ('order-002', 1, 0), ('order-003', 1, 0),
        ('order-004', 1, 0), ('order-005', 1, 0), ('order-006', 1, 0),
    ]
    # One effects row per applied transition, no duplicates.
    assert len(store.effects('supplier-a')) == 6
    assert client.get('/sources/supplier-a/checkpoint').json()['change_seq'] == 6

    incr = create_job(client, source_server, mode='incremental', change_seq=6)
    job2 = wait_for_state(client, incr)
    assert job2['state'] == 'completed'
    # Six upstream events, but only three transitions beat the local version.
    assert job2['received'] == 6 and job2['applied'] == 3
    assert job2['change_seq'] == 12

    visible = client.get('/sources/supplier-a/orders').json()['items']
    assert [o['order_id'] for o in visible] == [
        'order-001', 'order-003', 'order-004', 'order-005', 'order-006']
    rows = {r['order_id']: r for r in store.order_rows('supplier-a')}
    assert rows['order-001']['version'] == 2
    assert rows['order-002'] == {'order_id': 'order-002', 'version': 2, 'deleted': 1}
    assert rows['order-003']['version'] == 3
    # Stale events must not resurrect or downgrade anything.
    assert client.get('/sources/supplier-a/checkpoint').json()['change_seq'] == 12


def test_transient_429_and_503_are_retried(client, source_server, db_path, env):
    control(source_server, scenario='transient')
    job_id = create_job(client, source_server)
    job = wait_for_state(client, job_id)
    assert job['state'] == 'completed'
    assert job['retries'] >= 2, job
    assert job['received'] == 6 and job['applied'] == 6
    assert len(store.orders('supplier-a')) == 6


def test_malformed_json_exhausts_retries_and_keeps_checkpoint(client,
                                                              source_server,
                                                              db_path, env,
                                                              monkeypatch):
    monkeypatch.setenv('CAPTURE_MAX_ATTEMPTS', '3')
    control(source_server, scenario='malformed')
    job_id = create_job(client, source_server)
    job = wait_for_state(client, job_id)
    assert job['state'] == 'failed'
    assert 'retry budget exhausted' in job['error']
    assert job['retries'] == 2  # three attempts, two retries
    # Nothing visible and no checkpoint advancement.
    assert store.orders('supplier-a') == []
    assert store.checkpoint('supplier-a') is None
    with open_raw(db_path) as conn:
        stage = conn.execute(
            'SELECT COUNT(*) AS n FROM snapshot_staging').fetchone()['n']
    assert stage == 0

    # Resume after the upstream recovers: same job continues from page zero.
    control(source_server, scenario='normal')
    resp = client.post('/jobs/%s/resume' % job_id)
    assert resp.status_code == 202
    job = wait_for_state(client, job_id)
    assert job['state'] == 'completed'
    assert job['received'] == 6 and len(store.orders('supplier-a')) == 6


def test_protocol_error_does_not_advance_checkpoint(client, source_server,
                                                    db_path, env):
    # Point a queued full job at an unknown snapshot id: the simulator
    # answers 410, which the client classifies as a non-retryable protocol
    # error. The checkpoint must remain exactly where it was.
    import uuid
    bad_id = str(uuid.uuid4())
    jid = queue_job(source_server, db_path, source_id='supplier-b')
    with store.transaction() as db:
        db.execute(
            "UPDATE jobs SET snapshot_id=?, cursor='0' WHERE id=?",
            (bad_id, jid))
    resp = client.post('/jobs/%s/resume' % jid)
    assert resp.status_code == 202
    job = wait_for_state(client, jid)
    assert job['state'] == 'failed'
    assert 'protocol error' in job['error']
    assert job['cursor'] == '0'
    assert store.checkpoint('supplier-b') is None
    assert store.orders('supplier-b') == []
    assert job['retries'] == 0  # protocol errors are never retried


def test_cancel_stops_new_requests_and_keeps_resumable_checkpoint(
        client, source_server, db_path, env, no_auto_start):
    control(source_server, delay=0.25)
    job_id = create_job(client, source_server)
    proc = worker_subprocess(job_id, db_path)
    # Wait until at least the first page is committed.
    assert wait_until(lambda: (store.job(job_id) or {}).get('received', 0) > 0)
    resp = client.post('/jobs/%s/cancel' % job_id)
    assert resp.status_code == 202
    code, _, err = wait_for_exit(proc, timeout=40)
    assert code == 0, err
    job = store.job(job_id)
    assert job['state'] == 'cancelled'
    assert job['cancel_requested'] == 1
    # Visible data untouched because the full swap never happened.
    assert store.orders('supplier-a') == []
    received_at_cancel = job['received']
    assert received_at_cancel >= 2

    # Resume from the retained checkpoint and complete the run.
    control(source_server, delay=0)
    assert client.post('/jobs/%s/resume' % job_id).status_code == 202
    proc2 = worker_subprocess(job_id, db_path)
    code, _, err = wait_for_exit(proc2, timeout=40)
    assert code == 0, err
    job = store.job(job_id)
    assert job['state'] == 'completed'
    # Replayed first pages are counted again as received; applied is still
    # exactly the six order transitions.
    assert job['applied'] == 6
    assert len(store.orders('supplier-a')) == 6


def test_crash_after_read_is_recovered_without_loss(client, source_server,
                                                    db_path, env):
    proc = worker_subprocess(
        _queued_job(source_server, db_path), db_path, failpoint='after_read')
    code, _, err = wait_for_exit(proc)
    assert code == 77, err

    job_id = _single_job_id(db_path)
    # First page was read but never committed: no staging rows, cursor at 0.
    with open_raw(db_path) as conn:
        assert conn.execute(
            'SELECT COUNT(*) AS n FROM snapshot_staging').fetchone()['n'] == 0
    assert store.job(job_id)['cursor'] == '0'

    _resume_in_new_process(job_id, db_path)
    job = store.job(job_id)
    assert job['state'] == 'completed'
    assert len(store.orders('supplier-a')) == 6
    assert len(store.effects('supplier-a')) == 6


def test_crash_before_commit_rolls_back_then_recovers(client, source_server,
                                                      db_path, env):
    proc = worker_subprocess(
        _queued_job(source_server, db_path), db_path, failpoint='before_commit')
    code, _, err = wait_for_exit(proc)
    assert code == 77, err
    job_id = _single_job_id(db_path)
    with open_raw(db_path) as conn:
        assert conn.execute(
            'SELECT COUNT(*) AS n FROM snapshot_staging').fetchone()['n'] == 0
    assert store.job(job_id)['cursor'] == '0'

    _resume_in_new_process(job_id, db_path)
    job = store.job(job_id)
    assert job['state'] == 'completed'
    assert len(store.orders('supplier-a')) == 6
    assert len(store.effects('supplier-a')) == 6
    assert job['received'] == 6  # no double counting after clean replay


def test_crash_after_commit_resumes_without_duplicate_side_effects(
        client, source_server, db_path, env):
    proc = worker_subprocess(
        _queued_job(source_server, db_path), db_path, failpoint='after_commit')
    code, _, err = wait_for_exit(proc)
    assert code == 77, err
    job_id = _single_job_id(db_path)
    # First page (two orders) was durably committed before the crash.
    job = store.job(job_id)
    assert job['cursor'] == '2'
    with open_raw(db_path) as conn:
        assert conn.execute(
            'SELECT COUNT(*) AS n FROM snapshot_staging').fetchone()['n'] == 2
    assert store.orders('supplier-a') == []  # swap not reached: still hidden

    _resume_in_new_process(job_id, db_path)
    job = store.job(job_id)
    assert job['state'] == 'completed'
    assert len(store.orders('supplier-a')) == 6
    # The replayed first page must not duplicate effects or inflate applied.
    assert len(store.effects('supplier-a')) == 6
    assert job['applied'] == 6


def test_incremental_crash_resume_skips_no_changes(client, source_server,
                                                   db_path, env):
    # Baseline full load first.
    jid = create_job(client, source_server)
    wait_for_state(client, jid)

    proc = worker_subprocess(
        _queued_job(source_server, db_path, mode='incremental', change_seq=6),
        db_path, failpoint='after_commit')
    code, _, err = wait_for_exit(proc)
    assert code == 77, err
    incr_id = [j for j in _all_jobs(db_path) if j['mode'] == 'incremental'][0]['id']
    # The single changes page committed before crashing: checkpoint advanced.
    assert store.job(incr_id)['change_seq'] == 12

    _resume_in_new_process(incr_id, db_path)
    assert store.job(incr_id)['state'] == 'completed'
    rows = {r['order_id']: r for r in store.order_rows('supplier-a')}
    assert rows['order-002']['deleted'] == 1
    assert rows['order-001']['version'] == 2
    # Effects recorded once only despite the crash boundary.
    effects = store.effects('supplier-a')
    assert effects.count(('order-001', 2)) == 1
    assert effects.count(('order-002', 2)) == 1


def test_replayed_and_out_of_order_versions_are_idempotent(client,
                                                           source_server,
                                                           db_path, env):
    jid = create_job(client, source_server)
    wait_for_state(client, jid)
    # The replay scenario inserts the previous page's last row into each
    # subsequent page.
    control(source_server, scenario='replay')
    replay_job = create_job(client, source_server, source_id='supplier-replay')
    job = wait_for_state(client, replay_job)
    assert job['state'] == 'completed'
    # 6 real rows plus 2 duplicated rows arriving on pages 2 and 3.
    assert job['received'] == 8
    assert job['applied'] == 6
    assert len(store.orders('supplier-replay')) == 6
    assert len(store.effects('supplier-replay')) == 6

    # Out-of-order incremental events: stale versions never win.
    control(source_server, scenario='normal')
    incr = create_job(client, source_server, source_id='supplier-replay',
                      mode='incremental', change_seq=6)
    job2 = wait_for_state(client, incr)
    assert job2['state'] == 'completed'
    rows = {r['order_id']: r for r in store.order_rows('supplier-replay')}
    assert rows['order-001']['version'] == 2
    assert rows['order-002']['deleted'] == 1  # v2 tombstone beats v1 replay


def test_deleted_orders_stay_deleted_after_new_full_snapshot(client,
                                                             source_server,
                                                             db_path, env):
    jid = create_job(client, source_server)
    wait_for_state(client, jid)
    incr = create_job(client, source_server, mode='incremental', change_seq=6)
    wait_for_state(client, incr)
    assert store.order_rows('supplier-a')[1]['deleted'] == 1

    # A second full snapshot still carries v1 of the deleted order. The
    # version-gated swap must not resurrect it.
    second = create_job(client, source_server)
    job = wait_for_state(client, second)
    assert job['state'] == 'completed'
    rows = {r['order_id']: r for r in store.order_rows('supplier-a')}
    assert rows['order-002']['deleted'] == 1
    assert rows['order-002']['version'] == 2
    visible = client.get('/sources/supplier-a/orders').json()['items']
    assert 'order-002' not in [o['order_id'] for o in visible]


def test_half_finished_snapshot_never_replaces_visible_data(client,
                                                            source_server,
                                                            db_path, env):
    first = create_job(client, source_server)
    wait_for_state(client, first)
    assert len(store.orders('supplier-a')) == 6

    # Second full run crashes mid-way; the old visible data must remain
    # queryable and untouched while staging holds the partial new view.
    second_id = _queued_job(source_server, db_path)
    proc = worker_subprocess(second_id, db_path, failpoint='after_commit')
    code, _, _ = wait_for_exit(proc)
    assert code == 77
    with open_raw(db_path) as conn:
        staged = conn.execute(
            'SELECT COUNT(*) AS n FROM snapshot_staging WHERE job_id=?',
            (second_id,)).fetchone()['n']
    assert staged == 2
    # Visible table still serves the complete first snapshot.
    assert len(store.orders('supplier-a')) == 6

    _resume_in_new_process(second_id, db_path)
    assert store.job(second_id)['state'] == 'completed'
    assert len(store.orders('supplier-a')) == 6
    # Staging area is cleaned up after the swap.
    with open_raw(db_path) as conn:
        assert conn.execute(
            'SELECT COUNT(*) AS n FROM snapshot_staging').fetchone()['n'] == 0


# --------------------------------------------------------------------------
# helpers using the raw database (the API process and subprocesses share it)
# --------------------------------------------------------------------------

def _queued_job(source_url, db_path, mode='full', change_seq=0,
                source_id='supplier-a'):
    return queue_job(source_url, db_path, mode, change_seq, source_id)


def _resume_in_new_process(job_id, db_path):
    # The previous process is gone; clear its stale lease (equivalent to
    # waiting for natural expiry), then run a fresh worker process.
    force_release_lease(job_id)
    assert store.requeue(job_id, time.time())
    proc = worker_subprocess(job_id, db_path)
    code, out, err = wait_for_exit(proc, timeout=40)
    assert code == 0, err or out


def _single_job_id(db_path):
    jobs = _all_jobs(db_path)
    assert len(jobs) == 1
    return jobs[0]['id']


def _all_jobs(db_path):
    with open_raw(db_path) as conn:
        return [dict(r) for r in conn.execute('SELECT * FROM jobs')]
