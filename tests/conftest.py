"""Shared fixtures for the capture integration tests.

Everything runs locally: the supplier simulator (``fixtures.source_server``)
is started as a real uvicorn process so the worker under test talks to it
over real HTTP, and the capture API is exercised through FastAPI's
TestClient. A fresh SQLite database is created per test.
"""
import os
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[1]


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def _wait_for_port(port, timeout=15.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError('simulator on port %d did not start' % port)


@pytest.fixture
def source_server():
    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, '-m', 'uvicorn', 'fixtures.source_server:app',
         '--host', '127.0.0.1', '--port', str(port), '--log-level', 'warning'],
        cwd=str(REPO_ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_port(port)
        yield 'http://127.0.0.1:%d' % port
    finally:
        proc.terminate()
        proc.wait(timeout=10)


@pytest.fixture
def db_path(tmp_path, monkeypatch):
    path = tmp_path / 'capture.db'
    monkeypatch.setenv('CAPTURE_DB', str(path))
    return path


@pytest.fixture
def client():
    from capture.api import app
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def env(monkeypatch):
    """Fast, deterministic timing for retries and leases."""
    monkeypatch.setenv('CAPTURE_BACKOFF_BASE', '0.02')
    monkeypatch.setenv('CAPTURE_BACKOFF_CAP', '0.2')
    monkeypatch.setenv('CAPTURE_SLEEP_SLICE', '0.01')
    # The simulator injects a 429 and a 503 per page key; allow enough
    # attempts for the three-page full run to succeed under 'transient'.
    monkeypatch.setenv('CAPTURE_MAX_ATTEMPTS', '12')
    monkeypatch.setenv('CAPTURE_LEASE_TTL', '30')


@pytest.fixture
def no_auto_start(monkeypatch):
    """Disable API background scheduling so the test drives workers itself
    (required for concurrency tests: TestClient runs BackgroundTasks
    synchronously before the response is returned)."""
    monkeypatch.setenv('CAPTURE_AUTO_START', '0')


@pytest.fixture
def short_lease(monkeypatch):
    monkeypatch.setenv('CAPTURE_LEASE_TTL', '1')


def control(source_url, **settings):
    import json
    req = urllib.request.Request(
        source_url.rstrip('/') + '/control',
        data=json.dumps(settings).encode(),
        headers={'Content-Type': 'application/json'},
        method='POST',
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read())


def wait_for_state(client, job_id, terminal=('completed', 'failed',
                                             'cancelled', 'lease_lost'),
                   timeout=30.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = client.get('/jobs/%s' % job_id).json()
        if last['state'] in terminal:
            return last
        time.sleep(0.03)
    raise AssertionError('job %s did not finish; last=%r' % (job_id, last))


def create_job(client, source_url, source_id='supplier-a', mode='full',
               change_seq=0):
    resp = client.post('/jobs', json={
        'source_id': source_id, 'source_url': source_url,
        'mode': mode, 'change_seq': change_seq,
    })
    assert resp.status_code == 202, resp.text
    return resp.json()['id']


def worker_subprocess(job_id, db_path, failpoint=None, extra_env=None):
    """Run the standalone worker in a separate OS process.

    Subprocesses run the project's own code from the repo root; the database
    path is passed via the environment so the two processes share state.
    """
    env = dict(os.environ)
    env['CAPTURE_DB'] = str(db_path)
    if failpoint:
        env['CAPTURE_FAILPOINT'] = failpoint
    if extra_env:
        env.update(extra_env)
    return subprocess.Popen(
        [sys.executable, '-m', 'capture.worker', job_id],
        cwd=str(REPO_ROOT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )


def wait_for_exit(proc, timeout=20):
    try:
        out, err = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate()
        raise
    return proc.returncode, out.decode(errors='replace'), err.decode(errors='replace')


def wait_until(predicate, timeout=15.0, interval=0.03):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def force_release_lease(job_id):
    """Simulate lease expiry after a crashed process: the owner is gone and
    its lease can be cleared so the job is resumable immediately."""
    from capture import store
    with store.transaction() as db:
        db.execute(
            'UPDATE jobs SET lease_owner=NULL, lease_until=0 WHERE id=?',
            (job_id,),
        )


def queue_job(source_url, db_path, mode='full', change_seq=0,
              source_id='supplier-a'):
    """Insert a queued job without scheduling an in-process worker, so the
    test alone decides which process picks it up."""
    import uuid
    from capture import store
    job_id = str(uuid.uuid4())
    store.create_job(job_id, source_id, source_url, mode, change_seq,
                     time.time())
    return job_id
