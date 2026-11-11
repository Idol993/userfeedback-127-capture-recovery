"""HTTP interface of the capture service.

The original endpoints (``POST /jobs``, ``GET /jobs/{id}``,
``GET /sources/{id}/orders``) are preserved; cancel, resume and checkpoint
endpoints are added on top. Job payloads distinguish ``received`` (records
seen upstream, including duplicates) from ``applied`` (version transitions
that actually changed local data) and expose ``retries`` and the last error.
"""
import os
import time
import uuid

from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel

from . import store, worker

app = FastAPI(title='Supplier Capture')


class JobRequest(BaseModel):
    source_id: str
    source_url: str
    mode: str = 'full'
    change_seq: int = 0


class ResumeRequest(BaseModel):
    # Optional explicit resume position; defaults to the stored checkpoint.
    change_seq: int | None = None


def _start_job(job_id, background):
    # CAPTURE_AUTO_START=0 is used by tests that drive workers in separate
    # processes; the job stays queued and the HTTP contract is unchanged.
    if os.environ.get('CAPTURE_AUTO_START', '1') != '0':
        background.add_task(worker.run, job_id)


@app.post('/jobs', status_code=202)
def create_job(body: JobRequest, background: BackgroundTasks):
    if body.mode not in ('full', 'incremental'):
        raise HTTPException(400, "mode must be 'full' or 'incremental'")
    now = time.time()
    # A source may have only one actively leased job at a time. A second
    # concurrent trigger is rejected instead of racing the same data.
    active = store.active_job_for_source(body.source_id, now)
    if active is not None:
        raise HTTPException(
            409,
            {'error': 'source already has an active job',
             'job_id': active['id'], 'state': active['state']},
        )
    job_id = str(uuid.uuid4())
    store.create_job(job_id, body.source_id, body.source_url, body.mode,
                     body.change_seq, now)
    _start_job(job_id, background)
    return {'id': job_id, 'state': 'queued'}


@app.get('/jobs/{job_id}')
def get_job(job_id: str):
    result = store.job(job_id)
    if result is None:
        raise HTTPException(404, 'unknown job')
    result['lease_active'] = bool(
        result['lease_owner'] and result['lease_until'] > time.time(),
    )
    return result


@app.post('/jobs/{job_id}/cancel', status_code=202)
def cancel_job(job_id: str):
    if store.job(job_id) is None:
        raise HTTPException(404, 'unknown job')
    if not store.request_cancel(job_id, time.time()):
        raise HTTPException(409, 'job is not active')
    return {'id': job_id, 'state': 'cancelling'}


@app.post('/jobs/{job_id}/resume', status_code=202)
def resume_job(job_id: str, background: BackgroundTasks, body: ResumeRequest | None = None):
    job = store.job(job_id)
    if job is None:
        raise HTTPException(404, 'unknown job')
    now = time.time()
    if not store.requeue(job_id, now):
        raise HTTPException(409, {'error': 'job cannot be resumed',
                                  'state': job['state']})
    if body and body.change_seq is not None and job['mode'] == 'incremental':
        with store.transaction() as db:
            db.execute('UPDATE jobs SET change_seq=? WHERE id=?',
                       (body.change_seq, job_id))
    _start_job(job_id, background)
    return {'id': job_id, 'state': 'queued'}


@app.get('/sources/{source_id}/checkpoint')
def get_checkpoint(source_id: str):
    checkpoint = store.checkpoint(source_id)
    if checkpoint is None:
        return {'source_id': source_id, 'change_seq': 0, 'snapshot_id': None}
    return checkpoint


@app.get('/sources/{source_id}/orders')
def get_orders(source_id: str):
    return {'items': store.orders(source_id)}
