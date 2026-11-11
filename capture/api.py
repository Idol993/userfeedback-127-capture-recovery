import uuid
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from . import store, worker

app=FastAPI(title='Supplier Capture')
class JobRequest(BaseModel):
    source_id: str
    source_url: str
    mode: str='full'
    change_seq: int=0

@app.post('/jobs',status_code=202)
def create_job(body:JobRequest,background:BackgroundTasks):
    job_id=str(uuid.uuid4())
    with store.connect() as db:
        db.execute('INSERT INTO jobs(id,source_id,source_url,mode,state,change_seq) VALUES(?,?,?,?,?,?)',
            (job_id,body.source_id,body.source_url,body.mode,'queued',body.change_seq))
    background.add_task(worker.run,job_id)
    return {'id':job_id,'state':'queued'}

@app.get('/jobs/{job_id}')
def get_job(job_id:str):
    result=store.job(job_id)
    if result is None: raise HTTPException(404,'unknown job')
    return result

@app.get('/sources/{source_id}/orders')
def get_orders(source_id:str): return {'items':store.orders(source_id)}
