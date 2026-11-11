import os, sys
from . import store
from .source import Source

def failpoint(name):
    if os.environ.get('CAPTURE_FAILPOINT')==name: os._exit(77)

def run(job_id):
    job=store.job(job_id)
    if not job: raise ValueError('unknown job')
    source=Source(job['source_url'])
    store.update(job_id,state='running',error=None)
    try:
        if job['mode']=='full':
            snapshot=source.snapshot()
            store.update(job_id,snapshot_id=snapshot['snapshot_id'],cursor='0')
            with store.connect() as db:
                db.execute('DELETE FROM orders WHERE source_id=?',(job['source_id'],))
            cursor='0'
            while cursor is not None:
                page=source.page(snapshot['snapshot_id'],cursor)
                failpoint('after_read')
                cursor=page['next_cursor']
                store.update(job_id,cursor=cursor,change_seq=snapshot['change_seq'])
                failpoint('before_commit')
                store.apply(job['source_id'],page['items'])
                failpoint('after_commit')
                current=store.job(job_id)
                store.update(job_id,received=current['received']+len(page['items']),applied=current['applied']+len(page['items']))
        else:
            page=source.changes(job['change_seq'])
            store.update(job_id,change_seq=page['next_seq'])
            store.apply(job['source_id'],page['items'])
        store.update(job_id,state='completed')
    except Exception as exc:
        store.update(job_id,state='failed',error=str(exc))
        raise
    finally: source.close()

if __name__=='__main__': run(sys.argv[1])
