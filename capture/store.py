import os, sqlite3, json
from pathlib import Path

def connect():
    path = Path(os.environ.get('CAPTURE_DB', 'var/capture.db'))
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.executescript("""
    CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY, source_id TEXT, source_url TEXT,
      mode TEXT, state TEXT, snapshot_id TEXT, cursor TEXT, change_seq INTEGER DEFAULT 0,
      received INTEGER DEFAULT 0, applied INTEGER DEFAULT 0, retries INTEGER DEFAULT 0, error TEXT);
    CREATE TABLE IF NOT EXISTS orders(source_id TEXT, order_id TEXT, version INTEGER,
      deleted INTEGER, payload TEXT, PRIMARY KEY(source_id,order_id));
    CREATE TABLE IF NOT EXISTS effects(source_id TEXT, order_id TEXT, version INTEGER);
    """)
    return db

def job(job_id):
    with connect() as db:
        row = db.execute('SELECT * FROM jobs WHERE id=?',(job_id,)).fetchone()
        return dict(row) if row else None

def update(job_id, **values):
    with connect() as db:
        db.execute('UPDATE jobs SET '+','.join(k+'=?' for k in values)+' WHERE id=?',[*values.values(),job_id])

def apply(source_id, items):
    with connect() as db:
        for item in items:
            db.execute('INSERT OR REPLACE INTO orders VALUES(?,?,?,?,?)',
                (source_id,item['order_id'],item['version'],int(item.get('deleted',False)),json.dumps(item)))
            db.execute('INSERT INTO effects VALUES(?,?,?)',(source_id,item['order_id'],item['version']))

def orders(source_id):
    with connect() as db:
        return [json.loads(x['payload']) for x in db.execute(
            'SELECT payload FROM orders WHERE source_id=? AND deleted=0 ORDER BY order_id',(source_id,))]
