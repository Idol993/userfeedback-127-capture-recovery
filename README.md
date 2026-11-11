# Supplier Capture

Local order ingestion service. Python 3.12+, FastAPI, httpx, SQLite.

```text
python -m pip install -r requirements.txt
python -m uvicorn fixtures.source_server:app --host 127.0.0.1 --port 8127
python -m uvicorn capture.api:app --host 127.0.0.1 --port 8227
python -m pytest -q
```

POST `/jobs` with `{"source_id":"supplier-a","source_url":"http://127.0.0.1:8127","mode":"full"}`.
GET `/jobs/{id}` and `/sources/{source_id}/orders`.
An incremental request uses `mode: incremental` and `change_seq: 6`.
`CAPTURE_DB` selects the SQLite file. The standalone worker entry is `python -m capture.worker JOB_ID`.

The simulator freezes six orders in each POST `/snapshots` response and returns `snapshot_id` and baseline `change_seq`.
GET `/snapshots/{snapshot_id}?cursor=0` returns `items` and nullable `next_cursor`.
GET `/changes?after=6` returns change events (including duplicate and stale versions), `next_seq`, and `has_more`.
POST `/control` accepts `scenario` (`normal`, `transient`, `malformed`, `replay`), `delay` seconds, and `page_size`.
All test records are synthetic. The simulator is local only.

Current implementation commits pagination and order writes separately and has no cross-process ownership.
The three crash hooks are `CAPTURE_FAILPOINT=after_read|before_commit|after_commit`.
No production secrets or external services are required.
