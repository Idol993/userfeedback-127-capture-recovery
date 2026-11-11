import copy, time, uuid
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, Response

app=FastAPI(title='Local supplier simulator')
ITEMS=[{'order_id':f'order-{i:03}','version':1,'amount':i*100,'deleted':False} for i in range(1,7)]
CHANGES=[dict(ITEMS[0],version=2,amount=901,change_seq=7),
         dict(ITEMS[1],version=2,deleted=True,change_seq=8),
         dict(ITEMS[0],version=1,change_seq=9),
         dict(ITEMS[0],version=2,amount=901,change_seq=10),
         dict(ITEMS[1],version=1,change_seq=11),
         dict(ITEMS[2],version=3,amount=777,change_seq=12)]
SNAPSHOTS={}; CALLS={}; SETTINGS={'scenario':'normal','delay':0,'page_size':2}

@app.post('/control')
def control(body:dict):
    SETTINGS.update(body); CALLS.clear()
    return SETTINGS

@app.post('/snapshots')
def create_snapshot():
    ident=str(uuid.uuid4()); SNAPSHOTS[ident]=copy.deepcopy(ITEMS)
    return {'snapshot_id':ident,'change_seq':6}

def fault(key):
    CALLS[key]=CALLS.get(key,0)+1
    time.sleep(float(SETTINGS['delay']))
    if SETTINGS['scenario']=='transient' and CALLS[key]==1:
        return JSONResponse({'error':'rate limited'},status_code=429,headers={'Retry-After':'0.1'})
    if SETTINGS['scenario']=='transient' and CALLS[key]==2:
        return JSONResponse({'error':'unavailable'},status_code=503)
    if SETTINGS['scenario']=='malformed': return Response('{broken',media_type='application/json')

@app.get('/snapshots/{snapshot_id}')
def page(snapshot_id:str,cursor:str='0'):
    if snapshot_id not in SNAPSHOTS: raise HTTPException(410,'snapshot expired')
    problem=fault(snapshot_id+':'+cursor)
    if problem is not None: return problem
    start=int(cursor); size=int(SETTINGS['page_size']); rows=SNAPSHOTS[snapshot_id]
    items=copy.deepcopy(rows[start:start+size]); end=start+size
    if SETTINGS['scenario']=='replay' and start>0: items.insert(0,copy.deepcopy(rows[start-1]))
    return {'snapshot_id':snapshot_id,'items':items,'next_cursor':str(end) if end<len(rows) else None}

@app.get('/changes')
def changes(after:int=6):
    problem=fault('changes:'+str(after))
    if problem is not None: return problem
    items=[copy.deepcopy(x) for x in CHANGES if x['change_seq']>after]
    return {'items':items,'next_seq':max([after]+[x['change_seq'] for x in items]),'has_more':False}
