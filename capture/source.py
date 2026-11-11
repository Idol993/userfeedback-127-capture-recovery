import httpx

class Source:
    def __init__(self,url):
        self.client=httpx.Client(base_url=url.rstrip('/'),timeout=10,trust_env=False)
    def snapshot(self):
        r=self.client.post('/snapshots'); r.raise_for_status(); return r.json()
    def page(self,snapshot_id,cursor):
        r=self.client.get('/snapshots/'+snapshot_id,params={'cursor':cursor or '0'})
        r.raise_for_status(); return r.json()
    def changes(self,seq):
        r=self.client.get('/changes',params={'after':seq})
        r.raise_for_status(); return r.json()
    def close(self): self.client.close()
