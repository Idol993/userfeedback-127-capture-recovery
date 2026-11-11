from capture import store
def test_save_and_read(tmp_path,monkeypatch):
    monkeypatch.setenv('CAPTURE_DB',str(tmp_path/'data.db'))
    store.apply('supplier-a',[{'order_id':'o1','version':1,'amount':10}])
    assert store.orders('supplier-a')[0]['amount']==10
