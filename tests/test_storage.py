import pytest

@pytest.fixture
def dbstore(request, fake_buckets):
    from s3logparse.config import Config
    from s3logparse.storage import LogStorage
    conf_fn = fake_buckets['tmpdir'].join('config-test.conf')
    c = Config(str(conf_fn))
    c.section('storage_backends', 'mongo')['database'] = 's3_logparse_test'
    store = LogStorage(c)
    def remove_db():
        with store.backend:
            store.backend.client.drop_database('s3_logparse_test')
    request.addfinalizer(remove_db)
    assert len(store.bucket_sources)
    assert len(store.backend.collections) == 0
    fake_buckets['store'] = store
    return fake_buckets

def test_storage(dbstore):
    store = dbstore['store']
    fake_entries = dbstore['entries']
    store.store_entries()
    entries = {}
    with store.backend:
        for table_name, entry in store.backend.get_all_entries():
            if table_name not in entries:
                entries[table_name] = []
            entries[table_name].append(entry)
    with store.backend:
        for key, d in fake_entries.items():
            key = key.replace('target', 'source')
            assert len(d.values()) == len(entries[key])
            for fake_entry in d.values():
                table_name = fake_entry.fields['bucket_name']
                q = store.backend.search(
                    table_name,
                    filt=dict(
                        request_id=fake_entry.fields['request_id'],
                    ),
                )
                assert q.count() == 1
                db_entry = q[0]
                for k, v in fake_entry.fields.items():
                    assert db_entry[k] == v

def test_dbops(dbstore):
    store = dbstore['store']
    store.store_entries()
    fake_entries = dbstore['entries']
    coll_names = dbstore['source_bucket_names']
    field_names = ['user_agent', 'operation', 'key_name']
    fake_fields = {}
    for key, d in fake_entries.items():
        bucket_name = key.replace('target', 'source')
        if bucket_name not in fake_fields:
            fake_fields[bucket_name] = {}
        for e in d.values():
            for field in field_names:
                if field not in fake_fields[bucket_name]:
                    fake_fields[bucket_name][field] = set()
                val = e.fields[field]
                fake_fields[bucket_name][field].add(val)
    with store.backend:
        for coll_name in coll_names:
            coll = store.backend.get_collection(coll_name)
            for field in field_names:
                q = coll.unique_values(field)
                assert len(q) == len(fake_fields[coll_name][field])
