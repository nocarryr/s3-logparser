
def test_storage(request, fake_buckets):
    from s3logparse.config import Config
    from s3logparse.storage import LogStorage
    conf_fn = fake_buckets['tmpdir'].join('config-test.conf')
    c = Config(str(conf_fn))
    c.section('storage_backends', 'mongo')['database'] = 's3_logparse_test'
    store = LogStorage(c)
    def remove_db():
        store.backend.client.drop_database('s3_logparse_test')
    request.addfinalizer(remove_db)
    assert len(store.bucket_sources)
    assert len(store.backend.get_log_collections()) == 0
    store.store_entries()
    entries = {}
    for table_name, entry in store.backend.get_all_entries():
        if table_name not in entries:
            entries[table_name] = []
        entries[table_name].append(entry)
    for key, d in fake_buckets['entries'].items():
        key = key.replace('target', 'source')
        assert len(d.values()) == len(entries[key])
