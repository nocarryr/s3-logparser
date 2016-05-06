
def test_storage(request, fake_buckets):
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
    store.store_entries()
    entries = {}
    with store.backend:
        for table_name, entry in store.backend.get_all_entries():
            if table_name not in entries:
                entries[table_name] = []
            entries[table_name].append(entry)
    with store.backend:
        for key, d in fake_buckets['entries'].items():
            key = key.replace('target', 'source')
            assert len(d.values()) == len(entries[key])
            for fake_entry in d.values():
                table_name = fake_entry.fields['bucket_name']
                q = store.backend.search(
                    table_name,
                    request_id=fake_entry.fields['request_id'],
                )
                assert q.count() == 1
                db_entry = q[0]
                for k, v in fake_entry.fields.items():
                    assert db_entry[k] == v
