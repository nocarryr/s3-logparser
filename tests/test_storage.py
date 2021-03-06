import os
import shlex
import pytest

def build_config(conf_fn, bucket_sources=None):
    from s3logparse.config import Config
    c = Config(str(conf_fn))
    c.section('storage_backends', 'mongo')['database'] = 's3_logparse_test'
    c.section('log_storage')
    if bucket_sources is not None:
        log_sources = c.section('buckets').section('log_sources')
        for source, d in bucket_sources.items():
            log_sources.section(source)._data.update(d)
    c.write()
    c = Config(str(conf_fn))
    return c

@pytest.fixture(params=['clean', 'preconfigured'])
def dbstore(request, fake_buckets):
    from s3logparse.storage import LogStorage
    conf_fn = fake_buckets['tmpdir'].join('config-test.conf')
    fake_buckets['conf_fn'] = conf_fn
    if request.param == 'preconfigured':
        fake_entries = fake_buckets['entries']
        d = {}
        for target in fake_entries.keys():
            source = target.replace('target', 'source')
            target = ''.join([target, os.sep])
            d[source] = dict(prefix=source, target=target)
    else:
        d = None
    c = build_config(conf_fn, bucket_sources=d)
    store = LogStorage(c)
    def remove_db():
        with store.backend:
            store.backend.client.drop_database('s3_logparse_test')
    request.addfinalizer(remove_db)
    assert len(store.bucket_sources)
    assert len(store.backend.collections) == 0
    fake_buckets['store'] = store
    return fake_buckets

@pytest.fixture
def main_argv_override(dbstore, monkeypatch):
    conf_fn = dbstore['conf_fn']
    argv = shlex.split('main.py -c {}'.format(conf_fn))
    monkeypatch.setattr('sys.argv', argv)
    return dbstore

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
                    if k != 'log_filename':
                        assert db_entry[k] == v

def check_entries(**kwargs):
    from s3logparse.entry import FIELD_NAMES
    all_fields = set(FIELD_NAMES)
    store = kwargs['store']
    fake_entries = kwargs['entries']
    coll_names = kwargs['source_bucket_names']
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
            dbfields = set(coll.get_fields())

            # remove any fields added by backend (_id or pk fields)
            # we only want to make sure the fields we want exist
            extra_fields = dbfields - all_fields
            dbfields -= extra_fields
            assert dbfields == all_fields

            for field in field_names:
                q = coll.unique_values(field)
                assert len(q) == len(fake_fields[coll_name][field])

def test_dbops(dbstore):
    store = dbstore['store']
    store.store_entries()
    check_entries(**dbstore)

def test_main(main_argv_override):
    from s3logparse.main import Config, LogStorage, main

    def get_logfiles(store, flat=True):
        if flat:
            logfiles = []
        else:
            logfiles = {}
        for name, src in store.bucket_sources.items():
            for log_fn, logfile in src.iter_logfiles():
                if flat:
                    logfiles.append(logfile)
                else:
                    if name not in logfiles:
                        logfiles[name] = {}
                    logfiles[name][log_fn] = logfile
        return logfiles

    store = main_argv_override['store']
    store.config['delete_logfiles'] = False
    assert store.config.has_changes
    store.config.write()

    logfiles = get_logfiles(store)
    logfiles_by_bucket = get_logfiles(store, flat=False)

    main()

    # make sure the files were not deleted
    for logfile in logfiles:
        assert logfile.key.filename.check()

    conf_fn = main_argv_override['conf_fn']
    c = Config(str(conf_fn))

    # build a new storage object to collect the entries stored by the main script
    store = LogStorage(c)
    main_argv_override['store'] = store
    check_entries(**main_argv_override)

    store.config['delete_logfiles'] = True
    assert store.config.has_changes
    store.config.write()

    # one more run with delete_logfiles set to true
    c = Config(str(conf_fn))
    store = LogStorage(c)
    assert len(logfiles) == len(get_logfiles(store))
    store.store_entries()

    main_argv_override['store'] = store
    check_entries(**main_argv_override)

    # now check that the correct files were deleted
    last_lfs = [max(lfs.keys()) for lfs in logfiles_by_bucket.values()]
    for logfile in logfiles:
        exists = logfile.key.filename.check()
        if logfile.name in last_lfs:
            assert exists
        else:
            assert not exists
