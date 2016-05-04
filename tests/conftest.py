import os
import json

import pytest

@pytest.fixture
def fake_logs(tmpdir, monkeypatch):
    from utils import DummyS3Connection
    monkeypatch.setattr('boto.s3.connection.S3Connection', DummyS3Connection)
    import loggen
    p = tmpdir.mkdir('fakelogs')
    DummyS3Connection._temp_path = p
    d = loggen.fake_entries_to_path(p)
    d['tmpdir'] = tmpdir
    d['path'] = p
    return d

@pytest.fixture
def fake_buckets(fake_logs):
    path = fake_logs['path']
    for target_bucket in fake_logs['target_bucket_names']:
        name = target_bucket.replace('target', 'source')
        p = path.join(name)
        conf_fn = p.join('.logging_status.json')
        conf_fn.ensure()
        conf_fn.write(json.dumps({'target':target_bucket}))
    return fake_logs
