
def test_buckets(fake_buckets):
    from s3logparse.transport import LogBucketSource
    sources = {}
    targets = {}
    for src in LogBucketSource.iter_all():
        sources[src.bucket_name] = src
    assert len(sources) == len(fake_buckets['target_bucket_names'])
    logfiles = []
    for src in sources.values():
        for logfile in src.iter_logfiles():
            logfiles.append(logfile)
    assert len(logfiles) == len(fake_buckets['paths'])
