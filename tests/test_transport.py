
def test_buckets(fake_buckets):
    from s3logparse.transport import LogBucketSource
    sources = {}
    targets = {}
    for src in LogBucketSource.iter_all():
        sources[src.bucket_name] = src
    assert len(sources) == len(fake_buckets['target_bucket_names'])
    logfiles = []
    for src in sources.values():
        last_dt = None
        for name, logfile in src.iter_logfiles():
            logfiles.append(logfile)
            assert logfile.dt is not None
            if last_dt is None:
                last_dt = logfile.dt
            else:
                assert last_dt <= logfile.dt
                last_dt = logfile.dt
    assert len(logfiles) == len(fake_buckets['paths'])
