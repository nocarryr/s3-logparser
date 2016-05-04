from utils import DummyS3Key


def test_entries(fake_logs):
    from s3logparse.transport import LogFile
    from s3logparse.entry import LogEntry
    path = fake_logs['path']
    parsed = []
    fake_flat = []
    for fn, p in fake_logs['paths'].items():
        fake_entries = fake_logs['filenames'][fn]
        s3_key = DummyS3Key(p)
        log_file = LogFile(key=s3_key)
        for i, entry in enumerate(LogEntry.entries_from_logfile(log_file)):
            fake_entry = fake_entries[i]
            parsed.append(entry)
            fake_flat.append(fake_entry)
            d = entry._serialize()
            for key, fake_val in fake_entry.fields.items():
                assert entry._fields[key] == entry[key] == getattr(entry, key)
                val = d[key]
                if isinstance(val, float):
                    val = round(val)
                assert val == fake_val
    assert len(parsed) == len(fake_flat)
