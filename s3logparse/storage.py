import os

from s3logparse.transport import LogBucketSource
from s3logparse.entry import LogEntry
from s3logparse.backends.mongo_storage import MongoStorage

class LogStorage(object):
    def __init__(self):
        self.backend = MongoStorage()
        self.bucket_sources = {}
    def get_buckets(self):
        for src in LogBucketSource.iter_all():
            self.bucket_sources[src.bucket_name] = src
    def store_entries(self):
        if not len(self.bucket_sources):
            self.get_buckets()
        to_delete = set()
        for name, src in self.bucket_sources.items():
            count = 0
            existing = 0
            for log_fn, logfile in src.iter_logfiles():
                entries = set([e for e in LogEntry.entries_from_logfile(logfile)])
                for entry in entries.copy():
                    r = self.backend.search(name, **entry._serialize())
                    if r.count():
                        existing += 1
                        entries.discard(entry)
                _count = self.backend.add_entries(name, *[e._serialize() for e in entries])
                if _count == len(entries):
                    to_delete.add(logfile)
                count += _count
            print('skipped {} existing entries'.format(existing))
            print('added {} entries to {}'.format(count, name))
        print('deleting {} logfiles from s3'.format(len(to_delete)))
        for logfile in to_delete:
            logfile.delete()
