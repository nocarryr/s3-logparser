import os

from s3logparse.transport import LogBucketSource
from s3logparse.entry import LogEntry
from s3logparse.backends.mongo_storage import MongoStorage

class LogStorage(object):
    def __init__(self, config):
        self.config = config.root.section('log_storage')
        self.config.setdefault('skip_search', False)
        self.config.setdefault('delete_logfiles', True)
        self.backend = MongoStorage(config=self.config)
        self.bucket_sources = {}
        conf_buckets = self.config.get('bucket_sources')
        if conf_buckets is not None:
            for key, val in conf_buckets.items():
                bkwargs = val.copy()
                bkwargs.setdefault('config', config)
                b = LogBucketSource(**bkwargs)
                self.bucket_sources[b.bucket_name] = b
        if not self.config.get('skip_search'):
            self.get_buckets()
    def get_buckets(self):
        skip_names = self.bucket_sources.keys()
        for src in LogBucketSource.iter_all(skip_names, config=self.config):
            self.bucket_sources[src.bucket_name] = src
    def store_entries(self):
        if not len(self.bucket_sources):
            self.get_buckets()
        with self.backend:
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
        if not self.config.delete_logfiles:
            return
        print('deleting {} logfiles from s3'.format(len(to_delete)))
        for logfile in to_delete:
            logfile.delete()
