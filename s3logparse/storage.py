import os
import datetime

import pytz

from s3logparse.transport import LogBucketSource
from s3logparse.entry import LogEntry
from s3logparse.backends import build_backend

class LogStorage(object):
    def __init__(self, config):
        self.config = config.log_storage
        self.config.setdefault('skip_search', False)
        self.config.setdefault('delete_logfiles', True)
        self.backend = build_backend(config)
        self.bucket_sources = {}
        conf_buckets = config.get('buckets', {}).get('log_sources')
        if conf_buckets is not None:
            for key, val in conf_buckets.items():
                b = LogBucketSource(bucket_name=key, config=val)
                self.bucket_sources[b.bucket_name] = b
        if not self.config.get('skip_search'):
            self.get_buckets()
    def get_buckets(self):
        skip_names = self.bucket_sources.keys()
        for src in LogBucketSource.iter_all(skip_names, config=self.config):
            self.bucket_sources[src.bucket_name] = src
    def remove_parsed_logfiles(self):
        if not self.config.delete_logfiles:
            return
        with self.backend:
            for name, src in self.bucket_sources.items():
                parsed_dts = self.backend.unique_values(name, 'datetime')
                if not len(parsed_dts):
                    continue
                last_dt = max(parsed_dts) - datetime.timedelta(days=1)
                max_dt = max(src.target.logfiles_by_dt.keys())
                for dt, logfiles in src.target.logfiles_by_dt.items():
                    if not len(logfiles):
                        continue
                    if dt.date() >= last_dt.date() or dt.date() >= max_dt.date():
                        continue
                    print('deleting logfiles for {}'.format(dt.date()))
                    src.target.delete_logfiles(*logfiles.values())
    def store_entries(self):
        if not len(self.bucket_sources):
            self.get_buckets()
        with self.backend:
            self.remove_parsed_logfiles()
            for name, src in self.bucket_sources.items():
                to_delete = set()
                count = 0
                existing = 0
                for log_fn, logfile in src.iter_logfiles():
                    delete_ok = True
                    for entry in LogEntry.entries_from_logfile(logfile, log_fn):
                        r = self.backend.search(name, filt={'request_id':entry.request_id})
                        if r.count():
                            existing += 1
                            continue
                        r = self.backend.add_entry(name, entry._serialize())
                        if r is False:
                            delete_ok = False
                        else:
                            count += 1
                    if delete_ok and self.config.delete_logfiles:
                        to_delete.add(logfile)
                print('skipped {} existing entries'.format(existing))
                print('added {} entries to {}'.format(count, name))
                if len(to_delete) >= 2:
                    to_delete = {lf.name:lf for lf in to_delete}
                    del to_delete[max(to_delete.keys())]
                    print('deleting {} logfiles'.format(len(to_delete)))
                    src.target.delete_logfiles(*to_delete.values())
