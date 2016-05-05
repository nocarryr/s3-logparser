import threading

import pymongo
from pymongo import MongoClient

class MongoStorage(object):
    config_defaults = {
        'client':{
            'host':'localhost',
            'port':27017,
            'tz_aware':True,
        },
        'database':'s3_logparse',
    }
    def __init__(self, **kwargs):
        config = kwargs.get('config')
        self.config = config.root.section('storage_backends', 'mongo')
        for key, val in self.config_defaults.items():
            self.config.setdefault(key, val)
        self._is_open = False
        self._connection_lock = threading.Lock()
        self._context_lock = threading.RLock()
    def open(self):
        with self._connection_lock:
            if self._is_open:
                db = self._db
            else:
                self.client = MongoClient(**self.config.client)
                db = self._db = self.client[self.config.database]
                self._is_open = True
        return db
    def close(self):
        with self._connection_lock:
            if self._is_open:
                self._db = None
                self.client.close()
                self._is_open = False
    def __enter__(self):
        self._context_lock.acquire()
        return self.open()
    def __exit__(self, *args):
        self._context_lock.release()
        if not self._context_lock._is_owned():
            self.close()
    def add_entry(self, table_name, entry):
        with self as db:
            if self.search(table_name, **entry).count():
                return False
            coll = db[table_name]
            r = coll.insert_one(entry)
        return r
    def add_entries(self, table_name, *entries):
        with self as db:
            count = 0
            for entry in entries:
                r = self.add_entry(table_name, entry)
                if r is not False:
                    count += 1
        return count
    def get_log_collections(self):
        with self as db:
            names = db.collection_names()
        return [name for name in names if name != 'system.indexes']
    def get_all_entries(self, *table_names, **kwargs):
        sort_field = kwargs.pop('sort_field', '+datetime')
        sort_dir = pymongo.ASCENDING
        if sort_field.startswith('+'):
            sort_dir = pymongo.ASCENDING
            sort_field = sort_field[1:]
        elif sort_field.startswith('-'):
            sort_dir = pymongo.DESCENDING
            sort_field = sort_field[1:]
        with self as db:
            if not len(table_names):
                table_names = self.get_log_collections()
            for table_name in table_names:
                coll = db[table_name]
                for entry in coll.find(**kwargs).sort(sort_field, sort_dir):
                    yield table_name, entry
    def get_fields(self, table_name):
        with self as db:
            e = db[table_name].find_one()
        return e.keys()
    def search(self, table_name, **kwargs):
        with self as db:
            coll = db[table_name]
        return coll.find(kwargs)
