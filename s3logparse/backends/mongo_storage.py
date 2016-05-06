import threading

import pymongo
from pymongo import MongoClient

class ReentrantContext(object):
    def _init_locks(self):
        if hasattr(self, '_is_open'):
            return
        self._is_open = False
        self._connection_lock = threading.Lock()
        self._context_lock = threading.RLock()
        self._context_object = None
    def open(self):
        with self._connection_lock:
            if self._is_open:
                obj = self._context_object
            else:
                obj = self._context_object = self._do_open()
                self._is_open = True
        return obj
    def close(self):
        with self._connection_lock:
            if self._is_open:
                self._do_close()
                self._context_object = None
                self._is_open = False
    def _do_open(self):
        raise NotImplementedError('must be defined by subclass')
    def _do_close(self):
        raise NotImplementedError('must be defined by subclass')
    def acquire(self):
        self._init_locks()
        self._context_lock.acquire()
        return self.open()
    def release(self):
        self._context_lock.release()
        if not self._context_lock._is_owned():
            self.close()
    def __enter__(self):
        return self.acquire()
    def __exit__(self, *args):
        self.release()

class MongoStorage(ReentrantContext):
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
        self._collections = {}
    def _do_open(self):
        self.client = MongoClient(**self.config.client)
        db = self.client[self.config.database]
        return db
    def _do_close(self):
        self.client.close()
    @property
    def collections(self):
        self._sync_log_collections()
        return self._collections
    def _sync_log_collections(self):
        with self as db:
            names = db.collection_names()
            for name in db.collection_names():
                if name == 'system.indexes':
                    continue
                if name in self._collections:
                    continue
                self._build_log_collection(name)
    def _build_log_collection(self, name):
        coll = MongoLogCollection(backend=self, name=name)
        self._collections[name] = coll
        return coll
    def get_collection(self, name):
        coll = self.collections.get(name)
        if coll is not None:
            return coll
        return self._build_log_collection(name)
    def add_entry(self, table_name, entry):
        with self:
            coll = self.get_collection(table_name)
            r = coll.add_entry(self)
        return r
    def add_entries(self, table_name, *entries):
        with self:
            coll = self.get_collection(table_name)
            r = coll.add_entries(*entries)
        return r
    def get_all_entries(self, *table_names, **kwargs):
        with self as db:
            if not len(table_names):
                table_names = self.collections.keys()
            for table_name in table_names:
                coll = self.get_collection(table_name)
                for e in coll.get_all_entries(**kwargs):
                    yield table_name, e
    def get_fields(self, table_name):
        with self as db:
            coll = self.get_collection(table_name)
            fields = coll.get_fields()
        return fields
    def search(self, table_name, **kwargs):
        with self as db:
            coll = self.get_collection(table_name)
            return coll.search(**kwargs)

class MongoLogCollection(ReentrantContext):
    def __init__(self, **kwargs):
        self.backend = kwargs.get('backend')
        self.name = kwargs.get('name')
        self._collection = kwargs.get('collection')
    def _do_open(self):
        db = self.backend.acquire()
        coll = self._collection
        if coll is None:
            coll = self._collection = db[self.name]
        return coll
    def _do_close(self):
        self.backend.release()
    def add_entry(self, entry):
        with self as coll:
            if self.search(**entry).count():
                return False
            r = coll.insert_one(entry)
        return r
    def add_entries(self, *entries):
        with self as coll:
            count = 0
            for entry in entries:
                r = self.add_entry(entry)
                if r is not False:
                    count += 1
        return count
    def _prepare_sort(self, **kwargs):
        sort_field = kwargs.pop('sort_field', '+datetime')
        sort_dir = pymongo.ASCENDING
        if sort_field.startswith('+'):
            sort_dir = pymongo.ASCENDING
            sort_field = sort_field[1:]
        elif sort_field.startswith('-'):
            sort_dir = pymongo.DESCENDING
            sort_field = sort_field[1:]
        return kwargs, sort_field, sort_dir
    def get_all_entries(self, **kwargs):
        kwargs, sort_field, sort_dir = self._prepare_sort(**kwargs)
        with self as coll:
            for e in coll.find().sort(sort_field, sort_dir):
                yield e
    def get_fields(self):
        with self as coll:
            e = coll.find_one()
        return e.keys()
    def search(self, **kwargs):
        kwargs, sort_field, sort_dir = self._prepare_sort(**kwargs)
        with self as coll:
            return coll.find(kwargs).sort(sort_field, sort_dir)
