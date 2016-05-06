import pymongo
from pymongo import MongoClient

from s3logparse.backends.base import BackendBase, LogCollectionBase

class MongoStorage(BackendBase):
    config_section = 'mongo'
    config_defaults = {
        'client':{
            'host':'localhost',
            'port':27017,
            'tz_aware':True,
        },
        'database':'s3_logparse',
    }
    def _do_open(self):
        self.client = MongoClient(**self.config.client)
        db = self.client[self.config.database]
        return db
    def _do_close(self):
        self.client.close()
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


class MongoLogCollection(LogCollectionBase):
    def __init__(self, **kwargs):
        super(MongoLogCollection, self).__init__(**kwargs)
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
            if self.search(entry).count():
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
    def get_all_entries(self, filt=None, **kwargs):
        kwargs, sort_field, sort_dir = self._prepare_sort(**kwargs)
        if filt is None:
            filt = {}
        kwargs.setdefault('filter', filt)
        with self as coll:
            for e in coll.find(**kwargs).sort(sort_field, sort_dir):
                yield e
    def get_fields(self):
        with self as coll:
            e = coll.find_one()
        return e.keys()
    def search(self, filt=None, **kwargs):
        kwargs, sort_field, sort_dir = self._prepare_sort(**kwargs)
        if filt is None:
            filt = {}
        kwargs.setdefault('filter', filt)
        with self as coll:
            return coll.find(**kwargs).sort(sort_field, sort_dir)
    def unique_values(self, field_name, filt=None, **kwargs):
        with self as coll:
            return self.search(filt, **kwargs).distinct(field_name)
