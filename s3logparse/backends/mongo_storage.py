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
        self.client = MongoClient(**self.config.client)
        self.db = self.client[self.config.database]
    def add_entry(self, table_name, entry):
        if self.search(table_name, **entry).count():
            return False
        coll = self.db[table_name]
        r = coll.insert_one(entry)
        return r
    def add_entries(self, table_name, *entries):
        count = 0
        for entry in entries:
            r = self.add_entry(table_name, entry)
            if r is not False:
                count += 1
        return count
    def get_log_collections(self):
        names = self.db.collection_names()
        return [name for name in names if name != 'system.indexes']
    def get_all_entries(self, *table_names):
        if not len(table_names):
            table_names = self.get_log_collections()
        for table_name in table_names:
            coll = self.db[table_name]
            for entry in coll.find().sort('datetime', pymongo.ASCENDING):
                yield table_name, entry
    def search(self, table_name, **kwargs):
        coll = self.db[table_name]
        return coll.find(kwargs)
