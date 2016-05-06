import threading

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

class BackendBase(ReentrantContext):
    def __init__(self, **kwargs):
        config = kwargs.get('config')
        section_name = getattr(self, 'config_section', self.__class__.__name__)
        self.config = config.root.section('storage_backends', section_name)
        config_defaults = getattr(self, 'config_defaults', {})
        for key, val in config_defaults.items():
            self.config.setdefault(key, val)
        self._collections = {}
    @property
    def collections(self):
        self._sync_log_collections()
        return self._collections
    def _sync_log_collections(self):
        raise NotImplementedError('must be defined by subclass')
    def get_collection(self, name):
        coll = self.collections.get(name)
        if coll is not None:
            return coll
        return self._build_log_collection(name)
    def _build_log_collection(self, name):
        raise NotImplementedError('must be defined by subclass')
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
    def search(self, table_name, filt=None, **kwargs):
        with self as db:
            coll = self.get_collection(table_name)
            return coll.search(filt, **kwargs)

class LogCollectionBase(ReentrantContext):
    def __init__(self, **kwargs):
        self.backend = kwargs.get('backend')
        self.name = kwargs.get('name')
    def add_entry(self, entry):
        raise NotImplementedError('must be defined by subclass')
    def add_entries(self, *entries):
        raise NotImplementedError('must be defined by subclass')
    def get_all_entries(self, filt=None, **kwargs):
        raise NotImplementedError('must be defined by subclass')
    def get_fields(self):
        raise NotImplementedError('must be defined by subclass')
    def search(self, filt=None, **kwargs):
        raise NotImplementedError('must be defined by subclass')
