from s3logparse.backends.base import BackendBase, LogCollectionBase
from s3logparse.backends.mongo_storage import MongoStorage

BACKENDS = {
    'mongo':MongoStorage,
}

def build_backend(config):
    c = config.section('storage_backends')
    c.setdefault('backend_type', 'mongo')
    backend_cls = BACKENDS.get(c.get('backend_type'))
    return backend_cls(config=config)
