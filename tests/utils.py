import json
import py


class DummyS3Connection(object):
    _temp_path = py.path.local('/tmp')
    def __init__(self, **kwargs):
        pass
    def get_bucket(self, name):
        b = DummyS3Bucket(name=name, path=self._temp_path.join(name))
        return b
    def get_all_buckets(self):
        for p in self._temp_path.listdir():
            if not p.isdir():
                continue
            yield DummyS3Bucket(name=p.basename, path=p)
    def __iter__(self):
        return self.get_all_buckets()

class LoggingStatus(object):
    def __init__(self, **kwargs):
        self.target = kwargs.get('target')
        self.prefix = kwargs.get('prefix')
        if self.target is not None or self.prefix is not None:
            self.LoggingEnabled = ''

class DummyResult(object):
    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            setattr(self, key, val)

class DummyS3Bucket(object):
    def __init__(self, **kwargs):
        self.name = kwargs.get('name')
        self.path = kwargs.get('path')
        d = self._read_logging_status()
        self.logging_status = LoggingStatus(**d)
    def _read_logging_status(self):
        p = self.path.join('.logging_status.json')
        if not p.exists():
            return {}
        return json.loads(p.read())
    def get_logging_status(self):
        return self.logging_status
    def list(self, prefix=None):
        return self.get_all_keys(prefix)
    def get_all_keys(self, prefix=None):
        for p in self.path.visit():
            if p.isdir():
                continue
            c = p.common(self.path)
            keyname = str(p).lstrip(str(c)).lstrip('/')
            if prefix is not None and not keyname.startswith(prefix):
                continue
            yield DummyS3Key(p, bucket=self, name=keyname)
    def delete_keys(self, keys):
        result = DummyResult(deleted=[], errors=[])
        for key in keys:
            key.delete()
            result.deleted.append(DummyResult(key=key.name))
        return result

class DummyS3Key(object):
    def __init__(self, filename, bucket=None, name=None):
        if not isinstance(filename, py.path.local):
            filename = py.path.local(filename)
        self.filename = filename
        self.bucket = bucket
        self.name = name
    def get_contents_as_string(self):
        return self.filename.read()
    def delete(self):
        self.filename.remove()
