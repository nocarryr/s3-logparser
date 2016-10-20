import boto
from boto.s3.connection import S3Connection

CALLING_FORMAT = boto.config.get('s3', 'calling_format', 'boto.s3.connection.OrdinaryCallingFormat')
def build_connection(**kwargs):
    kwargs.setdefault('calling_format', CALLING_FORMAT)
    return S3Connection(**kwargs)

def iter_logfiles(bucket_name, prefix=None):
    c = build_connection()
    b = c.get_bucket(bucket_name)
    for key in b.get_all_keys(prefix=prefix):
        yield key


class S3Object(object):
    @property
    def connection(self):
        c = getattr(self, '_connection', None)
        if c is None:
            c = self._connection = build_connection()
        return c

class Bucket(S3Object):
    def __init__(self, **kwargs):
        self.bucket_name = kwargs.get('bucket_name')
        self.bucket = kwargs.get('bucket')
        self.config = kwargs.get('config')
    @property
    def bucket(self):
        b = getattr(self, '_bucket', None)
        if b is None:
            b = self._bucket = self.connection.get_bucket(self.bucket_name)
        return b
    @bucket.setter
    def bucket(self, b):
        if getattr(self, '_bucket', None) == b:
            return
        self._bucket = b
        self.bucket_name = b.name
    def __repr__(self):
        return '{}: {}'.format(self.__class__, self)
    def __str__(self):
        return self.bucket_name

class LogBucketSource(Bucket):
    def __init__(self, **kwargs):
        super(LogBucketSource, self).__init__(**kwargs)
        self.logging_status = kwargs.get('logging_status')
        if self.logging_status is None:
            self.logging_status = self.bucket.get_logging_status()
        if self.config is not None and self.config.name != self.bucket_name:
            sections = ['buckets', 'log_sources', self.bucket_name]
            self.config = self.config.root.section(*sections)
            for key in ['target', 'prefix']:
                self.config.setdefault(key, getattr(self.logging_status, key))
    @classmethod
    def iter_all(cls, skip_names=None, **kwargs):
        c = build_connection()
        for b in c:
            if skip_names and b.name in skip_names:
                continue
            logging_status = b.get_logging_status()
            if not hasattr(logging_status, 'LoggingEnabled'):
                continue
            kwargs.update(dict(bucket=b, logging_status=logging_status))
            obj = cls(**kwargs)
            yield obj
    @property
    def target(self):
        t = getattr(self, '_target', None)
        if t is None:
            t = self._target = self.build_target_bucket()
        return t
    def build_target_bucket(self):
        name = self.logging_status.target
        prefix = self.logging_status.prefix
        return LogBucketTarget(bucket_name=name, key_prefix=prefix)
    def iter_logfiles(self):
        for name, logfile in self.target.logfiles.items():
            yield name, logfile

class LogBucketTarget(Bucket):
    def __init__(self, **kwargs):
        super(LogBucketTarget, self).__init__(**kwargs)
        self.key_prefix = kwargs.get('key_prefix')
        self.logfiles = {}
        self.sync_logfiles()
    def iter_logfiles(self):
        for key in self.bucket.get_all_keys(prefix=self.key_prefix):
            yield LogFile(key=key, bucket=self)
    def delete_logfiles(*args):
        for arg in args:
            if not isinstance(arg, LogFile):
                arg = self.logfiles[arg]
            arg.delete()
        self.sync_logfiles()
    def on_logfile_deleted(self, logfile):
        del self.logfiles[logfile.name]
    def sync_logfiles(self):
        lf_names = set()
        for lf in self.iter_logfiles():
            lf_names.add(lf.name)
            if lf.name in self.logfiles:
                continue
            self.logfiles[lf.name] = lf
        for removed in lf_names - set(self.logfiles.keys()):
            del self.logfiles[removed]

class LogFile(S3Object):
    def __init__(self, **kwargs):
        self.key = kwargs.get('key')
        self.name = self.key.name
        self.bucket = kwargs.get('bucket')
    @property
    def content(self):
        return self.key.get_contents_as_string()
    def delete(self):
        self.key.delete()
        self.key = None
        self.bucket.on_logfile_deleted(self)
    def __repr__(self):
        return 'LogFile: {}'.format(self)
    def __str__(self):
        return self.name
