import os
import sys
import datetime
import random
import string

import pytz

from s3logparse.entry import FIELD_NAMES

DT_FMT = '[%d/%b/%Y:%H:%M:%S +0000]'

BUCKET_NAMES = ['testbucket_{}'.format(i) for i in range(3)]

def random_listitem(l):
    return l[int(random.random() * len(l))]

def random_chars(length, chars=None):
    if chars is None:
        chars = ''.join([string.ascii_lowercase, string.digits])
    return ''.join([random_listitem(chars) for i in range(length)])

def random_digits(length):
    return random_chars(length, string.digits)

def random_letters(length):
    return random_chars(length, string.ascii_lowercase)

class FakeEntry(object):
    uri_fmt_str = '{verb} /{bucket_name}/{key_name} HTTP/1.1'
    quoted_fields = [
        'request_uri',
        'referrer',
        'user_agent',
    ]
    def __init__(self, **kwargs):
        dt = kwargs.get('datetime')
        dt = dt.replace()
        if dt.tzinfo is None:
            dt = pytz.utc.localize(dt)
        self._datetime = dt
        kwargs['datetime'] = dt.replace(microsecond=0)
        self.fields = self.build_field_defaults(**kwargs)
    def build_field_defaults(self, **kwargs):
        for key in FIELD_NAMES:
            if key in kwargs:
                continue
            val = self.get_value_for_field(key)
            kwargs[key] = val
        kwargs.setdefault('request_uri', self.build_uri(**kwargs))
        return kwargs
    def build_key_name(self):
        exts = ['html', 'css', 'js', 'xml', 'json', 'jpg', 'mp3']
        paths = [p for p in sys.path if len(p.split(os.sep)) >= 2]
        p = random_listitem(paths)
        l = p.split(os.sep)
        key = '/'.join([random_listitem(l) for i in range(2)])
        return '.'.join([key, random_listitem(exts)])
    def build_uri(self, **kwargs):
        d = kwargs.copy()
        d['verb'] = kwargs['operation'].split('.')[1]
        return self.uri_fmt_str.format(**d)
    def get_value_for_field(self, key):
        val = None
        if key == 'bucket_name':
            val = random_listitem(BUCKET_NAMES)
        elif key == 'key_name':
            val = self.build_key_name()
        elif key == 'request_id':
            val = random_chars(16).upper()
        elif key == 'status_code':
            val = '200'
        elif key == 'client_ip':
            val = '.'.join([random_digits(3) for i in range(4)])
        elif key == 'object_size':
            val = int(random.random() * 1024 * 1024)
        elif key == 'tx_bytes':
            val = int(random.random() * 1024)
        elif key in ['total_time', 'turnaround_time']:
            val = int(random.random() * 2000)
        return val
    def to_string(self):
        l = []
        for key in FIELD_NAMES:
            val = self.fields[key]
            if val is None:
                val = '-'
            elif isinstance(val, datetime.datetime):
                val = val.strftime(DT_FMT)
            else:
                val = str(val)
            if key in self.quoted_fields:
                val = '"{}"'.format(val)
            l.append(val)
        return ' '.join(l)
    def __repr__(self):
        return '{}: {}'.format(self.__class__, self)
    def __str__(self):
        return str(self.fields)

class FakeWebEntry(FakeEntry):
    operations = ['WEBSITE.GET.OBJECT', 'WEBSITE.HEAD.OBJECT']
    def get_value_for_field(self, key):
        if key == 'user_agent':
            return 'Useragent/2.0 (X11; CoplandOS Enterprise) NaviWebKit/01'
        elif key == 'operation':
            return random_listitem(self.operations)
        return super(FakeWebEntry, self).get_value_for_field(key)

class FakeAPIEntry(FakeEntry):
    operations = [
        ['GET', 'HEAD', 'PUT'],
        ['OBJECT', 'BUCKET', 'ACL', 'LOCATION', 'VERSIONING', 'BUCKETPOLICY',
         'LOGGING_STATUS', 'REQUEST_PAYMENT', 'REPLICATION', 'ACCELERATE',
         'LIFECYCLE', 'NOTIFICATION', 'WEBSITE', 'TAGGING', 'CORS']
    ]
    def build_field_defaults(self, **kwargs):
        kwargs = super(FakeAPIEntry, self).build_field_defaults(**kwargs)
        if not kwargs['operation'].endswith('OBJECT'):
            kwargs['key_name'] = None
            kwargs['object_size'] = None
        return kwargs
    def get_value_for_field(self, key):
        if key == 'owner_id':
            return random_chars(64)
        elif key == 'client_id':
            return 'arn:aws:iam::{}:/user/testuser'.format(
                random_chars(12, string.digits))
        elif key == 'operation':
            return self.build_operation()
        elif key == 'user_agent':
            return 'Boto/2.40.0 Python/2.7.10 Linux/4.2.0-35-generic'
        return super(FakeAPIEntry, self).get_value_for_field(key)
    def build_operation(self):
        op = ['REST']
        for l in self.operations:
            op.append(random_listitem(l))
        return '.'.join(op)

ENTRY_CLASSES = [FakeWebEntry, FakeAPIEntry]

def build_filename(dt):
    filename_fmt = '%Y-%m-%d-%H-%M-%S'
    fn = dt.strftime(filename_fmt)
    return '-'.join([fn, random_chars(16).upper()])

def build_fake_entries(end_dt=None, num_days=5, entries_per_day=100):
    if end_dt is None:
        end_dt = datetime.datetime.utcnow()
    end_dt = end_dt.replace(minute=0, hour=0)
    if end_dt.tzinfo is None:
        end_dt = pytz.utc.localize(end_dt)
    start_dt = end_dt - datetime.timedelta(days=num_days)
    td = datetime.timedelta(seconds=86400.0 / entries_per_day)
    one_minute = datetime.timedelta(minutes=1)
    dt = filename_dt = start_dt
    fn = build_filename(dt)
    entries = {k:{} for k in BUCKET_NAMES}
    filenames = {}
    while dt < end_dt:
        cls = random_listitem(ENTRY_CLASSES)
        entry = cls(datetime=dt)
        b = entry.fields['bucket_name']
        entry.filename = os.path.join(b, fn)
        entries[b][entry._datetime] = entry
        if entry.filename not in filenames:
            filenames[entry.filename] = []
        filenames[entry.filename].append(entry)
        dt += td
        if dt - filename_dt > one_minute:
            filename_dt += one_minute
            while filename_dt.day < dt.day:
                filename_dt -= datetime.timedelta(seconds=1)
            fn = build_filename(filename_dt)
            filenames[fn] = []
    return dict(entries=entries, filenames=filenames)

def fake_entries_to_path(path, end_dt=None, num_days=5, entries_per_day=100):
    d = build_fake_entries(end_dt, num_days, entries_per_day)
    for fn, entries in d['filenames'].items():
        lines = []
        for e in entries:
            lines.append(e.to_string())
        p = path.join(fn)
        p.ensure()
        with open(str(p), 'w') as f:
            f.write('\n'.join(lines))
    return d
