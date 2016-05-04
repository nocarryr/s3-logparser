import datetime

import pytz

UTC = pytz.utc

FIELD_NAMES = [
    'owner_id',
    'bucket_name',
    'datetime',
    'client_ip',
    'client_id',
    'request_id',
    'operation',
    'key_name',
    'request_uri',
    'status_code',
    'error_code',
    'tx_bytes',
    'object_size',
    'total_time',
    'turnaround_time',
    'referrer',
    'user_agent',
    'version_id',
]

DT_FMT = '%d/%b/%Y:%H:%M:%S +0000'

class LogEntry(object):
    def __init__(self, line=None, **kwargs):
        self._fields = {}
        if line is not None:
            for field_name, field in self.parse_line(line):
                kwargs.setdefault(field_name, field)
        for field_name in FIELD_NAMES:
            self[field_name] = kwargs.get(field_name)
    @classmethod
    def entries_from_logfile(cls, logfile):
        for line in logfile.content.splitlines():
            line = line.strip('\n')
            yield cls(line)
    def parse_line(self, line):
        def iter_fields(_line):
            found = False
            for start_chr, end_chr in ['[]', '""']:
                if _line.startswith(start_chr):
                    found = True
                    i = _line.find(end_chr, 1)
                    field = _line[:i+1].lstrip(start_chr).rstrip(end_chr)
                    _line = _line[i+1:].lstrip(' ')
                    yield field, _line
            if not found:
                field = _line.split(' ')[0]
                _line = ' '.join(_line.split(' ')[1:])
                yield field, _line
                if not len(_line):
                    raise StopIteration
            for field, _line in iter_fields(_line):
                yield field, _line
        name_iter = iter(FIELD_NAMES)
        for field, _line in iter_fields(line):
            field_name = next(name_iter)
            yield field_name, field
    def __setitem__(self, key, item):
        item = self.format_field(key, item)
        self._fields[key] = item
    def __getitem__(self, key):
        return self._fields[key]
    def get(self, key, default=None):
        return self._fields.get(key, default)
    def __getattr__(self, name):
        val = None
        if name in FIELD_NAMES:
            try:
                return self[name]
            except KeyError:
                raise AttributeError
        raise AttributeError
    def format_field(self, field_name, value):
        if value == '-':
            value = None
        if value is None:
            return value
        if field_name == 'datetime':
            value = datetime.datetime.strptime(value, DT_FMT)
            value = UTC.localize(value)
        elif field_name in ['total_time', 'turnaround_time']:
            value = datetime.timedelta(milliseconds=float(value))
        elif field_name in ['tx_bytes', 'object_size']:
            value = int(value)
        return value
    def _serialize(self, dt_to_str=False):
        d = {}
        for key, val in self._fields.items():
            if isinstance(val, datetime.datetime) and dt_to_str:
                val = val.strftime(DT_FMT)
            elif isinstance(val, datetime.timedelta):
                val = int(round(val.total_seconds() * 1000.))
            d[key] = val
        return d
    def __repr__(self):
        return '{}: {}'.format(self.__class__, self)
    def __str__(self):
        return str(self._fields)
