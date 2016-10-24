import os

import yaml
import pyaml

class ConfigError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)

class DuplicateKeyError(ConfigError):
    pass

class ConfigSection(object):
    def __init__(self, name, parent=None, initdata=None):
        self._initialized = False
        self._has_changes = False
        self.name = name
        self.parent = parent
        self.sections = {}
        self._data = {}
        if initdata is not None:
            self._deserialize(initdata)
        else:
            self._initialized = True
    @property
    def root(self):
        p = self.parent
        if p is None:
            return self
        return p.root
    @property
    def dotted_name(self):
        p = self.parent
        if p is None:
            return self.name
        return '.'.join([p.dotted_name, self.name])
    @property
    def has_changes(self):
        if self._has_changes:
            return True
        p = self.parent
        if p is None:
            return self._get_has_changes()
        return p.has_changes
    def _get_has_changes(self):
        if self._has_changes:
            return True
        for section in self.sections.values():
            if section._get_has_changes():
                return True
        return False
    def _reset_changes(self):
        self._has_changes = False
        for section in self.sections.values():
            section._reset_changes()
    def write(self):
        self.root.write()
    def __setitem__(self, key, item):
        item_changed = False
        if isinstance(item, ConfigSection):
            if key in self._data or key in self.sections:
                raise DuplicateKeyError('{} exists in this section'.format(key))
            self.sections[key] = item
            item_changed = True
        else:
            if key in self.sections:
                raise DuplicateKeyError('{} is already a config section'.format(key))
            if key not in self._data:
                item_changed = True
            elif self._data[key] != item:
                item_changed = True
            self._data[key] = item
        if item_changed and self._initialized:
            self._has_changes = True
    def __getitem__(self, key):
        if key in self.sections:
            return self.sections[key]
        return self._data[key]
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            pass
        raise AttributeError
    def __iter__(self):
        for key in self.sections.keys():
            yield key
        for key in self._data.keys():
            if key not in self.sections:
                yield key
    def keys(self):
        for key in self:
            yield key
    def items(self):
        for key in self:
            yield key, self[key]
    def values(self):
        for key in self:
            yield self[key]
    def copy(self):
        d = {}
        for key, val in self.items():
            if isinstance(val, (ConfigSection, dict)):
                val = val.copy()
            else:
                val = type(val)(val)
            d[key] = val
        return d
    def get(self, name, default=None):
        try:
            return self[name]
        except KeyError:
            return default
    def setdefault(self, key, value):
        if key in self.sections or key in self._data:
            return
        self[key] = value
    def section(self, *names):
        section = self
        for name in names:
            obj = section.sections.get(name)
            if obj is None:
                obj = section.create_section(name)
            section = obj
        return section
    def create_section(self, name, initdata=None):
        obj = ConfigSection(name, self, initdata)
        self[name] = obj
        return obj
    def _serialize(self):
        d = self._data.copy()
        for key, section in self.sections.items():
            d.update(section._serialize())
        key = 'Section--{}'.format(self.name)
        return {key:d}
    def _deserialize(self, data):
        for key, val in data.items():
            if key.startswith('Section--'):
                name = key.split('Section--')[1]
                self.create_section(name, val)
            else:
                self[key] = val
        self._initialized = True
    def __repr__(self):
        return 'Config: {}'.format(self)
    def __str__(self):
        return self.dotted_name

class Config(ConfigSection):
    def __init__(self, filename):
        filename = os.path.expanduser(filename)
        self.filename = filename
        initdata = self.read()
        super(Config, self).__init__('main', initdata=initdata)
    def _get_has_changes(self):
        if not os.path.exists(self.filename):
            return True
        return super(Config, self)._get_has_changes()
    def read(self):
        if not os.path.exists(self.filename):
            return None
        with open(self.filename, 'r') as f:
            s = f.read()
        return yaml.load(s)
    def _serialize(self):
        d = super(Config, self)._serialize()
        return d['Section--main']
    def write(self):
        d = self._serialize()
        s = pyaml.dump(d)
        p = os.path.dirname(self.filename)
        if not os.path.exists(p):
            os.makedirs(p)
        with open(self.filename, 'w') as f:
            f.write(s)
        self._reset_changes()
