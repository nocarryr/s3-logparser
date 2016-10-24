import pytest

def test_config(tmpdir):
    from s3logparse.config import Config, DuplicateKeyError
    p = tmpdir.join('config-test.conf')
    c = Config(str(p))
    assert c._initialized
    assert c.has_changes
    c.write()
    assert not c.has_changes
    assert c._initialized
    d = {}
    section = c
    assert section._initialized
    for x in range(5):
        d[section.dotted_name] = {}
        for y in range(5):
            section[chr(y+65)] = y
            d[section.dotted_name][chr(y+65)] = y
        assert section.has_changes
        name = 'section_{}'.format(x)
        section = section.section(name)
        assert str(section) == section.dotted_name
    assert c.has_changes
    c.write()
    assert not c.has_changes
    c2 = Config(str(p))
    assert not c2.has_changes
    c3 = c2.copy()
    for name, data in d.items():
        l = name.split('.')[1:]
        section = c.section(*l)
        section2 = c2.section(*l)
        section3 = c3.section(*l)

        assert section is not section2
        assert section2 is not section3

        assert str(section) == str(section2) == str(section3)

        for key, val in data.items():
            assert section[key] == getattr(section, key) == section.get(key) == val
            assert section2[key] == getattr(section2, key) == section2.get(key) == val
            assert section3[key] == getattr(section3, key) == section3.get(key) == val

            assert section[key] == section2[key] == section3[key]

        if section.parent is not None:
            sec_name = l[-1]
            msg = '{} exists in this section'.format(sec_name)
            with pytest.raises(DuplicateKeyError, message=msg):
                section.parent[sec_name] = 'foo'

            key = list(data.keys())[0]
            msg = '{} exists in this section'.format(key)
            with pytest.raises(DuplicateKeyError, message=msg):
                section.parent[key] = section2
