
def test_config(tmpdir):
    from s3logparse.config import Config
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
    assert c.has_changes
    c.write()
    assert not c.has_changes
    c2 = Config(str(p))
    assert not c2.has_changes
    for name, data in d.items():
        l = name.split('.')[1:]
        section = c.section(*l)
        for key, val in data.items():
            assert section[key] == getattr(section, key) == section.get(key) == val
