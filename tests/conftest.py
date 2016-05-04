import os

import pytest

@pytest.fixture
def fake_logs(tmpdir):
    import loggen
    p = tmpdir.mkdir('fakelogs')
    d = loggen.fake_entries_to_path(p)
    d['path'] = p
    return d
