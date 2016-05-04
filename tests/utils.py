
class DummyS3Key(object):
    def __init__(self, filename):
        with open(filename, 'r') as f:
            self.content = f.read()
