#! /usr/bin/env python

import os
import argparse

DEFAULT_CONFPATH = '~/.s3-logparser.conf'

from s3logparse.storage import LogStorage
from s3logparse.config import Config

def main():
    p = argparse.ArgumentParser()
    p.add_argument('-c', '--config', dest='config_file', default=DEFAULT_CONFPATH)
    args = p.parse_args()
    config = Config(args.config_file)
    s = LogStorage(config=config)
    if config.has_changes:
        config.write()
    s.store_entries()
    if config.has_changes:
        config.write()

if __name__ == '__main__':
    main()
