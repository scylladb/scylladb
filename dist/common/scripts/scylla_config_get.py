#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2016-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later

import argparse
import sys
import yaml


def get(config, key):
    s = open(config).read()
    cfg = yaml.safe_load(s)
    try:
        val = cfg[key]
    except KeyError:
        print("key '%s' not found" % key)
        sys.exit(1)
    if isinstance(val, list):
        for v in val:
            print("%s" % v)
    elif isinstance(val, dict):
        for k, v in list(val.items()):
            print("%s:%s" % (k, v))
    else:
        print(val)


def main():
    parser = argparse.ArgumentParser(description='scylla.yaml config reader/writer from shellscript.')
    parser.add_argument('-c', '--config', dest='config', action='store',
                        default='/etc/scylla/scylla.yaml',
                        help='path to scylla.yaml')
    parser.add_argument('-g', '--get', dest='get', action='store',
                        required=True, help='get parameter')
    args = parser.parse_args()
    get(args.config, args.get)


if __name__ == "__main__":
    main()
