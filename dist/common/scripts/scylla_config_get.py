#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2016-present ScyllaDB
#

#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

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
