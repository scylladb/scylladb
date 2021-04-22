#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2021 ScyllaDB
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
#

#
# Called by configure.py to merge several compdbs into one, so your
# favourite compatible code indexer can present you with a relevant
# maximally-complete unified index.
#

# Usage: merge-compdb.py build/{debug,release,dev,sanitize} [input...]
#
# Glue inputs, which must be compdb ("compile_command.json") files,
# together, filtering out anything that is not indexable or not built
# under the given prefix, and write the result to stdout.
#
# An input is either a file name or "-" for stdin.
#
# configure.py calls this with "-" for the ninja-generated scylla
# compdb and mode-specific seastar & abseil compdbs.
#

import json
import logging
import re
import sys

prefix = sys.argv[1]
inputs = sys.argv[2:]

def read_input(fname):
    try:
        if fname == '-':
            f = sys.stdin
        else:
            f = open(fname)

        return json.load(f)
    except Exception as e:
        logging.error('failed to parse %s: %s', fname, e)
        return [];
    finally:
        if fname != '-':
            f.close()

def is_indexable(e):
    return any(e['file'].endswith('.' + suffix) for suffix in ['c', 'C', 'cc', 'cxx'])

# We can only definitely say whether an entry is built under the right
# prefix when it has an "output" field, so those without are assumed
# to be OK.  This works for our usage, where only the "-" input (which
# results from "ninja -t compdb") has 'output' entries _and_ needs to
# be filtered in the first place.
def is_relevant(e):
    return ('output' not in e) or (e['output'].startswith(prefix))

json.dump([e for fname in inputs
           for e in read_input(fname) if is_indexable(e) and is_relevant(e)],
          sys.stdout, indent=True)
