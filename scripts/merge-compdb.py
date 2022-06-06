#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2021 ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
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
import re
import sys

prefix = sys.argv[1]
inputs = sys.argv[2:]

def read_input(fname):
    with open(fname) as f:
        return json.load(f)

def is_indexable(e):
    return any(e['file'].endswith('.' + suffix) for suffix in ['c', 'C', 'cc', 'cxx'])

# We can only definitely say whether an entry is built under the right
# prefix when it has an "output" field, so those without are assumed
# to be OK.  This works for our usage, where only "ninja -t compdb"
# creates entries with an 'output' field _and_ needs to be filtered in
# the first place.
def is_relevant(e):
    return ('output' not in e) or (e['output'].startswith(prefix))

json.dump([e for fname in inputs
           for e in read_input(fname) if is_indexable(e) and is_relevant(e)],
          sys.stdout, indent=True)
