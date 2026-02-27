#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2021 ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

#
# Called by configure.py to merge several compdbs into one, so your
# favourite compatible code indexer can present you with a relevant
# maximally-complete unified index.
#

# Usage: merge-compdb.py [input...]
#
# Each input must be a compdb (i.e., a `compile_commands.json`) file.
# Inputs are specified as file names, optionally with an output prefix,
# using the format: <path-to-compdb>[:<output-prefix>]
#
# The script combines the inputs by:
#   - Filtering out entries that are not source files (e.g., non-C/C++ files)
#   - Excluding entries not built under the specified output prefix (if provided)
#   - Writing the filtered and merged result to stdout
#
# `configure.py` calls this script with:
#   - The ninja-generated Scylla compdb, with an output prefix for the current build mode
#   - Mode-specific Seastar and Abseil compdbs, without output prefixes

import json
import sys
import os.path

inputs = sys.argv[1:]
indexable_exts = {'.c', '.C', '.cc', '.cxx'}

def read_json(fname: str):
    with open(fname) as f:
        return json.load(f)


output_entries = [e
           for f, _, prefix in (i.partition(':') for i in inputs)
           for e in read_json(f)
           if os.path.splitext(e['file'])[1] in indexable_exts
           and (not prefix or e['output'].startswith(prefix))
           ]
json.dump(output_entries, sys.stdout, indent=True)
