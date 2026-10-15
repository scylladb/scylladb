#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Choose which of our heavy precompiled headers a source file should use.

Besides stdafx.hh, which has only headers of external libraries, we have
precompiled headers that also contain a heavy header of our own, whose
parsing and template instantiations would otherwise be repeated by every
source that includes it. A source uses such a precompiled header only if it
already includes that header (directly or indirectly), so the precompiled
header doesn't make any object depend on a header it didn't depend on
before: editing a header rebuilds the same objects as without it.

Both configure.py and CMake use this module, so they make the same choice.
When run as a script, it prints "<precompiled header> <source>" for every
source given on the command line that should use one.
"""

import argparse
import os
import re

# Pairs of (precompiled header, the heavy header it adds to stdafx.hh). A
# source that includes several heavy headers uses the first matching entry,
# so an entry must come before any entry whose heavy header it includes.
HEAVY_PRECOMPILED_HEADERS = [
    ('test/lib/stdafx_cql_test_env.hh', 'test/lib/cql_test_env.hh'),
    ('stdafx_database.hh', 'replica/database.hh'),
    ('stdafx_query_processor.hh', 'cql3/query_processor.hh'),
    ('stdafx_mutation.hh', 'mutation/mutation.hh'),
]

_quoted_include_re = re.compile(r'^\s*#\s*include\s*"([^"]+)"', re.M)
_boost_test_module_re = re.compile(r'^\s*#\s*define\s+BOOST_TEST_MODULE\b', re.M)


def _read(path):
    try:
        with open(path, encoding='utf-8', errors='replace') as f:
            return f.read()
    except OSError:
        return ''


class Chooser:
    def __init__(self, source_dir):
        self._source_dir = os.path.abspath(source_dir)
        self._includes = {}

    def _quoted_includes(self, path):
        """Return the existing files that path includes with #include "...",
        resolved like the compiler does: first relative to path's directory,
        then relative to the top of the source tree. Generated headers, which
        don't exist yet when configuring, are skipped."""
        if path not in self._includes:
            result = []
            for inc in _quoted_include_re.findall(_read(path)):
                for base in (os.path.dirname(path), self._source_dir):
                    p = os.path.normpath(os.path.join(base, inc))
                    if os.path.isfile(p):
                        result.append(p)
                        break
            self._includes[path] = result
        return self._includes[path]

    def _included_heavy_headers(self, src):
        """Return which heavy headers src includes, directly or indirectly.
        #if conditions are ignored, so this may find an include that the
        compiler would skip."""
        heavy = {os.path.join(self._source_dir, hh) for _, hh in HEAVY_PRECOMPILED_HEADERS}
        seen, todo, found = set(), [src], set()
        while todo:
            p = todo.pop()
            if p in seen:
                continue
            seen.add(p)
            if p in heavy:
                found.add(os.path.relpath(p, self._source_dir))
            todo.extend(self._quoted_includes(p))
        return found

    def choose(self, src):
        """Return the heavy precompiled header (relative to the source tree)
        that src should use, or None."""
        src = os.path.normpath(os.path.join(self._source_dir, src))
        found = self._included_heavy_headers(src)
        # stdafx.hh includes boost/test/unit_test.hpp, so a definition of
        # BOOST_TEST_MODULE in the source would come too late.
        if not found or _boost_test_module_re.search(_read(src)):
            return None
        for pch, hh in HEAVY_PRECOMPILED_HEADERS:
            if hh in found:
                return pch
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip().split('\n')[0])
    parser.add_argument('--source-dir', required=True, help='top of the source tree')
    parser.add_argument('sources', nargs='*')
    args = parser.parse_args()
    chooser = Chooser(args.source_dir)
    for src in args.sources:
        pch = chooser.choose(src)
        if pch:
            print(pch, src)


if __name__ == '__main__':
    main()
