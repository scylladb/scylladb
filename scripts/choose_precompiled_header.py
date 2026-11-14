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

Each heavy precompiled header is built on top of another precompiled
header, so it only stores what its own heavy header adds; see
HEAVY_PRECOMPILED_HEADERS below.

Both configure.py and CMake use this module, so they make the same choices.
They make them when configuring the build, not on every build: a source
that stops including a heavy header keeps using its precompiled header
until the build is configured again, which may hide an include that is now
missing (a freshly-configured build, like CI's, will catch it). Tracking
every scanned file as an input of configuration would avoid this, but would
configure again after almost every edit.
When run as a script, it prints one line for each source given on the
command line, in the same order: the precompiled header that source should
use, or "-" if none. The output doesn't repeat the sources, so their names
may contain spaces. With --list, it prints the heavy precompiled headers,
each after the one it is built on top of. With --parent PCH, it prints the
precompiled header that the heavy precompiled header PCH is built on top of.
"""

import argparse
import os
import re

# Our heavy precompiled headers - the only configuration of them: configure.py
# and CMake build exactly these. Each entry is a triple:
#   1. the precompiled header: a header like stdafx_database.hh, which only
#      includes stdafx.hh and the heavy header. Sources never include it -
#      the build compiles it into a precompiled header, and gives that to the
#      sources that should use it, with -include-pch.
#   2. the heavy header of ours that this precompiled header adds.
#   3. the precompiled header it is built on top of: stdafx.hh, or an earlier
#      entry in this list whose heavy header the heavy header (2) includes. A
#      precompiled header built on top of another only stores what its heavy
#      header adds, which makes it much smaller and cheaper to build.
# So the entries form a tree, rooted at stdafx.hh:
#
#   stdafx.hh
#     stdafx_mutation.hh
#       stdafx_query_processor.hh
#       stdafx_database.hh
#         test/lib/stdafx_cql_test_env.hh
#
# A source can use only one precompiled header: the last entry in this list
# whose heavy header the source includes (directly or indirectly). Any other
# heavy header it includes that isn't already in that precompiled header is
# compiled normally, as part of the source. So a source that includes
# cql_test_env.hh gets stdafx_cql_test_env.hh, which contains everything in
# stdafx_database.hh too; and a source that includes both database.hh and
# query_processor.hh (but not cql_test_env.hh) gets stdafx_database.hh, the
# bigger one, because it comes later.
HEAVY_PRECOMPILED_HEADERS = [
    ('stdafx_mutation.hh', 'mutation/mutation.hh', 'stdafx.hh'),
    ('stdafx_query_processor.hh', 'cql3/query_processor.hh', 'stdafx_mutation.hh'),
    ('stdafx_database.hh', 'replica/database.hh', 'stdafx_mutation.hh'),
    ('test/lib/stdafx_cql_test_env.hh', 'test/lib/cql_test_env.hh', 'stdafx_database.hh'),
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
        heavy = {os.path.join(self._source_dir, hh) for _, hh, _ in HEAVY_PRECOMPILED_HEADERS}
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
        for pch, hh, _ in reversed(HEAVY_PRECOMPILED_HEADERS):
            if hh in found:
                return pch
        return None

    def ordered(self):
        """Return the heavy precompiled headers, each after the one it is
        built on top of (that's the order of HEAVY_PRECOMPILED_HEADERS)."""
        self._check()
        return [p for p, _, _ in HEAVY_PRECOMPILED_HEADERS]

    def parent(self, pch):
        """Return the precompiled header that the heavy precompiled header
        pch is built on top of."""
        self._check()
        return next(parent for p, _, parent in HEAVY_PRECOMPILED_HEADERS if p == pch)

    def _check(self):
        """Check that HEAVY_PRECOMPILED_HEADERS is a valid tree: each entry is
        built on top of stdafx.hh or an earlier entry, whose heavy header its
        own heavy header includes. Otherwise the sources using it would see
        headers they don't include, and depend on them."""
        heavy = {p: hh for p, hh, _ in HEAVY_PRECOMPILED_HEADERS}
        seen = {'stdafx.hh'}
        for pch, hh, parent in HEAVY_PRECOMPILED_HEADERS:
            if parent not in seen:
                raise ValueError(f'{pch} is built on top of {parent}, which must come earlier in HEAVY_PRECOMPILED_HEADERS')
            if parent != 'stdafx.hh' and heavy[parent] not in self._included_heavy_headers(os.path.join(self._source_dir, hh)):
                raise ValueError(f'{pch} is built on top of {parent}, but {hh} does not include {heavy[parent]}')
            seen.add(pch)

def main():
    parser = argparse.ArgumentParser(description=__doc__.strip().split('\n')[0])
    parser.add_argument('--source-dir', required=True, help='top of the source tree')
    parser.add_argument('--list', action='store_true', help='print the heavy precompiled headers, each after its parent')
    parser.add_argument('--parent', metavar='PCH', help='print the precompiled header that PCH is built on top of')
    parser.add_argument('sources', nargs='*')
    args = parser.parse_args()
    chooser = Chooser(args.source_dir)
    if args.list:
        print('\n'.join(chooser.ordered()))
        return
    if args.parent:
        print(chooser.parent(args.parent))
        return
    for src in args.sources:
        print(chooser.choose(src) or '-')


if __name__ == '__main__':
    main()
