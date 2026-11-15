#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Wikimedia pageviews, 2024-01-15 hours 15-18 UTC -> bench.pageviews.

Usage: python3 -I conv_pageviews.py <data_root>

One hour holds ~8M (domain, title) pairs, so all hours are read and only a
fixed hash-selected subset of partitions is kept (crc32 of the raw key
bytes), which gives up to one row per loaded hour per partition.
Lines whose title is not valid UTF-8, or whose domain or title is empty,
are dropped and counted.
"""
import gzip
import os
import sys
import zlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

NAME = 'pageviews'
HOURS = [15, 16, 17, 18]
URL = 'https://dumps.wikimedia.org/other/pageviews/2024/2024-01/pageviews-20240115-%02d0000.gz'
KEEP_PER_MILLE = 115


def main(root):
    w = common.Writer(root, NAME, common.load_schema(NAME), ['domain_code', 'page_title', 'hour'], 2)
    for h in HOURS:
        hour = '2024-01-15T%02d:00:00.000Z' % h
        with gzip.open(os.path.join(root, 'raw', NAME, os.path.basename(URL % h)), 'rb') as g:
            for line in g:
                parts = line.rstrip(b'\n').split(b' ')
                if len(parts) != 4:
                    w.drop('malformed line')
                    continue
                dom, title, views = parts[0], parts[1], parts[2]
                if not dom or not title:
                    w.drop('empty domain_code or page_title')
                    continue
                try:
                    dom_s = dom.decode('utf-8')
                    title_s = title.decode('utf-8')
                except UnicodeDecodeError:
                    w.drop('invalid UTF-8 in key')
                    continue
                if zlib.crc32(dom + b' ' + title) % 1000 >= KEEP_PER_MILLE:
                    continue
                v = int(views)
                if v > common.INT_MAX:
                    raise SystemExit('views overflow int: %r' % line)
                w.add({'domain_code': dom_s, 'page_title': title_s, 'hour': hour, 'views': v})
    w.close(sources=[URL % h for h in HOURS],
            licence='Wikimedia pageviews: CC0 1.0',
            download_bytes=common.raw_bytes(root, NAME),
            description='Tiny rows (one int) behind long, high-cardinality text partition keys; '
                        '1-4 rows per (domain, title) partition, one per hour.',
            extra={'partition_subset': 'crc32(domain_code + " " + page_title) %% 1000 < %d' % KEEP_PER_MILLE,
                   'note': 'drop counts cover all source lines, before hash selection'})


if __name__ == '__main__':
    main(sys.argv[1])
