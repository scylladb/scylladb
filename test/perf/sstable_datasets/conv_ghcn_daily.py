#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""NOAA GHCN-Daily 2023 -> bench.ghcn_daily.

Usage: python3 -I conv_ghcn_daily.py <data_root>

The source is ordered by date, so the prefix covers all stations for the
first days of 2023.
"""
import csv
import gzip
import io
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

NAME = 'ghcn_daily'
URLS = ['https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/2023.csv.gz',
        'https://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/by_year/2023.csv.gz (mirror)']


def main(root):
    w = common.Writer(root, NAME, common.load_schema(NAME),
                      ['station_id', 'obs_date', 'element'], 1)
    last_date = None
    with gzip.open(os.path.join(root, 'raw', NAME, '2023.csv.gz'), 'rb') as g:
        r = csv.reader(io.TextIOWrapper(g, encoding='utf-8', newline=''))
        for rec in r:
            if len(rec) != 8:
                w.drop('malformed line')
                continue
            sid, date, elem, val, mflag, qflag, sflag, otime = rec
            v = int(val)
            if not common.INT_MIN <= v <= common.INT_MAX:
                raise SystemExit('value out of int range: %r' % rec)
            last_date = date
            w.add({'station_id': sid, 'obs_date': common.date_str(date), 'element': elem,
                   'value': v, 'mflag': common.opt_text(mflag), 'qflag': common.opt_text(qflag),
                   'sflag': common.opt_text(sflag), 'obs_time': common.opt_text(otime)})
            if w.full:
                break
    w.close(sources=URLS,
            licence='NOAA public data, no restrictions (U.S. Government work; cite GHCN-Daily, Menne et al.)',
            download_bytes=common.raw_bytes(root, NAME),
            description='Narrow numeric time series; partition = weather station, clustered by '
                        '(date, element); prefix of a date-ordered file, so partitions are small.',
            extra={'last_source_date': last_date})


if __name__ == '__main__':
    main(sys.argv[1])
