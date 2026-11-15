#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""NYC TLC High Volume FHV trips, January 2024 -> bench.fhvhv_trips.

Usage: python3 -I conv_fhvhv_trips.py <data_root>

Reads row groups in order until the size target is reached.  seq is the
0-based row number in the source file.  Source timestamps are naive NYC
local time; they are written unchanged with a 'Z' suffix.
"""
import os
import sys

import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

NAME = 'fhvhv_trips'
URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-01.parquet'

RENAME = {'PULocationID': 'pu_location_id', 'DOLocationID': 'do_location_id'}
TS = {'request_datetime', 'on_scene_datetime', 'pickup_datetime', 'dropoff_datetime'}


def main(root):
    w = common.Writer(root, NAME, common.load_schema(NAME),
                      ['pu_location_id', 'pickup_date', 'pickup_datetime', 'seq'], 2)
    f = pq.ParquetFile(os.path.join(root, 'raw', NAME, os.path.basename(URL)))
    seq = 0
    groups = 0
    for g in range(f.metadata.num_row_groups):
        if w.full:
            break
        groups += 1
        for rec in f.read_row_group(g).to_pylist():
            row = {}
            for k, v in rec.items():
                k = RENAME.get(k, k)
                if v is not None and k in TS:
                    v = common.ts_from_dt(v)
                row[k] = v
            pk = rec['pickup_datetime']
            row['pickup_date'] = common.date_str(pk) if pk is not None else None
            row['seq'] = seq
            seq += 1
            w.add(row)
            if w.full:
                break
    w.close(sources=[URL],
            licence='NYC TLC trip record data, NYC Open Data terms of use (public, no restrictions)',
            download_bytes=common.raw_bytes(root, NAME),
            description='Medium-width numeric/flag rows; partition = (pickup zone, pickup day), '
                        'clustered by pickup time; source timestamps are NYC local time written as Z.',
            extra={'row_groups_read': groups, 'source_rows_read': seq})


if __name__ == '__main__':
    main(sys.argv[1])
