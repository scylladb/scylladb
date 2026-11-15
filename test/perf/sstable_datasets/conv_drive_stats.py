#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Backblaze Drive Stats Q1 2024 -> bench.drive_stats.

Usage: python3 -I conv_drive_stats.py <data_root>

Reads the daily CSVs in date order from the downloaded zip.  To include
several days without cutting partitions, only vaults whose crc32(vault_id)
falls in a fixed 40% subset are kept; whole days are processed until the
size target is reached.
"""
import csv
import io
import os
import sys
import zipfile
import zlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

NAME = 'drive_stats'
URL = 'https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2024.zip'
VAULT_KEEP = (5, 2)  # keep vault if crc32 % 5 < 2


def main(root):
    schema = common.load_schema(NAME)
    cols = common.schema_columns(schema)
    w = common.Writer(root, NAME, schema,
                      ['date', 'cluster_id', 'vault_id', 'pod_id', 'pod_slot_num', 'serial_number'], 3)
    z = zipfile.ZipFile(os.path.join(root, 'raw', NAME, 'data_Q1_2024.zip'))
    members = sorted(i for i in z.namelist()
                     if i.startswith('data_Q1_2024/') and i.endswith('.csv'))
    days = []
    day_bytes = 0
    for m in members:
        # whole days only: stop before a day that would overshoot the target by >15%
        if w.bytes + day_bytes > w.target * 1.15 or w.full:
            break
        start = w.bytes
        days.append(os.path.basename(m)[:-4])
        r = csv.reader(io.TextIOWrapper(z.open(m), encoding='utf-8', newline=''))
        hdr = next(r)
        unknown = [c for c in hdr if c not in cols]
        if unknown:
            raise SystemExit('columns not in schema: %s' % unknown)
        for rec in r:
            d = dict(zip(hdr, rec))
            vault = d['vault_id']
            if zlib.crc32(vault.encode()) % VAULT_KEEP[0] >= VAULT_KEEP[1]:
                continue
            row = {'date': common.date_str(d['date']),
                   'serial_number': d['serial_number'],
                   'model': common.opt_text(d['model']),
                   'capacity_bytes': common.opt_int(d['capacity_bytes']),
                   'failure': d['failure'] == '1' if d['failure'] != '' else None,
                   'datacenter': common.opt_text(d['datacenter']),
                   'is_legacy_format': {'True': True, 'False': False}.get(d['is_legacy_format'])}
            if d['serial_number'] == '':
                w.drop('empty serial_number')
                continue
            for c in ('cluster_id', 'vault_id', 'pod_id', 'pod_slot_num'):
                if d[c] == '':
                    w.note('empty %s mapped to -1' % c)
                    row[c] = -1
                else:
                    row[c] = int(d[c])
            for c in hdr:
                if not c.startswith('smart_'):
                    continue
                v = common.opt_int(d[c])
                if v is None:
                    continue
                if cols[c] == 'int' and not common.INT_MIN <= v <= common.INT_MAX:
                    raise SystemExit('int overflow %s=%d' % (c, v))
                if cols[c] == 'bigint' and v > common.BIGINT_MAX:
                    # unsigned 64-bit counter: store two's-complement bit pattern
                    w.note('smart raw > 2^63-1 wrapped to signed')
                    v -= 2 ** 64
                row[c] = v
            w.add(row)
        day_bytes = w.bytes - start
    z.close()
    w.close(sources=[URL], licence='Backblaze Drive Stats: free to use with attribution to Backblaze; '
                                   'may not be sold (https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data)',
            download_bytes=common.raw_bytes(root, NAME),
            description='Wide (193 cols, ~55 non-empty per row) sparse daily SMART snapshots; '
                        'partition = (day, cluster, vault) with ~1-2.4k drives sorted by pod/slot/serial.',
            extra={'days_included': days,
                   'vault_subset': 'crc32(vault_id) %% %d < %d' % VAULT_KEEP})


if __name__ == '__main__':
    main(sys.argv[1])
