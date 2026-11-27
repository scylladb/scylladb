#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Describe the layout of the Parquet Data files of perf-sstable-datasets results.

Usage: python3 parquet_layout.py <results_dir> [--leaves DATASET]

For each <results_dir>/<name>/aa_zstd/*-Data.db, prints one markdown table
row: leaves, row groups, data pages, the mean stored size of a data page,
and the share of the file taken by the metadata leaves (`_scylla.*`).
With --leaves, prints a table of the leaves of one dataset instead.

The footer and the OffsetIndexes are decoded here, with a minimal reader of
the Thrift compact protocol, so the script needs the standard library only.
"""

import argparse
import glob
import os
import struct
import sys

ORDER = ['drive_stats', 'ghcn_daily', 'fhvhv_trips', 'github_events', 'reviews', 'trades', 'pageviews', 'price_paid', 'isd_lite']


class Compact:
    """A decoder of Thrift compact protocol structs into {field id: value}."""

    def __init__(self, buf, pos=0):
        self.b = buf
        self.p = pos

    def varint(self):
        r = s = 0
        while True:
            x = self.b[self.p]
            self.p += 1
            r |= (x & 0x7f) << s
            s += 7
            if not x & 0x80:
                return r

    def zigzag(self):
        v = self.varint()
        return (v >> 1) ^ -(v & 1)

    def value(self, t):
        if t in (1, 2):
            return t == 1
        if t == 3:
            self.p += 1
            return struct.unpack('b', self.b[self.p - 1:self.p])[0]
        if t in (4, 5, 6):
            return self.zigzag()
        if t == 7:
            self.p += 8
            return struct.unpack('<d', self.b[self.p - 8:self.p])[0]
        if t == 8:
            n = self.varint()
            self.p += n
            return bytes(self.b[self.p - n:self.p])
        if t in (9, 10):
            h = self.b[self.p]
            self.p += 1
            n, et = h >> 4, h & 0x0f
            if n == 15:
                n = self.varint()
            if et in (1, 2):
                out = [self.b[self.p + i] == 1 for i in range(n)]
                self.p += n
                return out
            return [self.value(et) for _ in range(n)]
        if t == 11:
            n = self.varint()
            if n == 0:
                return {}
            kv = self.b[self.p]
            self.p += 1
            return {self.value(kv >> 4): self.value(kv & 0x0f) for _ in range(n)}
        if t == 12:
            return self.struct()
        raise ValueError('unknown compact type %d at %d' % (t, self.p))

    def struct(self):
        out = {}
        fid = 0
        while True:
            h = self.b[self.p]
            self.p += 1
            if h == 0:
                return out
            delta, t = h >> 4, h & 0x0f
            fid = fid + delta if delta else self.zigzag()
            out[fid] = self.value(t)


def read_layout(path):
    with open(path, 'rb') as f:
        data = f.read()
    if data[:4] != b'PAR1' or data[-4:] != b'PAR1':
        raise SystemExit('%s is not a Parquet file' % path)
    flen = struct.unpack('<I', data[-8:-4])[0]
    fmd = Compact(data, len(data) - 8 - flen).struct()
    leaves = {}
    for rg in fmd[4]:
        for cc in rg[1]:
            md = cc[3]
            name = '.'.join(x.decode() for x in md[3])
            leaf = leaves.setdefault(name, {'bytes': 0, 'pages': 0, 'dict_pages': 0, 'values': 0})
            leaf['bytes'] += md[7]
            leaf['values'] += md[5]
            if 11 in md:
                leaf['dict_pages'] += 1
            if 4 in cc:
                oi = Compact(data, cc[4]).struct()
                leaf['pages'] += len(oi[1])
    return {'size': len(data), 'rows': fmd[3], 'row_groups': len(fmd[4]), 'footer': flen, 'leaves': leaves}


def datasets(results_dir):
    names = [n for n in os.listdir(results_dir) if glob.glob(os.path.join(results_dir, n, 'aa_zstd', '*-Data.db'))]
    names.sort(key=lambda n: (ORDER.index(n) if n in ORDER else len(ORDER), n))
    return names


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('results_dir')
    ap.add_argument('--leaves', metavar='DATASET')
    a = ap.parse_args()
    if a.leaves:
        lay = read_layout(glob.glob(os.path.join(a.results_dir, a.leaves, 'aa_zstd', '*-Data.db'))[0])
        print('| Leaf | Stored (KiB) | Share of file | Data pages | Mean page (B) | Chunks with a dictionary |')
        print('|---|---|---|---|---|---|')
        for name, l in sorted(lay['leaves'].items(), key=lambda kv: -kv[1]['bytes']):
            print('| %s | %s | %.1f%% | %d | %s | %d |' % (
                name, '{:,.0f}'.format(l['bytes'] / 1024), 100.0 * l['bytes'] / lay['size'], l['pages'],
                '{:,.0f}'.format(l['bytes'] / l['pages']) if l['pages'] else 'n/a', l['dict_pages']))
        return
    print('| Dataset | Leaves | Metadata leaves | Row groups | Rows per row group | Data pages | Mean page (B) '
          '| Rows per page | Metadata leaves share | Footer (B) |')
    print('|---|---|---|---|---|---|---|---|---|---|')
    for n in datasets(a.results_dir):
        lay = read_layout(glob.glob(os.path.join(a.results_dir, n, 'aa_zstd', '*-Data.db'))[0])
        lv = lay['leaves']
        meta = [k for k in lv if k.startswith('_scylla')]
        pages = sum(l['pages'] for l in lv.values())
        stored = sum(l['bytes'] for l in lv.values())
        meta_bytes = sum(lv[k]['bytes'] for k in meta)
        print('| %s | %d | %d | %d | %s | %s | %s | %s | %.1f%% | %s |' % (
            n, len(lv), len(meta), lay['row_groups'], '{:,.0f}'.format(lay['rows'] / lay['row_groups']),
            '{:,}'.format(pages), '{:,.0f}'.format(stored / pages), '{:,.0f}'.format(lay['rows'] * len(lv) / pages),
            100.0 * meta_bytes / lay['size'], '{:,}'.format(lay['footer'])))


if __name__ == '__main__':
    sys.exit(main())
