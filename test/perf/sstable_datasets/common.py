#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Shared row emission for benchmark dataset converters.

All output-format decisions (JSON encoding, timestamp format, handling of
absent values, primary-key checks, deduplication, size target, meta.json)
live here, so that a change to the output contract touches one file.
"""

import datetime
import json
import math
import os
import statistics
import sys
from collections import Counter

DEFAULT_TARGET_BYTES = 400 * 1000 * 1000

INT_MIN, INT_MAX = -(2 ** 31), 2 ** 31 - 1
BIGINT_MIN, BIGINT_MAX = -(2 ** 63), 2 ** 63 - 1

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


# ---------------------------------------------------------------- value helpers

def ts_from_ms(ms):
    """Milliseconds since the epoch -> ISO-8601 string with ms and 'Z'."""
    ms = int(ms)
    dt = _EPOCH + datetime.timedelta(milliseconds=ms)
    return dt.strftime('%Y-%m-%dT%H:%M:%S.') + '%03dZ' % (ms % 1000)


def ts_from_dt(dt):
    """datetime (naive values are taken as UTC) -> ISO-8601 string with ms and 'Z'."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(datetime.timezone.utc)
    return dt.strftime('%Y-%m-%dT%H:%M:%S.') + '%03dZ' % (dt.microsecond // 1000)


def ts_from_iso(s):
    """'2024-01-15T15:00:00Z' (or with offset) -> canonical ISO string with ms."""
    return ts_from_dt(datetime.datetime.fromisoformat(s.replace('Z', '+00:00')))


def date_str(d):
    """date/datetime or 'YYYYMMDD' / 'YYYY-MM-DD...' string -> 'YYYY-MM-DD'."""
    if isinstance(d, (datetime.date, datetime.datetime)):
        return d.strftime('%Y-%m-%d')
    if len(d) == 8 and d.isdigit():
        return '%s-%s-%s' % (d[0:4], d[4:6], d[6:8])
    datetime.date.fromisoformat(d[:10])  # validate
    return d[:10]


def opt_int(s):
    """CSV field -> int or None if empty."""
    if s is None or s == '':
        return None
    return int(s)


def opt_float(s):
    if s is None or s == '':
        return None
    return float(s)


def opt_text(s):
    if s is None or s == '':
        return None
    return s


# ---------------------------------------------------------------- writer

class Writer:
    """Writes <root>/prepared/<name>/{schema.cql,data.jsonl,meta.json}.

    pk: list of primary-key column names; the first `npart` form the
    partition key.  Rows whose PK columns are missing (or whose text
    partition-key components are empty) are dropped; duplicate primary keys
    keep the first occurrence.  Both are counted for meta.json.
    """

    def __init__(self, root, name, schema_cql, pk, npart,
                 target_bytes=DEFAULT_TARGET_BYTES):
        self.name = name
        self.dir = os.path.join(root, 'prepared', name)
        os.makedirs(self.dir, exist_ok=True)
        with open(os.path.join(self.dir, 'schema.cql'), 'w') as f:
            f.write(schema_cql.strip() + '\n')
        self.path = os.path.join(self.dir, 'data.jsonl')
        self.f = open(self.path, 'wb')
        self.pk = list(pk)
        self.npart = npart
        self.target = target_bytes
        self.bytes = 0
        self.rows = 0
        self.keys = set()
        self.parts = Counter()
        self.dropped = Counter()      # reason -> count
        self.notes = Counter()        # converter-defined event counts
        self.duplicates = 0
        self.max_line = 0
        self.max_value = (0, None)    # (bytes, column)
        self.lines_with_quote = 0
        self.utf8_fixed = 0

    @property
    def full(self):
        return self.bytes >= self.target

    def drop(self, reason):
        self.dropped[reason] += 1

    def note(self, what, n=1):
        self.notes[what] += n

    def _clean(self, v):
        if isinstance(v, str):
            try:
                v.encode('utf-8')
            except UnicodeEncodeError:
                # lone surrogates and similar: replace, count
                self.utf8_fixed += 1
                v = v.encode('utf-8', 'replace').decode('utf-8')
        return v

    def add(self, row):
        """Emit one row (dict col -> value; None values are omitted).
        Returns True if written."""
        out = {}
        for k, v in row.items():
            if v is None:
                continue
            if isinstance(v, float) and not math.isfinite(v):
                self.notes['non-finite float omitted'] += 1
                continue
            out[k] = self._clean(v)
        key = []
        for i, c in enumerate(self.pk):
            v = out.get(c)
            if v is None:
                self.drop('missing pk column %s' % c)
                return False
            if i < self.npart and v == '':
                self.drop('empty partition key %s' % c)
                return False
            key.append(v)
        key = tuple(key)
        if key in self.keys:
            self.duplicates += 1
            return False
        self.keys.add(key)
        line = json.dumps(out, ensure_ascii=False, separators=(',', ':'),
                          allow_nan=False).encode('utf-8') + b'\n'
        self.f.write(line)
        self.bytes += len(line)
        self.rows += 1
        self.parts[key[:self.npart]] += 1
        if len(line) > self.max_line:
            self.max_line = len(line)
        for k, v in out.items():
            if isinstance(v, str):
                n = len(v.encode('utf-8'))
                if n > self.max_value[0]:
                    self.max_value = (n, k)
        if b"'" in line:
            self.lines_with_quote += 1
        return True

    def close(self, *, sources, licence, download_bytes, description, extra=None):
        self.f.close()
        counts = sorted(self.parts.values())
        n = len(counts)

        def pct(p):
            return counts[min(n - 1, int(math.ceil(p * n)) - 1)] if n else 0

        meta = {
            'name': self.name,
            'description': description,
            'source_urls': sources,
            'licence': licence,
            'download_bytes': download_bytes,
            'rows': self.rows,
            'distinct_partitions': n,
            'rows_per_partition': {
                'min': counts[0] if n else 0,
                'median': statistics.median_low(counts) if n else 0,
                'p99': pct(0.99),
                'max': counts[-1] if n else 0,
            },
            'data_jsonl_bytes': os.path.getsize(self.path),
            'max_line_bytes': self.max_line,
            'max_text_value': {'bytes': self.max_value[0], 'column': self.max_value[1]},
            'lines_containing_single_quote': self.lines_with_quote,
            'dropped_rows': dict(self.dropped),
            'duplicate_pk_rows_skipped': self.duplicates,
            'text_values_with_invalid_unicode_replaced': self.utf8_fixed,
            'notes': dict(self.notes),
        }
        if extra:
            meta.update(extra)
        with open(os.path.join(self.dir, 'meta.json'), 'w') as f:
            json.dump(meta, f, indent=2, sort_keys=False)
            f.write('\n')
        print(json.dumps(meta, indent=2), file=sys.stderr)
        return meta


def raw_bytes(root, name, files=None):
    """Total size of downloaded files (all files in raw/<name>, or the given subset)."""
    d = os.path.join(root, 'raw', name)
    names = files if files is not None else [x for x in os.listdir(d) if not x.endswith('.partial')]
    return sum(os.path.getsize(os.path.join(d, x)) for x in names)


def load_schema(name):
    """Read the schema for <name> from schemas/<name>.cql next to this file."""
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, 'schemas', name + '.cql')) as f:
        return f.read()


def schema_columns(cql):
    """Parse 'name type,' lines of a CREATE TABLE -> ordered dict col -> type."""
    cols = {}
    body = cql[cql.index('(') + 1:]
    for line in body.splitlines():
        line = line.split('--')[0].strip().rstrip(',')
        if not line or line.upper().startswith('PRIMARY KEY') or line.startswith(')'):
            continue
        parts = line.split()
        if len(parts) == 2:
            cols[parts[0]] = parts[1]
    return cols
