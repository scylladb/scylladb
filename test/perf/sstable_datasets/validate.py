#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Validate <root>/prepared/<name>/{schema.cql,data.jsonl,meta.json}.

Usage: python3 -I validate.py <data_root> <name>
Exit status is non-zero if any check fails.
"""
import datetime
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

ALLOWED = {'bigint', 'int', 'double', 'float', 'text', 'boolean', 'timestamp', 'uuid', 'blob', 'date'}
TS_RE = re.compile(r'^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$')
UUID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
BLOB_RE = re.compile(r'^0x[0-9a-fA-F]*$')


def check_value(t, v):
    if t in ('int', 'bigint'):
        lo, hi = (common.INT_MIN, common.INT_MAX) if t == 'int' else (common.BIGINT_MIN, common.BIGINT_MAX)
        return isinstance(v, int) and not isinstance(v, bool) and lo <= v <= hi
    if t in ('double', 'float'):
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            return False
        return t == 'double' or abs(v) <= 3.4e38
    if t == 'text':
        return isinstance(v, str)
    if t == 'boolean':
        return isinstance(v, bool)
    if t == 'timestamp':
        if not (isinstance(v, str) and TS_RE.match(v)):
            return False
        datetime.datetime.fromisoformat(v[:-1])
        return True
    if t == 'date':
        if not (isinstance(v, str) and len(v) == 10):
            return False
        datetime.date.fromisoformat(v)
        return True
    if t == 'uuid':
        return isinstance(v, str) and bool(UUID_RE.match(v))
    if t == 'blob':
        return isinstance(v, str) and bool(BLOB_RE.match(v))
    return False


def main(root, name):
    d = os.path.join(root, 'prepared', name)
    cql = open(os.path.join(d, 'schema.cql')).read()
    errors = []
    if cql.count('CREATE TABLE') != 1 or ('CREATE TABLE bench.%s (' % name) not in cql:
        errors.append('schema: expected exactly one CREATE TABLE bench.%s' % name)
    if 'compression' in cql.lower():
        errors.append('schema: must not set compression')
    cols = common.schema_columns(cql)
    bad_types = {c: t for c, t in cols.items() if t not in ALLOWED}
    if bad_types:
        errors.append('schema: disallowed types %s' % bad_types)
    m = re.search(r'PRIMARY KEY\s*\(\s*\(([^)]*)\)\s*(?:,([^)]*))?\)', cql)
    part = [x.strip() for x in m.group(1).split(',')]
    clust = [x.strip() for x in (m.group(2) or '').split(',') if x.strip()]
    pk = part + clust
    for c in pk:
        if c not in cols:
            errors.append('schema: pk column %s not defined' % c)

    keys = set()
    parts = {}
    n = 0
    samples = []
    bad = 0
    with open(os.path.join(d, 'data.jsonl'), 'rb') as f:
        for ln, line in enumerate(f, 1):
            n += 1
            try:
                row = json.loads(line.decode('utf-8'))
            except (UnicodeDecodeError, ValueError) as e:
                errors.append('line %d: %s' % (ln, e))
                bad += 1
                continue
            problems = []
            if not isinstance(row, dict):
                problems.append('not an object')
                row = {}
            for k, v in row.items():
                if k not in cols:
                    problems.append('unknown column %s' % k)
                elif v is None:
                    problems.append('null value %s' % k)
                elif not check_value(cols[k], v):
                    problems.append('bad %s value for %s: %r' % (cols[k], k, v if not isinstance(v, str) else v[:80]))
            for i, c in enumerate(pk):
                if c not in row:
                    problems.append('missing pk %s' % c)
                elif i < len(part) and row[c] == '':
                    problems.append('empty partition key %s' % c)
            if problems:
                bad += 1
                if bad <= 20:
                    errors.append('line %d: %s' % (ln, '; '.join(problems)))
                continue
            key = tuple(row[c] for c in pk)
            if key in keys:
                bad += 1
                if bad <= 20:
                    errors.append('line %d: duplicate primary key' % ln)
            keys.add(key)
            pkey = key[:len(part)]
            parts[pkey] = parts.get(pkey, 0) + 1
            if len(samples) < 2 and (ln == 1 or ln % 9973 == 0):
                samples.append(line.decode('utf-8').rstrip('\n'))
    meta = json.load(open(os.path.join(d, 'meta.json')))
    if meta['rows'] != n:
        errors.append('meta rows %d != lines %d' % (meta['rows'], n))
    if meta['distinct_partitions'] != len(parts):
        errors.append('meta partitions %d != %d' % (meta['distinct_partitions'], len(parts)))
    print('== %s: %d lines, %d partitions, %d bad rows' % (name, n, len(parts), bad))
    for s in samples:
        print('  sample: %s' % (s if len(s) <= 700 else s[:700] + '...[%d bytes]' % len(s)))
    for e in errors[:25]:
        print('  ERROR %s' % e)
    print('  %s' % ('OK' if not errors else 'FAILED'))
    return 0 if not errors else 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1], sys.argv[2]))
