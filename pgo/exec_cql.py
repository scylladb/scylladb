#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

"""exec_cql.py
Execute CQL statements from a file where each non-empty, non-comment line is exactly one CQL statement.
Requires python cassandra-driver. Stops at first failure.
Usage:
  ./exec_cql.py --file ./conf/auth.cql [--host 127.0.0.1 --port 9042]
"""
import argparse, os, sys
from typing import Sequence

def read_statements(path: str) -> list[tuple[int, str]]:
    stms: list[tuple[int, str]] = []
    with open(path, 'r', encoding='utf-8') as f:
        for lineno, raw in enumerate(f, start=1):
            line = raw.strip()
            if not line or line.startswith('--'):
                continue
            if line:
                stms.append((lineno, line))
    return stms

def exec_driver(statements: list[tuple[int, str]], host: str, port: int, timeout: float, username: str, password: str) -> int:
    try:
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider  # type: ignore
    except Exception:
        print('ERROR: cassandra-driver not installed. Install with: pip install cassandra-driver', file=sys.stderr)
        return 2
    auth_provider = None
    if username != "":
        auth_provider = PlainTextAuthProvider(username=username, password=password)
    cluster = Cluster([host], port=port, auth_provider=auth_provider)
    session = cluster.connect()
    try:
        for _, (lineno, s) in enumerate(statements, 1):
            try:
                session.execute(s, timeout=timeout)
            except Exception as e:
                print(f"ERROR executing statement from file line {lineno}: {s}\n{e}", file=sys.stderr)
                return 1
    finally:
        cluster.shutdown()
    return 0

def main(argv: Sequence[str]) -> int:
    ap = argparse.ArgumentParser(description='Execute one-line CQL statements from file (driver only)')
    ap.add_argument('--file', required=True)
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--port', type=int, default=9042)
    ap.add_argument('--timeout', type=float, default=30.0)
    ap.add_argument('--username', default='cassandra')
    ap.add_argument('--password', default='cassandra')
    args = ap.parse_args(argv)
    if not os.path.isfile(args.file):
        print(f"File not found: {args.file}", file=sys.stderr)
        return 2
    stmts = read_statements(args.file)
    if not stmts:
        print('No statements found', file=sys.stderr)
        return 1
    rc = exec_driver(stmts, args.host, args.port, args.timeout, args.username, args.password)
    if rc == 0:
        print('All statements executed successfully')
    return rc

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
