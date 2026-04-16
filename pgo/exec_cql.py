#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""exec_cql.py
Execute CQL statements from a file where each non-empty, non-comment line is exactly one CQL statement.
Connects via a Unix domain socket (maintenance socket), bypassing authentication.
Requires python cassandra-driver. Stops at first failure.
Usage:
  ./exec_cql.py --file ./conf/auth.cql --socket /path/to/cql.m
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

def exec_statements(statements: list[tuple[int, str]], socket_path: str, timeout: float) -> int:
    """Execute CQL statements via a Unix domain socket (maintenance socket).

    The maintenance socket only starts listening after the auth subsystem is
    fully initialised, so a successful connect means the node is ready.
    """
    from cassandra.cluster import Cluster
    from cassandra.connection import UnixSocketEndPoint  # type: ignore
    from cassandra.policies import WhiteListRoundRobinPolicy  # type: ignore

    ep = UnixSocketEndPoint(socket_path)
    try:
        cluster = Cluster(
            contact_points=[ep],
            load_balancing_policy=WhiteListRoundRobinPolicy([ep]),
        )
        session = cluster.connect()
    except Exception as e:
        print(f'ERROR: failed to connect to maintenance socket {socket_path}: {e}', file=sys.stderr)
        return 2

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
    ap = argparse.ArgumentParser(description='Execute one-line CQL statements from file via maintenance socket')
    ap.add_argument('--file', required=True)
    ap.add_argument('--socket', required=True,
                    help='Path to the Unix domain maintenance socket (<workdir>/cql.m)')
    ap.add_argument('--timeout', type=float, default=30.0)
    args = ap.parse_args(argv)
    if not os.path.isfile(args.file):
        print(f"File not found: {args.file}", file=sys.stderr)
        return 2
    stmts = read_statements(args.file)
    if not stmts:
        print('No statements found', file=sys.stderr)
        return 1
    rc = exec_statements(stmts, args.socket, args.timeout)
    if rc == 0:
        print('All statements executed successfully')
    return rc

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
