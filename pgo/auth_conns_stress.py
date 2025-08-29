#!/usr/bin/env python3
#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import sys
import random
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import concurrent.futures
import time

# args: host user password iterations concurrency
if len(sys.argv) != 6:
    print(f"Usage: {sys.argv[0]} <host> <user> <password> <iterations> <concurrency>")
    sys.exit(1)

host, user, password, iterations, concurrency = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5])
if concurrency < 1:
    print("concurrency must be >= 1")
    sys.exit(1)

base = iterations // concurrency
rem = iterations % concurrency
counts = [base + (1 if i < rem else 0) for i in range(concurrency)]

def worker(n: int):
    if n <= 0:
        return
    auth_provider = PlainTextAuthProvider(username=user, password=password)
    cluster = Cluster([host], auth_provider=auth_provider)
    try:
        # Prepare once (needs a session). Then reuse prepared statement with new sessions per iteration.
        prep_session = cluster.connect()
        prep_session.set_keyspace('auth_test')
        prepared = prep_session.prepare("SELECT v FROM auth_table WHERE pk=?")
        prep_session.shutdown()
        while n:
            try:
                session = cluster.connect()
            except:
                # we may operate near overload here, or
                # at least near overload protection so connects
                # may timeout, reduce the pressure
                time.sleep(0.2)
                continue
            try:
                session.set_keyspace('auth_test')
                pk = random.randint(1, 3)
                session.execute(prepared, (pk,))
            finally:
                session.shutdown()
            n -= 1
    finally:
        cluster.shutdown()

with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as ex:
    futures = [ex.submit(worker, c) for c in counts]
    for f in futures:
        f.result()
