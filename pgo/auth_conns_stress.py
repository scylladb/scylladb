#!/usr/bin/env python3
#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import sys
import random
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.auth import PlainTextAuthProvider
import os
import traceback
from concurrent.futures import ThreadPoolExecutor
import time
import argparse

"""auth_conns_stress

Process + thread fan-out stress tool creating a NEW session per request.

Total requests = iterations * processes * threads (minus retries on overload).
"""

def parse_args():
    p = argparse.ArgumentParser(description="Authentication connection stress generator")
    p.add_argument('--host', required=True, help='Contact point host')
    p.add_argument('--user', required=True, help='Username for auth')
    p.add_argument('--password', required=True, help='Password for auth')
    p.add_argument('--processes', type=int, required=True, help='Number of forked processes')
    p.add_argument('--threads', type=int, required=True, help='Threads per process')
    p.add_argument('--iterations', type=int, required=True, help='Operations per thread (each op opens a new session)')
    args = p.parse_args()
    if args.processes < 1 or args.threads < 1 or args.iterations < 1:
        p.error('processes, threads, iterations must all be >= 1')
    return args

args = parse_args()
host = args.host
user = args.user
password = args.password
processes = args.processes
threads = args.threads
iterations = args.iterations

def prepare_select(cluster):
    # Prepare once (needs a session). Retry silently on transient NoHostAvailable, likely an overload.
    while True:
        prep_session = None
        try:
            prep_session = cluster.connect()
            prepared = prep_session.prepare("SELECT v FROM auth_test.auth_table WHERE pk=?")
            return prepared
        except Exception as e:
            print(f"ERROR during prepare, retrying: {e}", file=sys.stderr, flush=True)
            traceback.print_exc()
            time.sleep(0.1)
        finally:
            if prep_session:
                try:
                    prep_session.shutdown()
                except Exception:
                    pass

def thread_worker(remaining: int, cluster, prepared):
    while remaining:
        session = None
        try:
            session = cluster.connect()
            pk = random.randint(1, 3)
            session.execute(prepared, (pk,))
        except NoHostAvailable:
            continue  # Retry without consuming remaining
        except Exception as e:
            print(f"ERROR: {e}", file=sys.stderr, flush=True)
            traceback.print_exc()
        finally:
            if session:
                try:
                    session.shutdown()
                except Exception:
                    pass
        remaining -= 1

def process_worker():
    auth_provider = PlainTextAuthProvider(username=user, password=password)
    cluster = Cluster([host], auth_provider=auth_provider)
    prepared = prepare_select(cluster)
    with ThreadPoolExecutor(max_workers=threads, thread_name_prefix="authstress") as executor:
        futures = [executor.submit(thread_worker, iterations, cluster, prepared) for _ in range(threads)]
        # Wait for all to complete; exceptions will be raised below when result() called.
        for f in futures:
            exc = f.exception()
            if exc:
                raise exc

child_pids = []
for _ in range(processes):
    pid = os.fork()
    if pid == 0:  # Child
        try:
            process_worker()
        except Exception as e:
            print(f"ERROR: {e}", file=sys.stderr, flush=True)
            traceback.print_exc()
            # In child: non-zero exit on failure
            os._exit(1)
        os._exit(0)
    else:
        child_pids.append(pid)

exit_code = 0
for pid in child_pids:
    _, status = os.waitpid(pid, 0)
    if os.WIFEXITED(status):
        code = os.WEXITSTATUS(status)
        if code != 0:
            exit_code = code
    elif os.WIFSIGNALED(status):
        exit_code = 128 + os.WTERMSIG(status)

if exit_code != 0:
    sys.exit(exit_code)
