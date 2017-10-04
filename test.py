#!/usr/bin/env python3
#
# Copyright (C) 2015 ScyllaDB
#

#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
#
import os
import sys
import argparse
import subprocess
import signal
import shlex
import threading

boost_tests = [
    'bytes_ostream_test',
    'chunked_vector_test',
    'compress_test',
    'types_test',
    'keys_test',
    'mutation_test',
    'mvcc_test',
    'schema_registry_test',
    'range_test',
    'mutation_reader_test',
    'serialized_action_test',
    'cql_query_test',
    'storage_proxy_test',
    'schema_change_test',
    'sstable_mutation_test',
    'sstable_atomic_deletion_test',
    'commitlog_test',
    'hash_test',
    'test-serialization',
    'cartesian_product_test',
    'allocation_strategy_test',
    'UUID_test',
    'compound_test',
    'murmur_hash_test',
    'partitioner_test',
    'frozen_mutation_test',
    'canonical_mutation_test',
    'gossiping_property_file_snitch_test',
    'row_cache_test',
    'cache_streamed_mutation_test',
    'network_topology_strategy_test',
    'query_processor_test',
    'batchlog_manager_test',
    'logalloc_test',
    'log_heap_test',
    'crc_test',
    'flush_queue_test',
    'config_test',
    'dynamic_bitset_test',
    'gossip_test',
    'managed_vector_test',
    'map_difference_test',
    'memtable_test',
    'mutation_query_test',
    'snitch_reset_test',
    'auth_test',
    'idl_test',
    'range_tombstone_list_test',
    'streamed_mutation_test',
    'anchorless_list_test',
    'database_test',
    'input_stream_test',
    'nonwrapping_range_test',
    'virtual_reader_test',
    'counter_test',
    'cell_locker_test',
    'view_schema_test',
    'combined_mutation_reader_test',
    'clustering_ranges_walker_test',
    'vint_serialization_test',
    'duration_test',
    'restricted_reader_test',
    'loading_cache_test',
]

other_tests = [
]

last_len = 0


def print_status_short(msg):
    global last_len
    print('\r' + ' ' * last_len, end='')
    last_len = len(msg)
    print('\r' + msg, end='')

print_status_verbose = print


class Alarm(Exception):
    pass


def alarm_handler(signum, frame):
    raise Alarm

def boost_test_wants_double_dash(path):
    magic = b'All the arguments after the -- are ignored'
    return magic in subprocess.check_output([path, '--help'], stderr=subprocess.STDOUT)

if __name__ == "__main__":
    all_modes = ['debug', 'release']

    parser = argparse.ArgumentParser(description="Seastar test runner")
    parser.add_argument('--fast',  action="store_true",
                        help="Run only fast tests")
    parser.add_argument('--name',  action="store",
                        help="Run only test whose name contains given string")
    parser.add_argument('--mode', choices=all_modes,
                        help="Run only tests for given build mode")
    parser.add_argument('--timeout', action="store", default="300", type=int,
                        help="timeout value for test execution")
    parser.add_argument('--jenkins', action="store",
                        help="jenkins output file prefix")
    parser.add_argument('--verbose', '-v', action='store_true', default=False,
                        help='Verbose reporting')
    args = parser.parse_args()

    black_hole = open('/dev/null', 'w')
    print_status = print_status_verbose if args.verbose else print_status_short

    test_to_run = []
    modes_to_run = all_modes if not args.mode else [args.mode]
    for mode in modes_to_run:
        prefix = os.path.join('build', mode, 'tests')
        for test in other_tests:
            test_to_run.append((os.path.join(prefix, test), 'other'))
        for test in boost_tests:
            test_to_run.append((os.path.join(prefix, test), 'boost'))

    if 'release' in modes_to_run:
        test_to_run.append(('build/release/tests/lsa_async_eviction_test', 'other',
                            '-c1 -m200M --size 1024 --batch 3000 --count 2000000'.split()))
        test_to_run.append(('build/release/tests/lsa_sync_eviction_test', 'other',
                            '-c1 -m100M --count 10 --standard-object-size 3000000'.split()))
        test_to_run.append(('build/release/tests/lsa_sync_eviction_test', 'other',
                            '-c1 -m100M --count 24000 --standard-object-size 2048'.split()))
        test_to_run.append(('build/release/tests/lsa_sync_eviction_test', 'other',
                            '-c1 -m1G --count 4000000 --standard-object-size 128'.split()))
        test_to_run.append(('build/release/tests/row_cache_alloc_stress', 'other',
                            '-c1 -m1G'.split()))
        test_to_run.append(('build/release/tests/sstable_test', 'boost', ['-c1']))
        test_to_run.append(('build/release/tests/row_cache_stress_test', 'other', '-c1 -m1G --seconds 10'.split()))
    if 'debug' in modes_to_run:
        test_to_run.append(('build/debug/tests/sstable_test', 'boost', ['-c1']))

    if args.name:
        test_to_run = [t for t in test_to_run if args.name in t[0]]

    all_ok = True

    n_total = len(test_to_run)
    env = os.environ
    # disable false positive due to new (with_alignment(...)) ...
    env['ASAN_OPTIONS'] = 'alloc_dealloc_mismatch=0'
    env['UBSAN_OPTIONS'] = 'print_stacktrace=1'
    for n, test in enumerate(test_to_run):
        path = test[0]
        exec_args = test[2] if len(test) >= 3 else []
        boost_args = []
        prefix = '[%d/%d]' % (n + 1, n_total)
        exec_args += '--collectd 0'.split()
        signal.signal(signal.SIGALRM, alarm_handler)
        if args.jenkins and test[1] == 'boost':
            mode = 'release'
            if test[0].startswith(os.path.join('build', 'debug')):
                mode = 'debug'
            xmlout = (args.jenkins + "." + mode + "." +
                      os.path.basename(test[0].split()[0]) + ".boost.xml")
            boost_args += ['--output_format=XML', '--log_level=test_suite', '--report_level=no', '--log_sink=' + xmlout]
        print_status('%s RUNNING %s %s' % (prefix, path, ' '.join(boost_args + exec_args)))
        if test[1] == 'boost' and boost_test_wants_double_dash(path):
            boost_args += ['--']
        proc = subprocess.Popen([path] + boost_args + exec_args, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                env=env, preexec_fn=os.setsid)
        out = None
        def on_timeout():
            if proc.returncode is None:
                print_status('TIMED OUT\n')
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                proc.kill()
        timeout = threading.Timer(args.timeout, on_timeout)
        timeout.start()
        out, _ = proc.communicate()
        timeout.cancel()
        if proc.returncode:
            print_status('FAILED: %s\n' % (path))
            print_status('  with error code {code}\n'.format(code=proc.returncode))
            if out:
                print('=== stdout START ===')
                print(str(out, encoding='UTF-8'))
                print('=== stdout END ===')
            all_ok = False
        else:
            print_status('%s PASSED %s' % (prefix, path))

    if all_ok:
        print('\nOK.')
    else:
        print_status('')
        sys.exit(1)
