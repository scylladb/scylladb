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
import concurrent.futures
import io

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
    'sstable_resharding_test',
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
    'cache_flat_mutation_reader_test',
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
    'mutation_fragment_test',
    'flat_mutation_reader_test',
    'anchorless_list_test',
    'database_test',
    'input_stream_test',
    'nonwrapping_range_test',
    'virtual_reader_test',
    'counter_test',
    'cell_locker_test',
    'view_schema_test',
    'clustering_ranges_walker_test',
    'vint_serialization_test',
    'duration_test',
    'loading_cache_test',
    'castas_fcts_test',
    'big_decimal_test',
    'aggregate_fcts_test',
    'role_manager_test',
    'caching_options_test',
    'auth_resource_test',
    'cql_auth_query_test',
    'enum_set_test',
    'extensions_test',
    'cql_auth_syntax_test',
    'querier_cache',
]

other_tests = [
    'memory_footprint',
    'querier_cache_resource_based_eviction',
]

last_len = 0


def print_status_short(msg, file=sys.stdout):
    global last_len
    print('\r' + ' ' * last_len, end='', file=file)
    last_len = len(msg)
    print('\r' + msg, end='', file=file)

print_status_verbose = print


class Alarm(Exception):
    pass


def alarm_handler(signum, frame):
    raise Alarm

if __name__ == "__main__":
    all_modes = ['debug', 'release']

    parser = argparse.ArgumentParser(description="Scylla test runner")
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

    print_status = print_status_verbose if args.verbose else print_status_short

    test_to_run = []
    modes_to_run = all_modes if not args.mode else [args.mode]
    for mode in modes_to_run:
        prefix = os.path.join('build', mode, 'tests')
        seastar_args = '-c2 -m2G --overprovisioned --unsafe-bypass-fsync 1'.split()
        for test in other_tests:
            test_to_run.append((os.path.join(prefix, test), 'other', seastar_args))
        for test in boost_tests:
            test_to_run.append((os.path.join(prefix, test), 'boost', seastar_args))

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
                            '-c1 -m2G'.split()))
        test_to_run.append(('build/release/tests/sstable_test', 'boost', ['-c1']))
        test_to_run.append(('build/release/tests/row_cache_stress_test', 'other', '-c1 -m1G --seconds 10'.split()))
    if 'debug' in modes_to_run:
        test_to_run.append(('build/debug/tests/sstable_test', 'boost', ['-c1']))

    if args.name:
        test_to_run = [t for t in test_to_run if args.name in t[0]]

    failed_tests = []

    n_total = len(test_to_run)
    env = os.environ
    # disable false positive due to new (with_alignment(...)) ...
    env['ASAN_OPTIONS'] = 'alloc_dealloc_mismatch=0'
    env['UBSAN_OPTIONS'] = 'print_stacktrace=1'
    env['BOOST_TEST_CATCH_SYSTEM_ERRORS'] = 'no'
    def run_test(path, type, exec_args):
        boost_args = []
        prefix = '[%d/%d]' % (n + 1, n_total)
        # avoid modifying in-place, it will change test_to_run
        exec_args = exec_args + '--collectd 0'.split()
        file = io.StringIO()
        if args.jenkins and type == 'boost':
            mode = 'release'
            if path.startswith(os.path.join('build', 'debug')):
                mode = 'debug'
            xmlout = (args.jenkins + "." + mode + "." +
                      os.path.basename(path.split()[0]) + ".boost.xml")
            boost_args += ['--report_level=no', '--logger=XML,test_suite,' + xmlout]
        print_status('%s RUNNING %s %s' % (prefix, path, ' '.join(boost_args + exec_args)))
        if type == 'boost':
            boost_args += ['--']
        def report_error(out, report_subcause):
            print_status('FAILED: %s\n' % (path), file=file)
            report_subcause()
            if out:
                print('=== stdout START ===', file=file)
                print(str(out, encoding='UTF-8'), file=file)
                print('=== stdout END ===', file=file)
        out = None
        success = False
        try:
            out = subprocess.check_output([path] + boost_args + exec_args,
                                stderr=subprocess.STDOUT,
                                timeout=args.timeout,
                                env=env, preexec_fn=os.setsid)
            print_status('%s PASSED %s' % (prefix, path))
            success = True
        except subprocess.TimeoutExpired as e:
            def report_subcause():
                print('  timed out', file=file)
            report_error(e.output, report_subcause=report_subcause)
        except subprocess.CalledProcessError as e:
            def report_subcause():
                print_status('  with error code {code}\n'.format(code=e.returncode), file=file)
            report_error(e.output, report_subcause=report_subcause)
        except Exception as e:
            def report_subcause():
                print_status('  with error {e}\n'.format(e=e), file=file)
            report_error(e.out, report_subcause=report_subcause)
        return (path, success, file.getvalue())
    sysmem = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    testmem = 2e9
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=((sysmem - 4e9) // testmem))
    futures = []
    for n, test in enumerate(test_to_run):
        path = test[0]
        type = test[1]
        exec_args = test[2] if len(test) >= 3 else []
        futures.append(executor.submit(run_test, path, type, exec_args))
    for future in futures:
        path, success, out = future.result()
        if not success:
            failed_tests.append(path)
        print(out)

    if not failed_tests:
        print('\nOK.')
    else:
        print('\n\nThe following test(s) have failed:')
        for test in failed_tests:
            print('  {}'.format(test))
        print('\nSummary: {} of the total {} tests failed'.format(len(failed_tests), len(test_to_run)))
        sys.exit(1)
