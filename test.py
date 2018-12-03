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
    'continuous_data_consumer_test',
    'types_test',
    'keys_test',
    'mutation_test',
    'mvcc_test',
    'schema_registry_test',
    'range_test',
    'mutation_reader_test',
    'serialized_action_test',
    'cql_query_test',
    'secondary_index_test',
    'filtering_test',
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
    'checksum_utils_test',
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
    'view_build_test',
    'view_complex_test',
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
    'limiting_data_source_test',
    'sstable_test',
    'sstable_3_x_test',
    'meta_test',
    'reusable_buffer_test',
    'multishard_writer_test',
    'observable_test',
    'transport_test',
    'fragmented_temporary_buffer_test',
    'auth_passwords_test',
    'multishard_mutation_query_test',
]

other_tests = [
    'memory_footprint',
]


def print_progress_succint(test_path, test_args, success, cookie):
    if type(cookie) is int:
        cookie = (0, 1, cookie)

    last_len, n, n_total = cookie
    if success:
        status = "PASSED"
    else:
        status = "FAILED"

    msg = "[{}/{}] {} {} {}".format(n, n_total, status, test_path, ' '.join(test_args))
    if sys.stdout.isatty():
        print('\r' + ' ' * last_len, end='')
        last_len = len(msg)
        print('\r' + msg, end='')
    else:
        print(msg)

    return (last_len, n + 1, n_total)


def print_status_verbose(test_path, test_args, success, cookie):
    if type(cookie) is int:
        cookie = (1, cookie)

    n, n_total = cookie
    if success:
        status = "PASSED"
    else:
        status = "FAILED"

    msg = "[{}/{}] {} {} {}".format(n, n_total, status, test_path, ' '.join(test_args))
    print(msg)

    return (n + 1, n_total)


class Alarm(Exception):
    pass


def alarm_handler(signum, frame):
    raise Alarm

if __name__ == "__main__":
    all_modes = ['debug', 'release']

    sysmem = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    testmem = 2e9
    default_num_jobs = ((sysmem - 4e9) // testmem)

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
    parser.add_argument('--jobs', '-j', action="store", default=default_num_jobs, type=int,
                        help="Number of jobs to use for running the tests")
    args = parser.parse_args()

    print_progress = print_status_verbose if args.verbose else print_progress_succint

    custom_seastar_args = {
        "sstable_test": ['-c1'],
        "sstable_3_x_test": ['-c1'],
        "mutation_reader_test": ['-c{}'.format(min(os.cpu_count(), 3)), '-m2G'],
    }

    test_to_run = []
    modes_to_run = all_modes if not args.mode else [args.mode]
    for mode in modes_to_run:
        prefix = os.path.join('build', mode, 'tests')
        standard_args =  '--overprovisioned --unsafe-bypass-fsync 1 --blocked-reactor-notify-ms 2000000'.split()
        seastar_args = '-c2 -m2G'.split()
        for test in other_tests:
            test_to_run.append((os.path.join(prefix, test), 'other', custom_seastar_args.get(test, seastar_args) + standard_args))
        for test in boost_tests:
            test_to_run.append((os.path.join(prefix, test), 'boost', custom_seastar_args.get(test, seastar_args) + standard_args))

    if 'release' in modes_to_run:
        test_to_run.append(('build/release/tests/lsa_async_eviction_test', 'other',
                            '-c1 -m200M --size 1024 --batch 3000 --count 2000000'.split() + standard_args))
        test_to_run.append(('build/release/tests/lsa_sync_eviction_test', 'other',
                            '-c1 -m100M --count 10 --standard-object-size 3000000'.split() + standard_args))
        test_to_run.append(('build/release/tests/lsa_sync_eviction_test', 'other',
                            '-c1 -m100M --count 24000 --standard-object-size 2048'.split() + standard_args))
        test_to_run.append(('build/release/tests/lsa_sync_eviction_test', 'other',
                            '-c1 -m1G --count 4000000 --standard-object-size 128'.split() + standard_args))
        test_to_run.append(('build/release/tests/row_cache_alloc_stress', 'other',
                            '-c1 -m2G'.split() + standard_args))
        test_to_run.append(('build/release/tests/row_cache_stress_test', 'other', '-c1 -m1G --seconds 10'.split() + standard_args))

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
        # avoid modifying in-place, it will change test_to_run
        exec_args = exec_args + '--collectd 0'.split()
        file = io.StringIO()
        if args.jenkins and type == 'boost':
            mode = 'release'
            if path.startswith(os.path.join('build', 'debug')):
                mode = 'debug'
            xmlout = (args.jenkins + "." + mode + "." +
                      os.path.basename(path.split()[0]) + ".boost.xml")
            boost_args += ['--report_level=no', '--logger=HRF,test_suite:XML,test_suite,' + xmlout]
        if type == 'boost':
            boost_args += ['--']
        def report_error(out, report_subcause):
            report_subcause()
            if out:
                print('=== stdout START ===', file=file)
                print(out, file=file)
                print('=== stdout END ===', file=file)
        out = None
        success = False
        try:
            out = subprocess.check_output([path] + boost_args + exec_args,
                                stderr=subprocess.STDOUT,
                                timeout=args.timeout,
                                env=env, preexec_fn=os.setsid)
            success = True
        except subprocess.TimeoutExpired as e:
            def report_subcause():
                print('  timed out', file=file)
            report_error(e.output.decode(encoding='UTF-8'), report_subcause=report_subcause)
        except subprocess.CalledProcessError as e:
            def report_subcause():
                print('  with error code {code}\n'.format(code=e.returncode), file=file)
            report_error(e.output.decode(encoding='UTF-8'), report_subcause=report_subcause)
        except Exception as e:
            def report_subcause():
                print('  with error {e}\n'.format(e=e), file=file)
            report_error(e, report_subcause=report_subcause)
        return (path, boost_args + exec_args, success, file.getvalue())
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs)
    futures = []
    for n, test in enumerate(test_to_run):
        path = test[0]
        test_type = test[1]
        exec_args = test[2] if len(test) >= 3 else []
        futures.append(executor.submit(run_test, path, test_type, exec_args))

    cookie = n_total
    for future in concurrent.futures.as_completed(futures):
        test_path, test_args, success, out = future.result()
        cookie = print_progress(test_path, test_args, success, cookie)
        if not success:
            failed_tests.append((test_path, test_args, out))

    if not failed_tests:
        print('\nOK.')
    else:
        print('\n\nOutput of the failed tests:')
        for test, args, out in failed_tests:
            print("Test {} {} failed:\n{}".format(test, ' '.join(args), out))
        print('\n\nThe following test(s) have failed:')
        for test, args, _ in failed_tests:
            print('  {} {}'.format(test, ' '.join(args)))
        print('\nSummary: {} of the total {} tests failed'.format(len(failed_tests), len(test_to_run)))
        sys.exit(1)
