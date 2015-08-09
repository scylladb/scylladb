#!/usr/bin/env python3
#
# Copyright 2015 Cloudius Systems
#
import os
import sys
import argparse
import subprocess
import signal
import re

boost_tests = [
    'bytes_ostream_test',
    'types_test',
    'keys_test',
    'mutation_test',
    'range_test',
    'mutation_reader_test',
    'cql_query_test',
    'storage_proxy_test',
    'sstable_test',
    'sstable_mutation_test',
    'commitlog_test',
    'hash_test',
    'serializer_test',
    'test-serialization',
    'cartesian_product_test',
    'allocation_strategy_test',
    'UUID_test',
    'compound_test',
    'murmur_hash_test',
    'partitioner_test',
    'frozen_mutation_test',
    'gossiping_property_file_snitch_test',
    'row_cache_test',
    'network_topology_strategy_test',
    'query_processor_test',
    'batchlog_manager_test',
    'logalloc_test',
    'crc_test',
]

other_tests = [
]

last_len = 0

def print_status_short(msg):
    global last_len
    print('\r' + ' '*last_len, end='')
    last_len = len(msg)
    print('\r' + msg, end='')

print_status_verbose = print

class Alarm(Exception):
    pass
def alarm_handler(signum, frame):
    raise Alarm

if __name__ == "__main__":
    all_modes = ['debug', 'release']

    parser = argparse.ArgumentParser(description="Seastar test runner")
    parser.add_argument('--fast',  action="store_true", help="Run only fast tests")
    parser.add_argument('--name',  action="store", help="Run only test whose name contains given string")
    parser.add_argument('--mode', choices=all_modes, help="Run only tests for given build mode")
    parser.add_argument('--timeout', action="store",default="300",type=int, help="timeout value for test execution")
    parser.add_argument('--jenkins', action="store",help="jenkins output file prefix")
    parser.add_argument('--verbose', '-v', action = 'store_true', default = False,
                        help = 'Verbose reporting')
    args = parser.parse_args()

    black_hole = open('/dev/null', 'w')
    print_status = print_status_verbose if args.verbose else print_status_short

    test_to_run = []
    modes_to_run = all_modes if not args.mode else [args.mode]
    for mode in modes_to_run:
        prefix = os.path.join('build', mode, 'tests')
        for test in other_tests:
            test_to_run.append((os.path.join(prefix, test),'other'))
        for test in boost_tests:
            test_to_run.append((os.path.join(prefix, test),'boost'))

    if args.name:
        test_to_run = [t for t in test_to_run if args.name in t[0]]


    all_ok = True

    n_total = len(test_to_run)
    env = os.environ
    # disable false positive due to new (with_alignment(...)) ...
    env['ASAN_OPTIONS'] = 'alloc_dealloc_mismatch=0'
    for n, test in enumerate(test_to_run):
        path = test[0]
        prefix = '[%d/%d]' % (n + 1, n_total)
        print_status('%s RUNNING %s' % (prefix, path))
        signal.signal(signal.SIGALRM, alarm_handler)
        if args.jenkins and test[1] == 'boost':
           mode = 'release'
           if test[0].startswith(os.path.join('build','debug')):
              mode = 'debug'
           xmlout = args.jenkins+"."+mode+"."+os.path.basename(test[0])+".boost.xml"
           path = path + " --output_format=XML --log_level=all --report_level=no --log_sink=" + xmlout
           print(path)
        proc = subprocess.Popen(path.split(' '), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env,preexec_fn=os.setsid)
        signal.alarm(args.timeout)
        err = None
        out = None
        try:
            out,err = proc.communicate()
            signal.alarm(0)
        except:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.kill()
            proc.returncode = -1
        finally:
            if proc.returncode:
                print_status('FAILED: %s\n' % (path))
                if proc.returncode == -1:
                    print_status('TIMED OUT\n')
                else:
                    print_status('  with error code {code}\n'.format(code=proc.returncode))
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
