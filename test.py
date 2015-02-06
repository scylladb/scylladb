#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess

all_tests = [
    'futures_test',
    'smp_test',
    'memcached/test_ascii_parser',
    'sstring_test',
    'output_stream_test',
]

last_len = 0

def print_status(msg):
    global last_len
    print('\r' + ' '*last_len, end='')
    last_len = len(msg)
    print('\r' + msg, end='')

if __name__ == "__main__":
    all_modes = ['debug', 'release']

    parser = argparse.ArgumentParser(description="Seastar test runner")
    parser.add_argument('--fast',  action="store_true", help="Run only fast tests")
    parser.add_argument('--name',  action="store", help="Run only test whose name contains given string")
    parser.add_argument('--mode', choices=all_modes, help="Run only tests for given build mode")
    args = parser.parse_args()

    black_hole = open('/dev/null', 'w')

    test_to_run = []
    modes_to_run = all_modes if not args.mode else [args.mode]
    for mode in modes_to_run:
        prefix = os.path.join('build', mode, 'tests')
        for test in all_tests:
            test_to_run.append(os.path.join(prefix, test))
        test_to_run.append('tests/memcached/test.py --mode ' + mode + (' --fast' if args.fast else ''))

        allocator_test_path = os.path.join(prefix, 'allocator_test')
        if args.fast:
            if mode == 'debug':
                test_to_run.append(allocator_test_path + ' --iterations 5')
            else:
                test_to_run.append(allocator_test_path + ' --time 0.1')
        else:
            test_to_run.append(allocator_test_path)

    if args.name:
        test_to_run = [t for t in test_to_run if args.name in t]

    all_ok = True

    n_total = len(test_to_run)
    env = os.environ
    # disable false positive due to new (with_alignment(...)) ...
    env['ASAN_OPTIONS'] = 'alloc_dealloc_mismatch=0'
    for n, path in enumerate(test_to_run):
        prefix = '[%d/%d]' % (n + 1, n_total)
        print_status('%s RUNNING %s' % (prefix, path))
        proc = subprocess.Popen(path.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        out, err = proc.communicate()
        if proc.returncode:
            print_status('FAILED: %s\n' % (path))
            if out:
                print('=== stdout START ===')
                print(out.decode())
                print('=== stdout END ===')
            if err:
                print('=== stderr START ===')
                print(err.decode())
                print('=== stderr END ===')
            all_ok = False
        else:
            print_status('%s PASSED %s' % (prefix, path))

    if all_ok:
        print('\nOK.')
    else:
        print_status('')
        sys.exit(1)
