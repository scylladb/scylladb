#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess

all_tests = [
    'futures_test',
    'memcached/test_ascii_parser',
    'sstring_test',
]

last_len = 0

def print_status(msg):
    global last_len
    print('\r' + ' '*last_len, end='')
    last_len = len(msg)
    print('\r' + msg, end='')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seastar test runner")
    parser.add_argument('--fast',  action="store_true", help="Run only fast tests")
    args = parser.parse_args()

    black_hole = open('/dev/null', 'w')

    test_to_run = []
    for mode in ['debug', 'release']:
        for test in all_tests:
            test_to_run.append(os.path.join('build', mode, 'tests', test))
        test_to_run.append('tests/memcached/test.py --mode ' + mode + (' --fast' if args.fast else ''))

    all_ok = True

    n_total = len(test_to_run)
    for n, path in enumerate(test_to_run):
        prefix = '[%d/%d]' % (n + 1, n_total)
        print_status('%s RUNNING %s' % (prefix, path))
        proc = subprocess.Popen(path.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
