#!/usr/bin/env python3
#
# This file is open source software, licensed to you under the terms
# of the Apache License, Version 2.0 (the "License").  See the NOTICE file
# distributed with this work for additional information regarding copyright
# ownership.  You may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import os
import sys
import argparse
import subprocess
import signal

all_tests = [
    'futures_test',
    'smp_test',
    'memcached/test_ascii_parser',
    'sstring_test',
    'output_stream_test',
    'httpd',
]

last_len = 0

def print_status(msg):
    global last_len
    print('\r' + ' '*last_len, end='')
    last_len = len(msg)
    print('\r' + msg, end='')

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
        signal.signal(signal.SIGALRM, alarm_handler)
        proc = subprocess.Popen(path.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,preexec_fn=os.setsid)
        signal.alarm(args.timeout)
        try:
            out, err = proc.communicate()
            signal.alarm(0)
        except:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.kill()
            proc.returncode = -1
        if proc.returncode:
            print_status('FAILED: %s\n' % (path))
            if proc.returncode == -1:
                print_status('TIMED OUT\n')
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
