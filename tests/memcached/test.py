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
import time
import sys
import os
import argparse
import subprocess

def run(args, cmd):
    mc = subprocess.Popen([os.path.join('build', args.mode, 'apps', 'memcached', 'memcached')])
    print('Memcached started.')
    try:
        cmdline = ['tests/memcached/test_memcached.py'] + cmd
        if args.fast:
            cmdline.append('--fast')
        print('Running: ' + ' '.join(cmdline))
        subprocess.check_call(cmdline)
    finally:
        print('Killing memcached...')
        mc.terminate();
        mc.wait()
        print('Memcached killed.')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seastar test runner")
    parser.add_argument('--fast',  action="store_true", help="Run only fast tests")
    parser.add_argument('--mode', action="store", help="Test app in given mode", default='release')
    args = parser.parse_args()

    run(args, [])
    run(args, ['-U'])
