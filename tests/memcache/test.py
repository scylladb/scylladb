#!/usr/bin/env python3
import time
import sys
import os
import argparse
import subprocess

def run(args, cmd):
    mc = subprocess.Popen([os.path.join('build', args.mode, 'apps', 'memcache', 'memcache'), '--smp', '1'])
    print('Memcache started.')
    try:
        cmdline = ['tests/memcache/test_memcache.py'] + cmd
        if args.fast:
            cmdline.append('--fast')
        print('Running: ' + ' '.join(cmdline))
        subprocess.check_call(cmdline)
    finally:
        print('Killing memcache...')
        mc.kill()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seastar test runner")
    parser.add_argument('--fast',  action="store_true", help="Run only fast tests")
    parser.add_argument('--mode', action="store", help="Test app in given mode", default='release')
    args = parser.parse_args()

    run(args, [])
    run(args, ['-U'])
