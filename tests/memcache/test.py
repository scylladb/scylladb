#!/usr/bin/env python3
import time
import subprocess
import sys

if len(sys.argv) < 2:
    print('Usage: %s <path-to-memcache> ...' % sys.argv[0])

memcache_path = sys.argv[1]

def run(cmd):
    mc = subprocess.Popen([memcache_path] + sys.argv[2:])
    print('Memcache started.')
    try:
        time.sleep(0.1)
        cmdline = ['tests/memcache/test_memcache.py'] + cmd
        print('Running: ' + ' '.join(cmdline))
        subprocess.check_call(cmdline)
    finally:
        print('Killing memcache...')
        mc.kill()

run([])
run(['-U'])
