#!/usr/bin/env python3
import time
import subprocess
import sys

if len(sys.argv) < 2:
    print('Usage: %s <path-to-memcache>' % sys.argv[0])

memcache_path = sys.argv[1]

mc = subprocess.Popen([memcache_path])
print('Memcache started.')
try:
    time.sleep(0.1)
    print('Starting tests...')
    subprocess.check_call(['tests/memcache/test_memcache.py'])
finally:
    print('Killing memcache...')
    mc.kill()
