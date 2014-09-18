#!/usr/bin/python3

tests = 'tests/test-reactor tests/fileiotest tests/virtiotest tests/l3_test'
tests += ' tests/ip_test tests/timertest tests/tcp_test'

apps = 'apps/httpd/httpd apps/seastar/seastar'

libnet = 'net/virtio.cc net/net.cc net/ip.cc net/ethernet.cc net/arp.cc'
libnet += ' net/stack.cc net/packet.cc net/ip_checksum.cc'

deps = {
    'apps/seastar/seastar': 'apps/seastar/main.cc core/reactor.cc',
    'tests/test-reactor': 'tests/test-reactor.cc core/reactor.cc',
    'apps/httpd/httpd': 'apps/httpd/httpd.cc core/reactor.cc ' + libnet,
    'tests/fileiotest': 'tests/fileiotest.cc core/reactor.cc',
    'tests/virtiotest': ('tests/virtiotest.cc net/virtio.cc core/reactor.cc'
                        + ' net/net.cc net/ip.cc net/ethernet.cc net/arp.cc net/packet.cc'
                        + ' net/ip_checksum.cc'),
    'tests/l3_test': ('tests/l3_test.cc net/virtio.cc core/reactor.cc net/net.cc'
                      + ' net/ip.cc net/ethernet.cc net/arp.cc'
                      + ' net/packet.cc net/ip_checksum.cc'),
    'tests/ip_test': ('tests/ip_test.cc net/virtio.cc core/reactor.cc net/net.cc'
                      + ' net/ip.cc net/arp.cc net/ethernet.cc net/packet.cc'
                      + ' net/ip_checksum.cc'),
    'tests/tcp_test': ('tests/tcp_test.cc net/virtio.cc core/reactor.cc net/net.cc'
                       + ' net/ip.cc net/arp.cc net/ethernet.cc net/packet.cc'
                       + ' net/ip_checksum.cc'),
    'tests/timertest': 'tests/timertest.cc core/reactor.cc',
}

modes = {
    'debug': {
        'sanitize': '-fsanitize=address -fsanitize=leak -fsanitize=undefined',
        'opt': '-O0',
        'libs': '',
    },
    'release': {
        'sanitize': '',
        'opt': '-O2 -flto',
        'libs': '-ltcmalloc',
    },
}

libs = '-laio -lboost_program_options -lboost_system'

import os, os.path

for mode in ['debug', 'release']:
    outdir = 'build/' + mode
    buildfile = outdir + '/build.ninja'
    os.makedirs(outdir, exist_ok = True)
    with open(buildfile, 'w') as f:
        modeval = modes[mode]
        f.write('builddir = {}\n'.format(outdir))
        f.write('cxx = g++\n')
        f.write('cxxflags = -std=gnu++1y -g -Wall -Werror {} {} -fvisibility=hidden -pthread -I.\n'.format(
              modeval['sanitize'], modeval['opt']))
        f.write('ldflags = -Wl,--no-as-needed\n')
        f.write('libs = {} {}\n'.format(libs, modeval['libs']))
        f.write('rule cxx\n')
        f.write('  command = $cxx -MMD -MT $out -MF $out.d $cxxflags -c -o $out $in\n')
        f.write('  description = CXX $out\n')
        f.write('  depfile = $out.d\n')
        f.write('rule link\n')
        f.write('  command = $cxx $cxxflags $ldflags -o $out $in $libs\n')
        f.write('  description = LINK $out\n')
        compiles = {}
        for binary in apps.split() + tests.split():
            srcs = deps[binary].split()
            objs = ['$builddir/' + src.replace('.cc', '.o') for src in srcs]
            f.write('build $builddir/{}: link {}\n'.format(binary, str.join(' ', objs)))
            for src in srcs:
                obj = '$builddir/' + src.replace('.cc', '.o')
                compiles[obj] = src
        for obj in compiles:
            src = compiles[obj]
            f.write('build {}: cxx {}\n'.format(obj, src))
        f.write('rule configure\n')
        f.write('  command = python3 configure.py\n')
        f.write('  generator = 1\n')
        f.write('build $builddir/build.ninja: configure | configure.py\n')
