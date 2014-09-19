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

import os, os.path, textwrap, argparse, sys

configure_args = str.join(' ', sys.argv[1:])

arg_parser = argparse.ArgumentParser('Configure seastar')
arg_parser.add_argument('--static', dest = 'libstdcxx', action = 'store_const', default = '',
                        const = '-static-libstdc++',
                        help = 'Use static libstdc++ (useful for running on hosts outside the build environment')
args = arg_parser.parse_args()
globals().update(vars(args))

outdir = 'build'
buildfile = 'build.ninja'
os.makedirs(outdir, exist_ok = True)
with open(buildfile, 'w') as f:
    f.write(textwrap.dedent('''\
        configure_args = {configure_args}
        builddir = {outdir}
        cxx = g++
        cxxflags = -std=gnu++1y -g -Wall -Werror -fvisibility=hidden -pthread -I.
        ldflags = -Wl,--no-as-needed {libstdcxx}
        libs = {libs}
        ''').format(**globals()))
    for mode in ['debug', 'release']:
        modeval = modes[mode]
        f.write(textwrap.dedent('''\
            cxxflags_{mode} = {sanitize} {opt}
            libs_{mode} = {libs}
            rule cxx.{mode}
              command = $cxx -MMD -MT $out -MF $out.d $cxxflags $cxxflags_{mode} -c -o $out $in
              description = CXX $out
              depfile = $out.d
            rule link.{mode}
              command = $cxx $cxxflags $cxxflags_{mode} $ldflags -o $out $in $libs $libs_{mode}
              description = LINK $out
            ''').format(mode = mode, **modeval))
        compiles = {}
        for binary in apps.split() + tests.split():
            srcs = deps[binary].split()
            objs = ['$builddir/' + mode + '/' + src.replace('.cc', '.o') for src in srcs]
            f.write('build $builddir/{}/{}: link.{} {}\n'.format(mode, binary, mode, str.join(' ', objs)))
            for src in srcs:
                obj = '$builddir/' + mode + '/' + src.replace('.cc', '.o')
                compiles[obj] = src
        for obj in compiles:
            src = compiles[obj]
            f.write('build {}: cxx.{} {}\n'.format(obj, mode, src))
    f.write(textwrap.dedent('''\
        rule configure
          command = python3 configure.py $configure_args
          generator = 1
        build build.ninja: configure | configure.py
        '''))
