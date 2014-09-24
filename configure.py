#!/usr/bin/python3

tests = [
    'tests/test-reactor',
    'tests/fileiotest',
    'tests/virtiotest',
    'tests/l3_test',
    'tests/ip_test',
    'tests/timertest',
    'tests/tcp_test',
    ]

apps = [
    'apps/httpd/httpd',
    'apps/seastar/seastar',
    ]

all_artifacts = apps + tests

libnet = [
    'net/virtio.cc',
    'net/net.cc',
    'net/ip.cc',
    'net/ethernet.cc',
    'net/arp.cc',
    'net/stack.cc',
    'net/packet.cc',
    'net/ip_checksum.cc',
    ]

core = [
    'core/reactor.cc'
    ]

deps = {
    'apps/seastar/seastar': ['apps/seastar/main.cc'] + core,
    'tests/test-reactor': ['tests/test-reactor.cc'] + core,
    'apps/httpd/httpd': ['apps/httpd/httpd.cc'] + libnet + core,
    'tests/fileiotest': ['tests/fileiotest.cc'] + core,
    'tests/virtiotest': ['tests/virtiotest.cc'] + core + libnet,
    'tests/l3_test': ['tests/l3_test.cc'] + core + libnet,
    'tests/ip_test': ['tests/ip_test.cc'] + core + libnet,
    'tests/tcp_test': ['tests/tcp_test.cc'] + core + libnet,
    'tests/timertest': ['tests/timertest.cc'] + core,
}

modes = {
    'debug': {
        'sanitize': '-fsanitize=address -fsanitize=leak -fsanitize=undefined',
        'opt': '-O0',
        'libs': '',
    },
    'release': {
        'sanitize': '',
        'opt': '-O2',
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
arg_parser.add_argument('--mode', action='store', choices=list(modes.keys()) + ['all'], default='all')
arg_parser.add_argument('--with', dest='artifacts', action='append', choices=all_artifacts, default=[])
args = arg_parser.parse_args()
globals().update(vars(args))

build_modes = modes if args.mode == 'all' else [args.mode]
build_artifacts = all_artifacts if not args.artifacts else args.artifacts

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
    for mode in build_modes:
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
        for binary in build_artifacts:
            srcs = deps[binary]
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
