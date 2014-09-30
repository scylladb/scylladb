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
    'apps/httpd/httpd': ['apps/httpd/httpd.cc', 'apps/httpd/request_parser.rl'] + libnet + core,
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
        'libs': '-lubsan -lasan',
    },
    'release': {
        'sanitize': '',
        'opt': '-O2',
        'libs': '-ltcmalloc',
    },
}

libs = '-laio -lboost_program_options -lboost_system -lstdc++'

import os, os.path, textwrap, argparse, sys

configure_args = str.join(' ', sys.argv[1:])

arg_parser = argparse.ArgumentParser('Configure seastar')
arg_parser.add_argument('--static', dest = 'static', action = 'store_const', default = '',
                        const = '-static',
                        help = 'Static link (useful for running on hosts outside the build environment')
arg_parser.add_argument('--mode', action='store', choices=list(modes.keys()) + ['all'], default='all')
arg_parser.add_argument('--with', dest='artifacts', action='append', choices=all_artifacts, default=[])
arg_parser.add_argument('--cflags', action = 'store', dest = 'user_cflags', default = '',
                        help = 'Extra flags for the C++ compiler')
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
        cxxflags = -std=gnu++1y -g -Wall -Werror -fvisibility=hidden -pthread -I. {user_cflags}
        ldflags = -Wl,--no-as-needed {static}
        libs = {libs}
        rule ragel
            command = ragel -G2 -o $out $in
            description = RAGEL $out
        ''').format(**globals()))
    for mode in build_modes:
        modeval = modes[mode]
        f.write(textwrap.dedent('''\
            cxxflags_{mode} = {sanitize} {opt} -I $builddir/{mode}/gen
            libs_{mode} = {libs}
            rule cxx.{mode}
              command = $cxx -MMD -MT $out -MF $out.d $cxxflags $cxxflags_{mode} -c -o $out $in
              description = CXX $out
              depfile = $out.d
            rule link.{mode}
              command = $cxx $cxxflags $cxxflags_{mode} $ldflags -o $out $in $libs $libs_{mode}
              description = LINK $out
            ''').format(mode = mode, **modeval))
        f.write('build {mode}: phony {artifacts}\n'.format(mode = mode,
            artifacts = str.join(' ', ('$builddir/' + mode + '/' + x for x in build_artifacts))))
        compiles = {}
        ragels = {}
        for binary in build_artifacts:
            srcs = deps[binary]
            objs = ['$builddir/' + mode + '/' + src.replace('.cc', '.o')
                    for src in srcs
                    if src.endswith('.cc')]
            f.write('build $builddir/{}/{}: link.{} {}\n'.format(mode, binary, mode, str.join(' ', objs)))
            for src in srcs:
                if src.endswith('.cc'):
                    obj = '$builddir/' + mode + '/' + src.replace('.cc', '.o')
                    compiles[obj] = src
                elif src.endswith('.rl'):
                    hh = '$builddir/' + mode + '/gen/' + src.replace('.rl', '.hh')
                    ragels[hh] = src
                else:
                    raise Exeception('No rule for ' + src)
        for obj in compiles:
            src = compiles[obj]
            gen_headers = ragels.keys()
            f.write('build {}: cxx.{} {} || {} \n'.format(obj, mode, src, ' '.join(gen_headers)))
        for hh in ragels:
            src = ragels[hh]
            f.write('build {}: ragel {}\n'.format(hh, src))
    f.write(textwrap.dedent('''\
        rule configure
          command = python3 configure.py $configure_args
          generator = 1
        build build.ninja: configure | configure.py
        rule cscope
            command = find -name '*.[chS]' -o -name "*.cc" -o -name "*.hh" | cscope -bq -i-
            description = CSCOPE
        build cscope: cscope
        default {modes_list}
        ''').format(modes_list = ' '.join(build_modes), **globals()))
