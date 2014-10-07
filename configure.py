#!/usr/bin/python3

tests = [
    'tests/test-reactor',
    'tests/fileiotest',
    'tests/virtiotest',
    'tests/l3_test',
    'tests/ip_test',
    'tests/timertest',
    'tests/tcp_test',
    'tests/futures_test',
    'tests/udp_server',
    'tests/udp_client',
    ]

apps = [
    'apps/httpd/httpd',
    'apps/seastar/seastar',
    ]

all_artifacts = apps + tests

libnet = [
    'net/proxy.cc',
    'net/virtio.cc',
    'net/net.cc',
    'net/ip.cc',
    'net/ethernet.cc',
    'net/arp.cc',
    'net/stack.cc',
    'net/ip_checksum.cc',
    'net/udp.cc',
    ]

core = [
    'core/reactor.cc',
    'core/posix.cc',
    'core/memory.cc',
    'net/packet.cc',
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
    'tests/futures_test': ['tests/futures_test.cc'] + core,
    'tests/udp_server': ['tests/udp_server.cc'] + core + libnet,
    'tests/udp_client': ['tests/udp_client.cc'] + core + libnet,
}

modes = {
    'debug': {
        'sanitize': '-fsanitize=address -fsanitize=leak -fsanitize=undefined',
        'opt': '-O0 -DDEBUG -DDEFAULT_ALLOCATOR',
        'libs': '-lubsan -lasan',
    },
    'release': {
        'sanitize': '',
        'opt': '-O2',
        'libs': '',
    },
}

libs = '-laio -lboost_program_options -lboost_system -lstdc++'

import os, os.path, textwrap, argparse, sys, shlex

configure_args = str.join(' ', [shlex.quote(x) for x in sys.argv[1:]])

arg_parser = argparse.ArgumentParser('Configure seastar')
arg_parser.add_argument('--static', dest = 'static', action = 'store_const', default = '',
                        const = '-static',
                        help = 'Static link (useful for running on hosts outside the build environment')
arg_parser.add_argument('--pie', dest = 'pie', action = 'store_true',
                        help = 'Build position-independent executable (PIE)')
arg_parser.add_argument('--so', dest = 'so', action = 'store_true',
                        help = 'Build shared object (SO) instead of executable')
arg_parser.add_argument('--mode', action='store', choices=list(modes.keys()) + ['all'], default='all')
arg_parser.add_argument('--with', dest='artifacts', action='append', choices=all_artifacts, default=[])
arg_parser.add_argument('--cflags', action = 'store', dest = 'user_cflags', default = '',
                        help = 'Extra flags for the C++ compiler')
arg_parser.add_argument('--ldflags', action = 'store', dest = 'user_ldflags', default = '',
                        help = 'Extra flags for the linker')
arg_parser.add_argument('--compiler', action = 'store', dest = 'cxx', default = 'g++',
                        help = 'C++ compiler path')
args = arg_parser.parse_args()

if args.so:
    args.pie = '-shared'
    args.fpie = '-fpic'
elif args.pie:
    args.pie = '-pie'
    args.fpie = '-fpie'
else:
    args.pie = ''
    args.fpie = ''

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
        cxx = {cxx}
        cxxflags = -std=gnu++1y -g {fpie} -Wall -Werror -fvisibility=hidden -pthread -I. {user_cflags}
        ldflags = -g -Wl,--no-as-needed {static} {pie} -fvisibility=hidden -pthread {user_ldflags}
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
              command = $cxx  $cxxflags_{mode} $ldflags -o $out $in $libs $libs_{mode}
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
