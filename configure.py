#!/usr/bin/python3
import os, os.path, textwrap, argparse, sys, shlex, subprocess, tempfile, re
from distutils.spawn import find_executable

configure_args = str.join(' ', [shlex.quote(x) for x in sys.argv[1:]])

def get_flags():
    with open('/proc/cpuinfo') as f:
        for line in f:
            if line.strip():
                if line.rstrip('\n').startswith('flags'):
                    return re.sub(r'^flags\s+: ', '', line).split()

def add_tristate(arg_parser, name, dest, help):
    arg_parser.add_argument('--enable-' + name, dest = dest, action = 'store_true', default = None,
                            help = 'Enable ' + help)
    arg_parser.add_argument('--disable-' + name, dest = dest, action = 'store_false', default = None,
                            help = 'Disable ' + help)

def apply_tristate(var, test, note, missing):
    if (var is None) or var:
        if test():
            return True
        elif var == True:
            print(missing)
            sys.exit(1)
        else:
            print(note)
            return False
    return False

def pkg_config(option, package):
    output = subprocess.check_output(['pkg-config', option, package])
    return output.decode('utf-8').strip()

def try_compile(compiler, source = '', flags = []):
    with tempfile.NamedTemporaryFile() as sfile:
        sfile.file.write(bytes(source, 'utf-8'))
        sfile.file.flush()
        return subprocess.call([compiler, '-x', 'c++', '-o', '/dev/null', '-c', sfile.name] + flags,
                               stdout = subprocess.DEVNULL,
                               stderr = subprocess.DEVNULL) == 0

def warning_supported(warning, compiler):
    # gcc ignores -Wno-x even if it is not supported
    adjusted = re.sub('^-Wno-', '-W', warning)
    return try_compile(flags = [adjusted], compiler = compiler)

def debug_flag(compiler):
    src_with_auto = textwrap.dedent('''\
        template <typename T>
        struct x { auto f() {} };

        x<int> a;
        ''')
    if try_compile(source = src_with_auto, flags = ['-g', '-std=gnu++1y'], compiler = compiler):
        return '-g'
    else:
        print('Note: debug information disabled; upgrade your compiler')
        return ''

class Thrift(object):
    def __init__(self, source, service):
        self.source = source
        self.service = service
    def generated(self, gen_dir):
        basename = os.path.splitext(os.path.basename(self.source))[0]
        files = [basename + '_' + ext
                 for ext in ['types.cpp', 'types.h', 'constants.cpp', 'constants.h']]
        files += [self.service + ext
                  for ext in ['.cpp', '.h']]
        return [os.path.join(gen_dir, file) for file in files]
    def headers(self, gen_dir):
        return [x for x in self.generated(gen_dir) if x.endswith('.h')]
    def sources(self, gen_dir):
        return [x for x in self.generated(gen_dir) if x.endswith('.cpp')]
    def objects(self, gen_dir):
        return [x.replace('.cpp', '.o') for x in self.sources(gen_dir)]
    def endswith(self, end):
        return self.source.endswith(end)

class Antlr3Grammar(object):
    def __init__(self, source):
        self.source = source
    def generated(self, gen_dir):
        basename = os.path.splitext(self.source)[0]
        files = [basename + ext
                 for ext in ['Lexer.cpp', 'Lexer.hpp', 'Parser.cpp', 'Parser.hpp']]
        return [os.path.join(gen_dir, file) for file in files]
    def headers(self, gen_dir):
        return [x for x in self.generated(gen_dir) if x.endswith('.hpp')]
    def sources(self, gen_dir):
        return [x for x in self.generated(gen_dir) if x.endswith('.cpp')]
    def objects(self, gen_dir):
        return [x.replace('.cpp', '.o') for x in self.sources(gen_dir)]
    def endswith(self, end):
        return self.source.endswith(end)

modes = {
    'debug': {
        'sanitize': '-fsanitize=address -fsanitize=leak -fsanitize=undefined',
        'sanitize_libs': '-lubsan -lasan',
        'opt': '-O0 -DDEBUG -DDEBUG_SHARED_PTR -DDEFAULT_ALLOCATOR',
        'libs': '',
    },
    'release': {
        'sanitize': '',
        'sanitize_libs': '',
        'opt': '-O2',
        'libs': '',
    },
}

urchin_tests = [
    'tests/mutation_test',
    'tests/range_test',
    'tests/types_test',
    'tests/keys_test',
    'tests/partitioner_test',
    'tests/frozen_mutation_test',
    'tests/perf/perf_mutation',
    'tests/perf/perf_hash',
    'tests/perf/perf_cql_parser',
    'tests/perf/perf_simple_query',
    'tests/perf/perf_sstable',
    'tests/cql_query_test',
    'tests/storage_proxy_test',
    'tests/mutation_reader_test',
    'tests/mutation_query_test',
    'tests/row_cache_test',
    'tests/test-serialization',
    'tests/sstable_test',
    'tests/sstable_mutation_test',
    'tests/memtable_test',
    'tests/commitlog_test',
    'tests/cartesian_product_test',
    'tests/hash_test',
    'tests/serializer_test',
    'tests/map_difference_test',
    'tests/message',
    'tests/gossip',
    'tests/gossip_test',
    'tests/compound_test',
    'tests/config_test',
    'tests/gossiping_property_file_snitch_test',
    'tests/snitch_reset_test',
    'tests/network_topology_strategy_test',
    'tests/query_processor_test',
    'tests/batchlog_manager_test',
    'tests/bytes_ostream_test',
    'tests/UUID_test',
    'tests/murmur_hash_test',
    'tests/allocation_strategy_test',
    'tests/logalloc_test',
    'tests/managed_vector_test',
    'tests/crc_test',
]

apps = [
    'scylla',
    ]

tests = urchin_tests

all_artifacts = apps + tests

arg_parser = argparse.ArgumentParser('Configure scylla')
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
arg_parser.add_argument('--with-osv', action = 'store', dest = 'with_osv', default = '',
                        help = 'Shortcut for compile for OSv')
arg_parser.add_argument('--enable-dpdk', action = 'store_true', dest = 'dpdk', default = False,
                        help = 'Enable dpdk (from seastar dpdk sources)')
arg_parser.add_argument('--dpdk-target', action = 'store', dest = 'dpdk_target', default = '',
                        help = 'Path to DPDK SDK target location (e.g. <DPDK SDK dir>/x86_64-native-linuxapp-gcc)')
arg_parser.add_argument('--debuginfo', action = 'store', dest = 'debuginfo', type = int, default = 1,
                        help = 'Enable(1)/disable(0)compiler debug information generation')
add_tristate(arg_parser, name = 'hwloc', dest = 'hwloc', help = 'hwloc support')
add_tristate(arg_parser, name = 'xen', dest = 'xen', help = 'Xen support')
args = arg_parser.parse_args()

defines = []
urchin_libs = '-llz4 -lsnappy -lz -lboost_thread -lcryptopp -lrt -lyaml-cpp -lboost_date_time'

cassandra_interface = Thrift(source = 'interface/cassandra.thrift', service = 'Cassandra')

urchin_core = (['database.cc',
                 'schema.cc',
                 'bytes.cc',
                 'mutation.cc',
                 'row_cache.cc',
                 'frozen_mutation.cc',
                 'memtable.cc',
                 'utils/logalloc.cc',
                 'utils/large_bitset.cc',
                 'mutation_partition.cc',
                 'mutation_partition_view.cc',
                 'mutation_partition_serializer.cc',
                 'mutation_reader.cc',
                 'mutation_query.cc',
                 'keys.cc',
                 'sstables/sstables.cc',
                 'sstables/compress.cc',
                 'sstables/row.cc',
                 'sstables/key.cc',
                 'sstables/partition.cc',
                 'sstables/filter.cc',
                 'sstables/compaction.cc',
                 'log.cc',
                 'transport/event.cc',
                 'transport/event_notifier.cc',
                 'transport/server.cc',
                 'cql3/abstract_marker.cc',
                 'cql3/attributes.cc',
                 'cql3/cf_name.cc',
                 'cql3/cql3_type.cc',
                 'cql3/operation.cc',
                 'cql3/index_name.cc',
                 'cql3/keyspace_element_name.cc',
                 'cql3/lists.cc',
                 'cql3/sets.cc',
                 'cql3/maps.cc',
                 'cql3/functions/functions.cc',
                 'cql3/statements/cf_prop_defs.cc',
                 'cql3/statements/create_table_statement.cc',
                 'cql3/statements/drop_keyspace_statement.cc',
                 'cql3/statements/drop_table_statement.cc',
                 'cql3/statements/schema_altering_statement.cc',
                 'cql3/statements/ks_prop_defs.cc',
                 'cql3/statements/modification_statement.cc',
                 'cql3/statements/update_statement.cc',
                 'cql3/statements/delete_statement.cc',
                 'cql3/statements/batch_statement.cc',
                 'cql3/statements/select_statement.cc',
                 'cql3/statements/use_statement.cc',
                 'cql3/statements/index_prop_defs.cc',
                 'cql3/statements/index_target.cc',
                 'cql3/statements/create_index_statement.cc',
                 'cql3/update_parameters.cc',
                 'cql3/ut_name.cc',
                 'thrift/handler.cc',
                 'thrift/server.cc',
                 'thrift/thrift_validation.cc',
                 'utils/runtime.cc',
                 'utils/murmur_hash.cc',
                 'utils/uuid.cc',
                 'types.cc',
                 'validation.cc',
                 'service/migration_manager.cc',
                 'service/storage_proxy.cc',
                 'cql3/operator.cc',
                 'cql3/relation.cc',
                 'cql3/column_identifier.cc',
                 'cql3/constants.cc',
                 'cql3/query_processor.cc',
                 'cql3/query_options.cc',
                 'cql3/single_column_relation.cc',
                 'cql3/token_relation.cc',
                 'cql3/column_condition.cc',
                 'cql3/user_types.cc',
                 'cql3/untyped_result_set.cc',
                 'cql3/selection/abstract_function_selector.cc',
                 'cql3/selection/simple_selector.cc',
                 'cql3/selection/selectable.cc',
                 'cql3/selection/selector_factories.cc',
                 'cql3/selection/selection.cc',
                 'cql3/selection/selector.cc',
                 'cql3/restrictions/statement_restrictions.cc',
                 'db/consistency_level.cc',
                 'db/system_keyspace.cc',
                 'db/schema_tables.cc',
                 'db/commitlog/commitlog.cc',
                 'db/commitlog/commitlog_replayer.cc',
                 'db/serializer.cc',
                 'db/config.cc',
                 'db/index/secondary_index.cc',
                 'db/marshal/type_parser.cc',
                 'db/batchlog_manager.cc',
                 'io/io.cc',
                 'utils/utils.cc',
                 'utils/UUID_gen.cc',
                 'utils/i_filter.cc',
                 'utils/bloom_filter.cc',
                 'utils/bloom_calculations.cc',
                 'utils/rate_limiter.cc',
                 'utils/compaction_manager.cc',
                 'gms/version_generator.cc',
                 'gms/versioned_value.cc',
                 'gms/gossiper.cc',
                 'gms/failure_detector.cc',
                 'gms/gossip_digest_syn.cc',
                 'gms/gossip_digest_ack.cc',
                 'gms/gossip_digest_ack2.cc',
                 'gms/endpoint_state.cc',
                 'dht/i_partitioner.cc',
                 'dht/murmur3_partitioner.cc',
                 'dht/byte_ordered_partitioner.cc',
                 'dht/boot_strapper.cc',
                 'unimplemented.cc',
                 'query.cc',
                 'query-result-set.cc',
                 'locator/abstract_replication_strategy.cc',
                 'locator/simple_strategy.cc',
                 'locator/local_strategy.cc',
                 'locator/network_topology_strategy.cc',
                 'locator/token_metadata.cc',
                 'locator/locator.cc',
                 'locator/snitch_base.cc',
                 'locator/simple_snitch.cc',
                 'locator/rack_inferring_snitch.cc',
                 'locator/gossiping_property_file_snitch.cc',
                 'message/messaging_service.cc',
                 'service/migration_task.cc',
                 'service/storage_service.cc',
                 'streaming/streaming.cc',
                 'streaming/stream_task.cc',
                 'streaming/stream_session.cc',
                 'streaming/stream_request.cc',
                 'streaming/stream_summary.cc',
                 'streaming/stream_transfer_task.cc',
                 'streaming/stream_receive_task.cc',
                 'streaming/stream_plan.cc',
                 'streaming/progress_info.cc',
                 'streaming/session_info.cc',
                 'streaming/stream_coordinator.cc',
                 'streaming/stream_manager.cc',
                 'streaming/stream_result_future.cc',
                 'streaming/messages/stream_init_message.cc',
                 'streaming/messages/retry_message.cc',
                 'streaming/messages/received_message.cc',
                 'streaming/messages/prepare_message.cc',
                 'streaming/messages/file_message_header.cc',
                 'streaming/messages/outgoing_file_message.cc',
                 'streaming/messages/incoming_file_message.cc',
                 'gc_clock.cc',
                 'partition_slice_builder.cc',
                 'init.cc',
                 'repair/repair.cc',
                 ]
                + [Antlr3Grammar('cql3/Cql.g')]
                + [Thrift('interface/cassandra.thrift', 'Cassandra')]
                )

api = ['api/api.cc',
       'api/api-doc/storage_service.json',
       'api/api-doc/lsa.json',
       'api/storage_service.cc',
       'api/api-doc/commitlog.json',
       'api/commitlog.cc',
       'api/api-doc/gossiper.json',
       'api/gossiper.cc',
       'api/api-doc/failure_detector.json',
       'api/failure_detector.cc',
       'api/api-doc/column_family.json',
       'api/column_family.cc',
       'api/messaging_service.cc',
       'api/api-doc/messaging_service.json',
       'api/api-doc/storage_proxy.json',
       'api/storage_proxy.cc',
       'api/api-doc/cache_service.json',
       'api/cache_service.cc',
       'api/api-doc/collectd.json',
       'api/collectd.cc',
       'api/api-doc/endpoint_snitch_info.json',
       'api/endpoint_snitch.cc',
       'api/api-doc/compaction_manager.json',
       'api/compaction_manager.cc',
       'api/api-doc/hinted_handoff.json',
       'api/hinted_handoff.cc',
       'api/api-doc/utils.json',
       'api/lsa.cc',
       ]

urchin_tests_dependencies = urchin_core + [
    'tests/cql_test_env.cc',
    'tests/cql_assertions.cc',
    'tests/result_set_assertions.cc',
    'tests/mutation_source_test.cc',
]

urchin_tests_seastar_deps = [
    'seastar/tests/test-utils.cc',
    'seastar/tests/test_runner.cc',
]

deps = {
    'scylla': ['main.cc'] + urchin_core + api,
}

for t in urchin_tests:
    deps[t] = urchin_tests_dependencies + [t + '.cc']
    if 'types_test' not in t and 'keys_test' not in t and 'partitioner_test' not in t and 'map_difference_test' not in t and 'frozen_mutation_test' not in t and 'perf_mutation' not in t and 'cartesian_product_test' not in t and 'perf_hash' not in t and 'perf_cql_parser' not in t and 'message' not in t and 'perf_simple_query' not in t and 'serialization' not in t and t != 'tests/gossip' and 'compound_test' not in t and 'range_test' not in t and 'crc_test' not in t and 'perf_sstable' not in t and 'managed_vector_test' not in t:
        deps[t] += urchin_tests_seastar_deps

deps['tests/sstable_test'] += ['tests/sstable_datafile_test.cc']

deps['tests/bytes_ostream_test'] = ['tests/bytes_ostream_test.cc']
deps['tests/UUID_test'] = ['utils/UUID_gen.cc', 'tests/UUID_test.cc']
deps['tests/murmur_hash_test'] = ['bytes.cc', 'utils/murmur_hash.cc', 'tests/murmur_hash_test.cc']
deps['tests/allocation_strategy_test'] = ['tests/allocation_strategy_test.cc', 'utils/logalloc.cc', 'log.cc']

warnings = [
    '-Wno-mismatched-tags',  # clang-only
    '-Wno-maybe-uninitialized', # false positives on gcc 5
    ]

warnings = [w
            for w in warnings
            if warning_supported(warning = w, compiler = args.cxx)]

warnings = ' '.join(warnings)

dbgflag = debug_flag(args.cxx) if args.debuginfo else ''

if args.so:
    args.pie = '-shared'
    args.fpie = '-fpic'
elif args.pie:
    args.pie = '-pie'
    args.fpie = '-fpie'
else:
    args.pie = ''
    args.fpie = ''

defines = ' '.join(['-D' + d for d in defines])

globals().update(vars(args))

total_memory = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
link_pool_depth = max(int(total_memory / 7e9), 1)

build_modes = modes if args.mode == 'all' else [args.mode]
build_artifacts = all_artifacts if not args.artifacts else args.artifacts

seastar_flags = ['--disable-xen']
if args.dpdk:
    # fake dependencies on dpdk, so that it is built before anything else
    seastar_flags += ['--enable-dpdk']
elif args.dpdk_target:
    seastar_flags += ['--dpdk-target', args.dpdk_target]

seastar_flags += ['--compiler', args.cxx]

status = subprocess.call(['./configure.py'] + seastar_flags, cwd = 'seastar')

if status != 0:
    print('Seastar configuration failed')
    sys.exit(1)


pc = { mode : 'build/{}/seastar.pc'.format(mode) for mode in build_modes }
ninja = find_executable('ninja') or find_executable('ninja-build')
if not ninja:
    print('Ninja executable (ninja or ninja-build) not found on PATH\n')
    sys.exit(1)
status = subprocess.call([ninja] + list(pc.values()), cwd = 'seastar')
if status:
    print('Failed to generate {}\n'.format(pc))
    sys.exit(1)

for mode in build_modes:
    cfg =  dict([line.strip().split(': ', 1)
                 for line in open('seastar/' + pc[mode])
                 if ': ' in line])
    modes[mode]['seastar_cflags'] = cfg['Cflags']
    modes[mode]['seastar_libs'] = cfg['Libs']

seastar_deps = 'practically_anything_can_change_so_lets_run_it_every_time_and_restat.'

args.user_cflags += " " + pkg_config("--cflags", "jsoncpp")
args.user_cflags = '-march=nehalem ' + args.user_cflags
libs = "-lyaml-cpp -llz4 -lz -lsnappy " + pkg_config("--libs", "jsoncpp") + ' -lboost_filesystem'
user_cflags = args.user_cflags

outdir = 'build'
buildfile = 'build.ninja'
os.makedirs(outdir, exist_ok = True)
do_sanitize = True
if args.static:
    do_sanitize = False
with open(buildfile, 'w') as f:
    f.write(textwrap.dedent('''\
        configure_args = {configure_args}
        builddir = {outdir}
        cxx = {cxx}
        cxxflags = {user_cflags} {warnings} {defines}
        ldflags = {user_ldflags}
        libs = {libs}
        pool link_pool
            depth = {link_pool_depth}
        rule ragel
            command = ragel -G2 -o $out $in
            description = RAGEL $out
        rule gen
            command = echo -e $text > $out
            description = GEN $out
        rule swagger
            command = seastar/json/json2code.py -f $in -o $out
            description = SWAGGER $out
        rule ninja
            command = {ninja} -C $subdir $target
            restat = 1
            description = NINJA $out
        ''').format(**globals()))
    for mode in build_modes:
        modeval = modes[mode]
        f.write(textwrap.dedent('''\
            cxxflags_{mode} = -I. -I $builddir/{mode}/gen -I seastar -I seastar/build/{mode}/gen
            rule cxx.{mode}
              command = $cxx -MMD -MT $out -MF $out.d {seastar_cflags} $cxxflags $cxxflags_{mode} -c -o $out $in
              description = CXX $out
              depfile = $out.d
            rule link.{mode}
              command = $cxx  $cxxflags_{mode} $ldflags {seastar_libs} -o $out $in $libs $libs_{mode}
              description = LINK $out
              pool = link_pool
            rule link_stripped.{mode}
              command = $cxx  $cxxflags_{mode} -s $ldflags {seastar_libs} -o $out $in $libs $libs_{mode}
              description = LINK (stripped) $out
              pool = link_pool
            rule ar.{mode}
              command = rm -f $out; ar cr $out $in; ranlib $out
              description = AR $out
            rule thrift.{mode}
                command = thrift -gen cpp:cob_style -out $builddir/{mode}/gen $in
                description = THRIFT $in
            rule antlr3.{mode}
                command = sed -e '/^#if 0/,/^#endif/d' $in > $builddir/{mode}/gen/$in && antlr3 $builddir/{mode}/gen/$in && sed -i 's/^\\( *\)\\(ImplTraits::CommonTokenType\\* [a-zA-Z0-9_]* = NULL;\\)$$/\\1const \\2/' build/{mode}/gen/${{stem}}Parser.cpp
                description = ANTLR3 $in
            ''').format(mode = mode, **modeval))
        f.write('build {mode}: phony {artifacts}\n'.format(mode = mode,
            artifacts = str.join(' ', ('$builddir/' + mode + '/' + x for x in build_artifacts))))
        compiles = {}
        ragels = {}
        swaggers = {}
        thrifts = set()
        antlr3_grammars = set()
        for binary in build_artifacts:
            srcs = deps[binary]
            objs = ['$builddir/' + mode + '/' + src.replace('.cc', '.o')
                    for src in srcs
                    if src.endswith('.cc')]
            has_thrift = False
            for dep in deps[binary]:
                if isinstance(dep, Thrift):
                    has_thrift = True
                    objs += dep.objects('$builddir/' + mode + '/gen')
                if isinstance(dep, Antlr3Grammar):
                    objs += dep.objects('$builddir/' + mode + '/gen')
            if binary.endswith('.pc'):
                vars = modeval.copy()
                vars.update(globals())
                pc = textwrap.dedent('''\
                        Name: Seastar
                        URL: http://seastar-project.org/
                        Description: Advanced C++ framework for high-performance server applications on modern hardware.
                        Version: 1.0
                        Libs: -L{srcdir}/{builddir} -Wl,--whole-archive -lseastar -Wl,--no-whole-archive {dbgflag} -Wl,--no-as-needed {static} {pie} -fvisibility=hidden -pthread {user_ldflags} {libs} {sanitize_libs}
                        Cflags: -std=gnu++1y {dbgflag} {fpie} -Wall -Werror -fvisibility=hidden -pthread -I{srcdir} -I{srcdir}/{builddir}/gen {user_cflags} {warnings} {defines} {sanitize} {opt}
                        ''').format(builddir = 'build/' + mode, srcdir = os.getcwd(), **vars)
                f.write('build $builddir/{}/{}: gen\n  text = {}\n'.format(mode, binary, repr(pc)))
            elif binary.endswith('.a'):
                f.write('build $builddir/{}/{}: ar.{} {}\n'.format(mode, binary, mode, str.join(' ', objs)))
            else:
                if binary.startswith('tests/'):
                    # Our code's debugging information is huge, and multiplied
                    # by many tests yields ridiculous amounts of disk space.
                    # So we strip the tests by default; The user can very
                    # quickly re-link the test unstripped by adding a "_g"
                    # to the test name, e.g., "ninja build/release/testname_g"
                    f.write('build $builddir/{}/{}: link_stripped.{} {} {}\n'.format(mode, binary, mode, str.join(' ', objs),
                                                                                     'seastar/build/{}/libseastar.a'.format(mode)))
                    if has_thrift:
                        f.write('   libs =  -lthrift -lboost_system $libs\n')
                    f.write('build $builddir/{}/{}_g: link.{} {} {}\n'.format(mode, binary, mode, str.join(' ', objs),
                                                                              'seastar/build/{}/libseastar.a'.format(mode)))
                else:
                    f.write('build $builddir/{}/{}: link.{} {} {}\n'.format(mode, binary, mode, str.join(' ', objs),
                                                                            'seastar/build/{}/libseastar.a'.format(mode)))
                if has_thrift:
                    f.write('   libs =  -lthrift -lboost_system $libs\n')
            for src in srcs:
                if src.endswith('.cc'):
                    obj = '$builddir/' + mode + '/' + src.replace('.cc', '.o')
                    compiles[obj] = src
                elif src.endswith('.rl'):
                    hh = '$builddir/' + mode + '/gen/' + src.replace('.rl', '.hh')
                    ragels[hh] = src
                elif src.endswith('.json'):
                    hh = '$builddir/' + mode + '/gen/' + src + '.hh'
                    swaggers[hh] = src
                elif src.endswith('.thrift'):
                    thrifts.add(src)
                elif src.endswith('.g'):
                    antlr3_grammars.add(src)
                else:
                    raise Exception('No rule for ' + src)
        for obj in compiles:
            src = compiles[obj]
            gen_headers = list(ragels.keys())
            gen_headers += ['seastar/build/{}/http/request_parser.hh'.format(mode)]
            for th in thrifts:
                gen_headers += th.headers('$builddir/{}/gen'.format(mode))
            for g in antlr3_grammars:
                gen_headers += g.headers('$builddir/{}/gen'.format(mode))
            gen_headers += list(swaggers.keys())
            f.write('build {}: cxx.{} {} || {} \n'.format(obj, mode, src, ' '.join(gen_headers)))
        for hh in ragels:
            src = ragels[hh]
            f.write('build {}: ragel {}\n'.format(hh, src))
        for hh in swaggers:
            src = swaggers[hh]
            f.write('build {}: swagger {}\n'.format(hh,src))
        for thrift in thrifts:
            outs = ' '.join(thrift.generated('$builddir/{}/gen'.format(mode)))
            f.write('build {}: thrift.{} {}\n'.format(outs, mode, thrift.source))
            for cc in thrift.sources('$builddir/{}/gen'.format(mode)):
                obj = cc.replace('.cpp', '.o')
                f.write('build {}: cxx.{} {}\n'.format(obj, mode, cc))
        for grammar in antlr3_grammars:
            outs = ' '.join(grammar.generated('$builddir/{}/gen'.format(mode)))
            f.write('build {}: antlr3.{} {}\n  stem = {}\n'.format(outs, mode, grammar.source,
                                                                   grammar.source.rsplit('.', 1)[0]))
            for cc in grammar.sources('$builddir/{}/gen'.format(mode)):
                obj = cc.replace('.cpp', '.o')
                f.write('build {}: cxx.{} {}\n'.format(obj, mode, cc))
        f.write('build seastar/build/{}/libseastar.a: ninja {}\n'.format(mode, seastar_deps))
        f.write('  subdir = seastar\n')
        f.write('  target = build/{}/libseastar.a\n'.format(mode))
    f.write('build {}: phony\n'.format(seastar_deps))
    f.write(textwrap.dedent('''\
        rule configure
          command = python3 configure.py $configure_args
          generator = 1
        build build.ninja: configure | configure.py
        rule cscope
            command = find -name '*.[chS]' -o -name "*.cc" -o -name "*.hh" | cscope -bq -i-
            description = CSCOPE
        build cscope: cscope
        rule request_parser_hh
           command = {ninja} -C seastar build/release/gen/http/request_parser.hh build/debug/gen/http/request_parser.hh
           description = GEN seastar/http/request_parser.hh
        build seastar/build/release/http/request_parser.hh seastar/build/debug/http/request_parser.hh: request_parser_hh
        rule clean
            command = rm -rf build
            description = CLEAN
        build clean: clean
        default {modes_list}
        ''').format(modes_list = ' '.join(build_modes), **globals()))
