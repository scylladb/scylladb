#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 ScyllaDB
#

#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
#

import argparse
import os
import platform
import re
import shlex
import subprocess
import sys
import tempfile
import textwrap
from distutils.spawn import find_executable

curdir = os.getcwd()

tempfile.tempdir = "./build/tmp"

configure_args = str.join(' ', [shlex.quote(x) for x in sys.argv[1:]])

for line in open('/etc/os-release'):
    key, _, value = line.partition('=')
    value = value.strip().strip('"')
    if key == 'ID':
        os_ids = [value]
    if key == 'ID_LIKE':
        os_ids += value.split(' ')


# distribution "internationalization", converting package names.
# Fedora name is key, values is distro -> package name dict.
i18n_xlat = {
    'boost-devel': {
        'debian': 'libboost-dev',
        'ubuntu': 'libboost-dev (libboost1.55-dev on 14.04)',
    },
}


def pkgname(name):
    if name in i18n_xlat:
        dict = i18n_xlat[name]
        for id in os_ids:
            if id in dict:
                return dict[id]
    return name


def get_flags():
    with open('/proc/cpuinfo') as f:
        for line in f:
            if line.strip():
                if line.rstrip('\n').startswith('flags'):
                    return re.sub(r'^flags\s+: ', '', line).split()


def add_tristate(arg_parser, name, dest, help):
    arg_parser.add_argument('--enable-' + name, dest=dest, action='store_true', default=None,
                            help='Enable ' + help)
    arg_parser.add_argument('--disable-' + name, dest=dest, action='store_false', default=None,
                            help='Disable ' + help)


def apply_tristate(var, test, note, missing):
    if (var is None) or var:
        if test():
            return True
        elif var is True:
            print(missing)
            sys.exit(1)
        else:
            print(note)
            return False
    return False


def have_pkg(package):
    return subprocess.call(['pkg-config', package]) == 0


def pkg_config(package, *options):
    # Add the directory containing the package to the search path, if a file is
    # specified instead of a name.
    if package.endswith('.pc'):
        local_path = os.path.dirname(package)
        env = { 'PKG_CONFIG_PATH' : '{}:{}'.format(local_path, os.environ.get('PKG_CONFIG_PATH', '')) }
    else:
        env = None

    output = subprocess.check_output(['pkg-config'] + list(options) + [package], env=env)
    return output.decode('utf-8').strip()


def try_compile(compiler, source='', flags=[]):
    return try_compile_and_link(compiler, source, flags=flags + ['-c'])


def ensure_tmp_dir_exists():
    if not os.path.exists(tempfile.tempdir):
        os.makedirs(tempfile.tempdir)


def try_compile_and_link(compiler, source='', flags=[]):
    ensure_tmp_dir_exists()
    with tempfile.NamedTemporaryFile() as sfile:
        ofile = tempfile.mktemp()
        try:
            sfile.file.write(bytes(source, 'utf-8'))
            sfile.file.flush()
            # We can't write to /dev/null, since in some cases (-ftest-coverage) gcc will create an auxiliary
            # output file based on the name of the output file, and "/dev/null.gcsa" is not a good name
            return subprocess.call([compiler, '-x', 'c++', '-o', ofile, sfile.name] + args.user_cflags.split() + flags,
                                   stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL) == 0
        finally:
            if os.path.exists(ofile):
                os.unlink(ofile)


def flag_supported(flag, compiler):
    # gcc ignores -Wno-x even if it is not supported
    adjusted = re.sub('^-Wno-', '-W', flag)
    split = adjusted.split(' ')
    return try_compile(flags=['-Werror'] + split, compiler=compiler)


def linker_flags(compiler):
    src_main = 'int main(int argc, char **argv) { return 0; }'
    link_flags = ['-fuse-ld=lld']
    if try_compile_and_link(source=src_main, flags=link_flags, compiler=compiler):
        print('Note: using the lld linker')
        return ' '.join(link_flags)
    link_flags = ['-fuse-ld=gold']
    if try_compile_and_link(source=src_main, flags=link_flags, compiler=compiler):
        print('Note: using the gold linker')
        threads_flag = '-Wl,--threads'
        if try_compile_and_link(source=src_main, flags=link_flags + [threads_flag], compiler=compiler):
            link_flags.append(threads_flag)
        return ' '.join(link_flags)
    else:
        print('Note: neither lld nor gold found; using default system linker')
        return ''


def maybe_static(flag, libs):
    if flag and not args.static:
        libs = '-Wl,-Bstatic {} -Wl,-Bdynamic'.format(libs)
    return libs


class Source(object):
    def __init__(self, source, hh_prefix, cc_prefix):
        self.source = source
        self.hh_prefix = hh_prefix
        self.cc_prefix = cc_prefix

    def headers(self, gen_dir):
        return [x for x in self.generated(gen_dir) if x.endswith(self.hh_prefix)]

    def sources(self, gen_dir):
        return [x for x in self.generated(gen_dir) if x.endswith(self.cc_prefix)]

    def objects(self, gen_dir):
        return [x.replace(self.cc_prefix, '.o') for x in self.sources(gen_dir)]

    def endswith(self, end):
        return self.source.endswith(end)

class Thrift(Source):
    def __init__(self, source, service):
        Source.__init__(self, source, '.h', '.cpp')
        self.service = service

    def generated(self, gen_dir):
        basename = os.path.splitext(os.path.basename(self.source))[0]
        files = [basename + '_' + ext
                 for ext in ['types.cpp', 'types.h', 'constants.cpp', 'constants.h']]
        files += [self.service + ext
                  for ext in ['.cpp', '.h']]
        return [os.path.join(gen_dir, file) for file in files]

def default_target_arch():
    if platform.machine() in ['i386', 'i686', 'x86_64']:
        return 'westmere'   # support PCLMUL
    elif platform.machine() == 'aarch64':
        return 'armv8-a+crc+crypto'
    else:
        return ''


class Antlr3Grammar(Source):
    def __init__(self, source):
        Source.__init__(self, source, '.hpp', '.cpp')

    def generated(self, gen_dir):
        basename = os.path.splitext(self.source)[0]
        files = [basename + ext
                 for ext in ['Lexer.cpp', 'Lexer.hpp', 'Parser.cpp', 'Parser.hpp']]
        return [os.path.join(gen_dir, file) for file in files]

class Json2Code(Source):
    def __init__(self, source):
        Source.__init__(self, source, '.hh', '.cc')

    def generated(self, gen_dir):
        return [os.path.join(gen_dir, self.source + '.hh'), os.path.join(gen_dir, self.source + '.cc')]

def find_headers(repodir, excluded_dirs):
    walker = os.walk(repodir)

    _, dirs, files = next(walker)
    for excl_dir in excluded_dirs:
        try:
            dirs.remove(excl_dir)
        except ValueError:
            # Ignore complaints about excl_dir not being in dirs
            pass

    is_hh = lambda f: f.endswith('.hh')
    headers = list(filter(is_hh, files))

    for dirpath, _, files in walker:
        if dirpath.startswith('./'):
            dirpath = dirpath[2:]
        headers += [os.path.join(dirpath, hh) for hh in filter(is_hh, files)]

    return headers


modes = {
    'debug': {
        'cxxflags': '-DDEBUG -DDEBUG_LSA_SANITIZER -DSCYLLA_ENABLE_ERROR_INJECTION',
        'cxx_ld_flags': '-Wstack-usage=%s' % (1024*40),
    },
    'release': {
        'cxxflags': '',
        'cxx_ld_flags': '-O3 -Wstack-usage=%s' % (1024*13),
    },
    'dev': {
        'cxxflags': '-DSEASTAR_ENABLE_ALLOC_FAILURE_INJECTION -DSCYLLA_ENABLE_ERROR_INJECTION',
        'cxx_ld_flags': '-O1 -Wstack-usage=%s' % (1024*21),
    },
    'sanitize': {
        'cxxflags': '-DDEBUG -DDEBUG_LSA_SANITIZER -DSCYLLA_ENABLE_ERROR_INJECTION',
        'cxx_ld_flags': '-Os -Wstack-usage=%s' % (1024*50),
    }
}

scylla_tests = set([
    'test/boost/UUID_test',
    'test/boost/aggregate_fcts_test',
    'test/boost/allocation_strategy_test',
    'test/boost/alternator_base64_test',
    'test/boost/anchorless_list_test',
    'test/boost/auth_passwords_test',
    'test/boost/auth_resource_test',
    'test/boost/auth_test',
    'test/boost/batchlog_manager_test',
    'test/boost/big_decimal_test',
    'test/boost/broken_sstable_test',
    'test/boost/bytes_ostream_test',
    'test/boost/cache_flat_mutation_reader_test',
    'test/boost/cached_file_test',
    'test/boost/caching_options_test',
    'test/boost/canonical_mutation_test',
    'test/boost/cartesian_product_test',
    'test/boost/castas_fcts_test',
    'test/boost/cdc_test',
    'test/boost/cell_locker_test',
    'test/boost/checksum_utils_test',
    'test/boost/chunked_vector_test',
    'test/boost/clustering_ranges_walker_test',
    'test/boost/commitlog_test',
    'test/boost/compound_test',
    'test/boost/compress_test',
    'test/boost/config_test',
    'test/boost/continuous_data_consumer_test',
    'test/boost/counter_test',
    'test/boost/cql_auth_query_test',
    'test/boost/cql_auth_syntax_test',
    'test/boost/cql_query_test',
    'test/boost/cql_query_large_test',
    'test/boost/cql_query_like_test',
    'test/boost/cql_query_group_test',
    'test/boost/cql_functions_test',
    'test/boost/crc_test',
    'test/boost/data_listeners_test',
    'test/boost/database_test',
    'test/boost/duration_test',
    'test/boost/dynamic_bitset_test',
    'test/boost/enum_option_test',
    'test/boost/enum_set_test',
    'test/boost/extensions_test',
    'test/boost/error_injection_test',
    'test/boost/filtering_test',
    'test/boost/flat_mutation_reader_test',
    'test/boost/flush_queue_test',
    'test/boost/fragmented_temporary_buffer_test',
    'test/boost/frozen_mutation_test',
    'test/boost/gossip_test',
    'test/boost/gossiping_property_file_snitch_test',
    'test/boost/hash_test',
    'test/boost/idl_test',
    'test/boost/input_stream_test',
    'test/boost/json_cql_query_test',
    'test/boost/keys_test',
    'test/boost/like_matcher_test',
    'test/boost/limiting_data_source_test',
    'test/boost/linearizing_input_stream_test',
    'test/boost/loading_cache_test',
    'test/boost/log_heap_test',
    'test/boost/estimated_histogram_test',
    'test/boost/logalloc_test',
    'test/boost/managed_vector_test',
    'test/boost/map_difference_test',
    'test/boost/memtable_test',
    'test/boost/meta_test',
    'test/boost/multishard_mutation_query_test',
    'test/boost/murmur_hash_test',
    'test/boost/mutation_fragment_test',
    'test/boost/mutation_query_test',
    'test/boost/mutation_reader_test',
    'test/boost/multishard_combining_reader_as_mutation_source_test',
    'test/boost/mutation_test',
    'test/boost/mutation_writer_test',
    'test/boost/mvcc_test',
    'test/boost/network_topology_strategy_test',
    'test/boost/nonwrapping_range_test',
    'test/boost/observable_test',
    'test/boost/partitioner_test',
    'test/boost/querier_cache_test',
    'test/boost/query_processor_test',
    'test/boost/range_test',
    'test/boost/range_tombstone_list_test',
    'test/boost/reusable_buffer_test',
    'test/boost/role_manager_test',
    'test/boost/row_cache_test',
    'test/boost/schema_change_test',
    'test/boost/schema_registry_test',
    'test/boost/secondary_index_test',
    'test/boost/index_with_paging_test',
    'test/boost/serialization_test',
    'test/boost/serialized_action_test',
    'test/boost/small_vector_test',
    'test/boost/snitch_reset_test',
    'test/boost/sstable_3_x_test',
    'test/boost/sstable_datafile_test',
    'test/boost/sstable_mutation_test',
    'test/boost/schema_changes_test',
    'test/boost/sstable_conforms_to_mutation_source_test',
    'test/boost/sstable_resharding_test',
    'test/boost/sstable_directory_test',
    'test/boost/sstable_test',
    'test/boost/storage_proxy_test',
    'test/boost/top_k_test',
    'test/boost/transport_test',
    'test/boost/truncation_migration_test',
    'test/boost/types_test',
    'test/boost/user_function_test',
    'test/boost/user_types_test',
    'test/boost/utf8_test',
    'test/boost/view_build_test',
    'test/boost/view_complex_test',
    'test/boost/view_schema_test',
    'test/boost/view_schema_pkey_test',
    'test/boost/view_schema_ckey_test',
    'test/boost/vint_serialization_test',
    'test/boost/virtual_reader_test',
    'test/manual/ec2_snitch_test',
    'test/manual/gce_snitch_test',
    'test/manual/gossip',
    'test/manual/hint_test',
    'test/manual/imr_test',
    'test/manual/json_test',
    'test/manual/message',
    'test/manual/partition_data_test',
    'test/manual/row_locker_test',
    'test/manual/streaming_histogram_test',
    'test/manual/sstable_scan_footprint_test',
    'test/perf/memory_footprint_test',
    'test/perf/perf_cache_eviction',
    'test/perf/perf_cql_parser',
    'test/perf/perf_fast_forward',
    'test/perf/perf_hash',
    'test/perf/perf_mutation',
    'test/perf/perf_row_cache_update',
    'test/perf/perf_simple_query',
    'test/perf/perf_sstable',
    'test/unit/lsa_async_eviction_test',
    'test/unit/lsa_sync_eviction_test',
    'test/unit/row_cache_alloc_stress_test',
    'test/unit/row_cache_stress_test',
])

perf_tests = set([
    'test/perf/perf_mutation_readers',
    'test/perf/perf_checksum',
    'test/perf/perf_mutation_fragment',
    'test/perf/perf_idl',
    'test/perf/perf_vint',
    'test/perf/perf_big_decimal',
])

apps = set([
    'scylla',
    'test/tools/cql_repl',
    'tools/scylla-types',
])

tests = scylla_tests | perf_tests

other = set([
    'iotune',
])

all_artifacts = apps | tests | other

arg_parser = argparse.ArgumentParser('Configure scylla')
arg_parser.add_argument('--static', dest='static', action='store_const', default='',
                        const='-static',
                        help='Static link (useful for running on hosts outside the build environment')
arg_parser.add_argument('--pie', dest='pie', action='store_true',
                        help='Build position-independent executable (PIE)')
arg_parser.add_argument('--so', dest='so', action='store_true',
                        help='Build shared object (SO) instead of executable')
arg_parser.add_argument('--mode', action='append', choices=list(modes.keys()), dest='selected_modes')
arg_parser.add_argument('--with', dest='artifacts', action='append', choices=all_artifacts, default=[])
arg_parser.add_argument('--with-seastar', action='store', dest='seastar_path', default='seastar', help='Path to Seastar sources')
arg_parser.add_argument('--cflags', action='store', dest='user_cflags', default='',
                        help='Extra flags for the C++ compiler')
arg_parser.add_argument('--ldflags', action='store', dest='user_ldflags', default='',
                        help='Extra flags for the linker')
arg_parser.add_argument('--target', action='store', dest='target', default=default_target_arch(),
                        help='Target architecture (-march)')
arg_parser.add_argument('--compiler', action='store', dest='cxx', default='g++',
                        help='C++ compiler path')
arg_parser.add_argument('--c-compiler', action='store', dest='cc', default='gcc',
                        help='C compiler path')
arg_parser.add_argument('--with-osv', action='store', dest='with_osv', default='',
                        help='Shortcut for compile for OSv')
add_tristate(arg_parser, name='dpdk', dest='dpdk',
                        help='Use dpdk (from seastar dpdk sources) (default=True for release builds)')
arg_parser.add_argument('--dpdk-target', action='store', dest='dpdk_target', default='',
                        help='Path to DPDK SDK target location (e.g. <DPDK SDK dir>/x86_64-native-linuxapp-gcc)')
arg_parser.add_argument('--debuginfo', action='store', dest='debuginfo', type=int, default=1,
                        help='Enable(1)/disable(0)compiler debug information generation')
arg_parser.add_argument('--static-stdc++', dest='staticcxx', action='store_true',
                        help='Link libgcc and libstdc++ statically')
arg_parser.add_argument('--static-thrift', dest='staticthrift', action='store_true',
                        help='Link libthrift statically')
arg_parser.add_argument('--static-boost', dest='staticboost', action='store_true',
                        help='Link boost statically')
arg_parser.add_argument('--static-yaml-cpp', dest='staticyamlcpp', action='store_true',
                        help='Link libyaml-cpp statically')
arg_parser.add_argument('--tests-debuginfo', action='store', dest='tests_debuginfo', type=int, default=0,
                        help='Enable(1)/disable(0)compiler debug information generation for tests')
arg_parser.add_argument('--python', action='store', dest='python', default='python3',
                        help='Python3 path')
arg_parser.add_argument('--split-dwarf', dest='split_dwarf', action='store_true', default=False,
                        help='use of split dwarf (https://gcc.gnu.org/wiki/DebugFission) to speed up linking')
arg_parser.add_argument('--enable-alloc-failure-injector', dest='alloc_failure_injector', action='store_true', default=False,
                        help='enable allocation failure injection')
arg_parser.add_argument('--with-antlr3', dest='antlr3_exec', action='store', default=None,
                        help='path to antlr3 executable')
arg_parser.add_argument('--with-ragel', dest='ragel_exec', action='store', default='ragel',
        help='path to ragel executable')
add_tristate(arg_parser, name='stack-guards', dest='stack_guards', help='Use stack guards')
args = arg_parser.parse_args()

defines = ['XXH_PRIVATE_API',
           'SEASTAR_TESTING_MAIN',
]

extra_cxxflags = {}

cassandra_interface = Thrift(source='interface/cassandra.thrift', service='Cassandra')

scylla_core = (['database.cc',
                'absl-flat_hash_map.cc',
                'table.cc',
                'atomic_cell.cc',
                'collection_mutation.cc',
                'connection_notifier.cc',
                'hashers.cc',
                'schema.cc',
                'frozen_schema.cc',
                'schema_registry.cc',
                'bytes.cc',
                'timeout_config.cc',
                'mutation.cc',
                'mutation_fragment.cc',
                'partition_version.cc',
                'row_cache.cc',
                'canonical_mutation.cc',
                'frozen_mutation.cc',
                'memtable.cc',
                'schema_mutations.cc',
                'utils/logalloc.cc',
                'utils/large_bitset.cc',
                'utils/buffer_input_stream.cc',
                'utils/limiting_data_source.cc',
                'utils/updateable_value.cc',
                'utils/directories.cc',
                'utils/generation-number.cc',
                'mutation_partition.cc',
                'mutation_partition_view.cc',
                'mutation_partition_serializer.cc',
                'converting_mutation_partition_applier.cc',
                'mutation_reader.cc',
                'flat_mutation_reader.cc',
                'mutation_query.cc',
                'json.cc',
                'keys.cc',
                'counters.cc',
                'compress.cc',
                'zstd.cc',
                'sstables/mp_row_consumer.cc',
                'sstables/sstables.cc',
                'sstables/sstables_manager.cc',
                'sstables/mc/writer.cc',
                'sstables/sstable_version.cc',
                'sstables/compress.cc',
                'sstables/partition.cc',
                'sstables/compaction.cc',
                'sstables/compaction_strategy.cc',
                'sstables/size_tiered_compaction_strategy.cc',
                'sstables/leveled_compaction_strategy.cc',
                'sstables/time_window_compaction_strategy.cc',
                'sstables/compaction_manager.cc',
                'sstables/integrity_checked_file_impl.cc',
                'sstables/prepended_input_stream.cc',
                'sstables/m_format_read_helpers.cc',
                'sstables/sstable_directory.cc',
                'transport/event.cc',
                'transport/event_notifier.cc',
                'transport/server.cc',
                'transport/controller.cc',
                'transport/messages/result_message.cc',
                'cdc/cdc_partitioner.cc',
                'cdc/log.cc',
                'cdc/split.cc',
                'cdc/generation.cc',
                'cdc/metadata.cc',
                'cql3/type_json.cc',
                'cql3/abstract_marker.cc',
                'cql3/attributes.cc',
                'cql3/cf_name.cc',
                'cql3/cql3_type.cc',
                'cql3/operation.cc',
                'cql3/index_name.cc',
                'cql3/keyspace_element_name.cc',
                'cql3/lists.cc',
                'cql3/sets.cc',
                'cql3/tuples.cc',
                'cql3/maps.cc',
                'cql3/functions/user_function.cc',
                'cql3/functions/functions.cc',
                'cql3/functions/aggregate_fcts.cc',
                'cql3/functions/castas_fcts.cc',
                'cql3/functions/error_injection_fcts.cc',
                'cql3/statements/cf_prop_defs.cc',
                'cql3/statements/cf_statement.cc',
                'cql3/statements/authentication_statement.cc',
                'cql3/statements/create_keyspace_statement.cc',
                'cql3/statements/create_table_statement.cc',
                'cql3/statements/create_view_statement.cc',
                'cql3/statements/create_type_statement.cc',
                'cql3/statements/create_function_statement.cc',
                'cql3/statements/drop_index_statement.cc',
                'cql3/statements/drop_keyspace_statement.cc',
                'cql3/statements/drop_table_statement.cc',
                'cql3/statements/drop_view_statement.cc',
                'cql3/statements/drop_type_statement.cc',
                'cql3/statements/drop_function_statement.cc',
                'cql3/statements/schema_altering_statement.cc',
                'cql3/statements/ks_prop_defs.cc',
                'cql3/statements/function_statement.cc',
                'cql3/statements/modification_statement.cc',
                'cql3/statements/cas_request.cc',
                'cql3/statements/raw/parsed_statement.cc',
                'cql3/statements/property_definitions.cc',
                'cql3/statements/update_statement.cc',
                'cql3/statements/delete_statement.cc',
                'cql3/statements/batch_statement.cc',
                'cql3/statements/select_statement.cc',
                'cql3/statements/use_statement.cc',
                'cql3/statements/index_prop_defs.cc',
                'cql3/statements/index_target.cc',
                'cql3/statements/create_index_statement.cc',
                'cql3/statements/truncate_statement.cc',
                'cql3/statements/alter_table_statement.cc',
                'cql3/statements/alter_view_statement.cc',
                'cql3/statements/list_users_statement.cc',
                'cql3/statements/authorization_statement.cc',
                'cql3/statements/permission_altering_statement.cc',
                'cql3/statements/list_permissions_statement.cc',
                'cql3/statements/grant_statement.cc',
                'cql3/statements/revoke_statement.cc',
                'cql3/statements/alter_type_statement.cc',
                'cql3/statements/alter_keyspace_statement.cc',
                'cql3/statements/role-management-statements.cc',
                'cql3/update_parameters.cc',
                'cql3/ut_name.cc',
                'cql3/role_name.cc',
                'thrift/handler.cc',
                'thrift/server.cc',
                'thrift/controller.cc',
                'thrift/thrift_validation.cc',
                'utils/runtime.cc',
                'utils/murmur_hash.cc',
                'utils/uuid.cc',
                'utils/big_decimal.cc',
                'types.cc',
                'validation.cc',
                'service/priority_manager.cc',
                'service/migration_manager.cc',
                'service/storage_proxy.cc',
                'service/paxos/proposal.cc',
                'service/paxos/prepare_response.cc',
                'service/paxos/paxos_state.cc',
                'service/paxos/prepare_summary.cc',
                'cql3/operator.cc',
                'cql3/relation.cc',
                'cql3/column_identifier.cc',
                'cql3/column_specification.cc',
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
                'cql3/result_set.cc',
                'cql3/variable_specifications.cc',
                'db/consistency_level.cc',
                'db/system_keyspace.cc',
                'db/system_distributed_keyspace.cc',
                'db/size_estimates_virtual_reader.cc',
                'db/schema_tables.cc',
                'db/cql_type_parser.cc',
                'db/legacy_schema_migrator.cc',
                'db/commitlog/commitlog.cc',
                'db/commitlog/commitlog_replayer.cc',
                'db/commitlog/commitlog_entry.cc',
                'db/data_listeners.cc',
                'db/hints/manager.cc',
                'db/hints/resource_manager.cc',
                'db/config.cc',
                'db/extensions.cc',
                'db/heat_load_balance.cc',
                'db/large_data_handler.cc',
                'db/marshal/type_parser.cc',
                'db/batchlog_manager.cc',
                'db/view/view.cc',
                'db/view/view_update_generator.cc',
                'db/view/row_locking.cc',
                'db/sstables-format-selector.cc',
                'index/secondary_index_manager.cc',
                'index/secondary_index.cc',
                'utils/UUID_gen.cc',
                'utils/i_filter.cc',
                'utils/bloom_filter.cc',
                'utils/bloom_calculations.cc',
                'utils/rate_limiter.cc',
                'utils/file_lock.cc',
                'utils/dynamic_bitset.cc',
                'utils/managed_bytes.cc',
                'utils/exceptions.cc',
                'utils/config_file.cc',
                'utils/multiprecision_int.cc',
                'utils/gz/crc_combine.cc',
                'gms/version_generator.cc',
                'gms/versioned_value.cc',
                'gms/gossiper.cc',
                'gms/feature_service.cc',
                'gms/failure_detector.cc',
                'gms/gossip_digest_syn.cc',
                'gms/gossip_digest_ack.cc',
                'gms/gossip_digest_ack2.cc',
                'gms/endpoint_state.cc',
                'gms/application_state.cc',
                'gms/inet_address.cc',
                'dht/i_partitioner.cc',
                'dht/token.cc',
                'dht/murmur3_partitioner.cc',
                'dht/boot_strapper.cc',
                'dht/range_streamer.cc',
                'unimplemented.cc',
                'query.cc',
                'query-result-set.cc',
                'locator/abstract_replication_strategy.cc',
                'locator/simple_strategy.cc',
                'locator/local_strategy.cc',
                'locator/network_topology_strategy.cc',
                'locator/everywhere_replication_strategy.cc',
                'locator/token_metadata.cc',
                'locator/snitch_base.cc',
                'locator/simple_snitch.cc',
                'locator/rack_inferring_snitch.cc',
                'locator/gossiping_property_file_snitch.cc',
                'locator/production_snitch_base.cc',
                'locator/ec2_snitch.cc',
                'locator/ec2_multi_region_snitch.cc',
                'locator/gce_snitch.cc',
                'message/messaging_service.cc',
                'service/client_state.cc',
                'service/migration_task.cc',
                'service/storage_service.cc',
                'service/misc_services.cc',
                'service/pager/paging_state.cc',
                'service/pager/query_pagers.cc',
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
                'streaming/stream_session_state.cc',
                'clocks-impl.cc',
                'partition_slice_builder.cc',
                'init.cc',
                'lister.cc',
                'repair/repair.cc',
                'repair/row_level.cc',
                'exceptions/exceptions.cc',
                'auth/allow_all_authenticator.cc',
                'auth/allow_all_authorizer.cc',
                'auth/authenticated_user.cc',
                'auth/authenticator.cc',
                'auth/common.cc',
                'auth/default_authorizer.cc',
                'auth/resource.cc',
                'auth/roles-metadata.cc',
                'auth/passwords.cc',
                'auth/password_authenticator.cc',
                'auth/permission.cc',
                'auth/permissions_cache.cc',
                'auth/service.cc',
                'auth/standard_role_manager.cc',
                'auth/transitional.cc',
                'auth/authentication_options.cc',
                'auth/role_or_anonymous.cc',
                'auth/sasl_challenge.cc',
                'tracing/tracing.cc',
                'tracing/trace_keyspace_helper.cc',
                'tracing/trace_state.cc',
                'tracing/tracing_backend_registry.cc',
                'tracing/traced_file.cc',
                'table_helper.cc',
                'range_tombstone.cc',
                'range_tombstone_list.cc',
                'utils/disk-error-handler.cc',
                'duration.cc',
                'vint-serialization.cc',
                'utils/arch/powerpc/crc32-vpmsum/crc32_wrapper.cc',
                'querier.cc',
                'data/cell.cc',
                'mutation_writer/multishard_writer.cc',
                'multishard_mutation_query.cc',
                'reader_concurrency_semaphore.cc',
                'distributed_loader.cc',
                'utils/utf8.cc',
                'utils/ascii.cc',
                'utils/like_matcher.cc',
                'utils/error_injection.cc',
                'mutation_writer/timestamp_based_splitting_writer.cc',
                'mutation_writer/shard_based_splitting_writer.cc',
                'lua.cc',
                ] + [Antlr3Grammar('cql3/Cql.g')] + [Thrift('interface/cassandra.thrift', 'Cassandra')]
               )

api = ['api/api.cc',
       Json2Code('api/api-doc/storage_service.json'),
       Json2Code('api/api-doc/lsa.json'),
       'api/storage_service.cc',
       Json2Code('api/api-doc/commitlog.json'),
       'api/commitlog.cc',
       Json2Code('api/api-doc/gossiper.json'),
       'api/gossiper.cc',
       Json2Code('api/api-doc/failure_detector.json'),
       'api/failure_detector.cc',
       Json2Code('api/api-doc/column_family.json'),
       'api/column_family.cc',
       'api/messaging_service.cc',
       Json2Code('api/api-doc/messaging_service.json'),
       Json2Code('api/api-doc/storage_proxy.json'),
       'api/storage_proxy.cc',
       Json2Code('api/api-doc/cache_service.json'),
       'api/cache_service.cc',
       Json2Code('api/api-doc/collectd.json'),
       'api/collectd.cc',
       Json2Code('api/api-doc/endpoint_snitch_info.json'),
       'api/endpoint_snitch.cc',
       Json2Code('api/api-doc/compaction_manager.json'),
       'api/compaction_manager.cc',
       Json2Code('api/api-doc/hinted_handoff.json'),
       'api/hinted_handoff.cc',
       Json2Code('api/api-doc/utils.json'),
       'api/lsa.cc',
       Json2Code('api/api-doc/stream_manager.json'),
       'api/stream_manager.cc',
       Json2Code('api/api-doc/system.json'),
       'api/system.cc',
       'api/config.cc',
       Json2Code('api/api-doc/config.json'),
       'api/error_injection.cc',
       Json2Code('api/api-doc/error_injection.json'),
       ]

alternator = [
       'alternator/server.cc',
       'alternator/executor.cc',
       'alternator/stats.cc',
       'alternator/base64.cc',
       'alternator/serialization.cc',
       'alternator/expressions.cc',
       Antlr3Grammar('alternator/expressions.g'),
       'alternator/conditions.cc',
       'alternator/rjson.cc',
       'alternator/auth.cc',
]

redis = [
        'redis/service.cc',
        'redis/server.cc',
        'redis/query_processor.cc',
        'redis/protocol_parser.rl',
        'redis/keyspace_utils.cc',
        'redis/options.cc',
        'redis/stats.cc',
        'redis/mutation_utils.cc',
        'redis/query_utils.cc',
        'redis/abstract_command.cc',
        'redis/command_factory.cc',
        'redis/commands.cc',
        'redis/lolwut.cc',
        ]

idls = ['idl/gossip_digest.idl.hh',
        'idl/uuid.idl.hh',
        'idl/range.idl.hh',
        'idl/keys.idl.hh',
        'idl/read_command.idl.hh',
        'idl/token.idl.hh',
        'idl/ring_position.idl.hh',
        'idl/result.idl.hh',
        'idl/frozen_mutation.idl.hh',
        'idl/reconcilable_result.idl.hh',
        'idl/streaming.idl.hh',
        'idl/paging_state.idl.hh',
        'idl/frozen_schema.idl.hh',
        'idl/partition_checksum.idl.hh',
        'idl/replay_position.idl.hh',
        'idl/truncation_record.idl.hh',
        'idl/mutation.idl.hh',
        'idl/query.idl.hh',
        'idl/idl_test.idl.hh',
        'idl/commitlog.idl.hh',
        'idl/tracing.idl.hh',
        'idl/consistency_level.idl.hh',
        'idl/cache_temperature.idl.hh',
        'idl/view.idl.hh',
        'idl/messaging_service.idl.hh',
        'idl/paxos.idl.hh',
        ]

headers = find_headers('.', excluded_dirs=['idl', 'build', 'seastar', '.git'])

scylla_tests_generic_dependencies = [
    'test/lib/cql_test_env.cc',
    'test/lib/test_services.cc',
    'test/lib/log.cc',
    'test/lib/reader_permit.cc',
    'test/lib/test_utils.cc',
]

scylla_tests_dependencies = scylla_core + idls + scylla_tests_generic_dependencies + [
    'test/lib/cql_assertions.cc',
    'test/lib/result_set_assertions.cc',
    'test/lib/mutation_source_test.cc',
    'test/lib/sstable_utils.cc',
    'test/lib/data_model.cc',
    'test/lib/exception_utils.cc',
    'test/lib/random_schema.cc',
]

deps = {
    'scylla': idls + ['main.cc', 'release.cc', 'build_id.cc'] + scylla_core + api + alternator + redis,
    'test/tools/cql_repl': idls + ['test/tools/cql_repl.cc'] + scylla_core + scylla_tests_generic_dependencies,
    #FIXME: we don't need all of scylla_core here, only the types module, need to modularize scylla_core.
    'tools/scylla-types': idls + ['tools/scylla-types.cc'] + scylla_core,
}

pure_boost_tests = set([
    'test/boost/anchorless_list_test',
    'test/boost/auth_passwords_test',
    'test/boost/auth_resource_test',
    'test/boost/big_decimal_test',
    'test/boost/caching_options_test',
    'test/boost/cartesian_product_test',
    'test/boost/checksum_utils_test',
    'test/boost/chunked_vector_test',
    'test/boost/compound_test',
    'test/boost/compress_test',
    'test/boost/cql_auth_syntax_test',
    'test/boost/crc_test',
    'test/boost/duration_test',
    'test/boost/dynamic_bitset_test',
    'test/boost/enum_option_test',
    'test/boost/enum_set_test',
    'test/boost/idl_test',
    'test/boost/keys_test',
    'test/boost/like_matcher_test',
    'test/boost/linearizing_input_stream_test',
    'test/boost/map_difference_test',
    'test/boost/meta_test',
    'test/boost/nonwrapping_range_test',
    'test/boost/observable_test',
    'test/boost/range_test',
    'test/boost/range_tombstone_list_test',
    'test/boost/serialization_test',
    'test/boost/small_vector_test',
    'test/boost/top_k_test',
    'test/boost/vint_serialization_test',
    'test/manual/json_test',
    'test/manual/streaming_histogram_test',
])

tests_not_using_seastar_test_framework = set([
    'test/boost/alternator_base64_test',
    'test/boost/small_vector_test',
    'test/manual/gossip',
    'test/manual/message',
    'test/perf/memory_footprint_test',
    'test/perf/perf_cache_eviction',
    'test/perf/perf_cql_parser',
    'test/perf/perf_hash',
    'test/perf/perf_mutation',
    'test/perf/perf_row_cache_update',
    'test/unit/lsa_async_eviction_test',
    'test/unit/lsa_sync_eviction_test',
    'test/unit/row_cache_alloc_stress_test',
    'test/manual/sstable_scan_footprint_test',
]) | pure_boost_tests

for t in tests_not_using_seastar_test_framework:
    if t not in scylla_tests:
        raise Exception("Test %s not found in scylla_tests" % (t))

for t in scylla_tests:
    deps[t] = [t + '.cc']
    if t not in tests_not_using_seastar_test_framework:
        deps[t] += scylla_tests_dependencies
    else:
        deps[t] += scylla_core + idls + scylla_tests_generic_dependencies

perf_tests_seastar_deps = [
    'seastar/tests/perf/perf_tests.cc'
]

for t in perf_tests:
    deps[t] = [t + '.cc'] + scylla_tests_dependencies + perf_tests_seastar_deps

deps['test/boost/sstable_test'] += ['test/lib/normalizing_reader.cc']
deps['test/boost/sstable_datafile_test'] += ['test/lib/normalizing_reader.cc']
deps['test/boost/mutation_reader_test'] += ['test/lib/dummy_sharder.cc' ]
deps['test/boost/multishard_combining_reader_as_mutation_source_test'] += ['test/lib/dummy_sharder.cc' ]

deps['test/boost/bytes_ostream_test'] = [
    "test/boost/bytes_ostream_test.cc",
    "utils/managed_bytes.cc",
    "utils/logalloc.cc",
    "utils/dynamic_bitset.cc",
    "test/lib/log.cc",
]
deps['test/boost/input_stream_test'] = ['test/boost/input_stream_test.cc']
deps['test/boost/UUID_test'] = ['utils/UUID_gen.cc', 'test/boost/UUID_test.cc', 'utils/uuid.cc', 'utils/managed_bytes.cc', 'utils/logalloc.cc', 'utils/dynamic_bitset.cc', 'hashers.cc']
deps['test/boost/murmur_hash_test'] = ['bytes.cc', 'utils/murmur_hash.cc', 'test/boost/murmur_hash_test.cc']
deps['test/boost/allocation_strategy_test'] = ['test/boost/allocation_strategy_test.cc', 'utils/logalloc.cc', 'utils/dynamic_bitset.cc']
deps['test/boost/log_heap_test'] = ['test/boost/log_heap_test.cc']
deps['test/boost/estimated_histogram_test'] = ['test/boost/estimated_histogram_test.cc']
deps['test/boost/anchorless_list_test'] = ['test/boost/anchorless_list_test.cc']
deps['test/perf/perf_fast_forward'] += ['release.cc']
deps['test/perf/perf_simple_query'] += ['release.cc']
deps['test/boost/meta_test'] = ['test/boost/meta_test.cc']
deps['test/manual/imr_test'] = ['test/manual/imr_test.cc', 'utils/logalloc.cc', 'utils/dynamic_bitset.cc']
deps['test/boost/reusable_buffer_test'] = [
    "test/boost/reusable_buffer_test.cc",
    "test/lib/log.cc",
]
deps['test/boost/utf8_test'] = ['utils/utf8.cc', 'test/boost/utf8_test.cc']
deps['test/boost/small_vector_test'] = ['test/boost/small_vector_test.cc']
deps['test/boost/multishard_mutation_query_test'] += ['test/boost/test_table.cc']
deps['test/boost/vint_serialization_test'] = ['test/boost/vint_serialization_test.cc', 'vint-serialization.cc', 'bytes.cc']
deps['test/boost/linearizing_input_stream_test'] = [
    "test/boost/linearizing_input_stream_test.cc",
    "test/lib/log.cc",
]

deps['test/boost/duration_test'] += ['test/lib/exception_utils.cc']
deps['test/boost/alternator_base64_test'] += ['alternator/base64.cc']

deps['utils/gz/gen_crc_combine_table'] = ['utils/gz/gen_crc_combine_table.cc']

warnings = [
    '-Wall',
    '-Werror',
    '-Wno-mismatched-tags',  # clang-only
    '-Wno-maybe-uninitialized',  # false positives on gcc 5
    '-Wno-tautological-compare',
    '-Wno-parentheses-equality',
    '-Wno-c++11-narrowing',
    '-Wno-c++1z-extensions',
    '-Wno-sometimes-uninitialized',
    '-Wno-return-stack-address',
    '-Wno-missing-braces',
    '-Wno-unused-lambda-capture',
    '-Wno-misleading-indentation',
    '-Wno-overflow',
    '-Wno-noexcept-type',
    '-Wno-nonnull-compare',
    '-Wno-error=cpp',
    '-Wno-ignored-attributes',
    '-Wno-overloaded-virtual',
    '-Wno-stringop-overflow',
]

warnings = [w
            for w in warnings
            if flag_supported(flag=w, compiler=args.cxx)]

warnings = ' '.join(warnings + ['-Wno-error=deprecated-declarations'])

optimization_flags = [
    '--param inline-unit-growth=300',
]
optimization_flags = [o
                      for o in optimization_flags
                      if flag_supported(flag=o, compiler=args.cxx)]
modes['release']['cxx_ld_flags'] += ' ' + ' '.join(optimization_flags)

linker_flags = linker_flags(compiler=args.cxx)

dbgflag = '-g -gz' if args.debuginfo else ''
tests_link_rule = 'link' if args.tests_debuginfo else 'link_stripped'

# Strip if debuginfo is disabled, otherwise we end up with partial
# debug info from the libraries we static link with
regular_link_rule = 'link' if args.debuginfo else 'link_stripped'

if args.so:
    args.pie = '-shared'
    args.fpie = '-fpic'
elif args.pie:
    args.pie = '-pie'
    args.fpie = '-fpie'
else:
    args.pie = ''
    args.fpie = ''

# a list element means a list of alternative packages to consider
# the first element becomes the HAVE_pkg define
# a string element is a package name with no alternatives
optional_packages = [[]]
pkgs = []

# Lua can be provided by lua53 package on Debian-like
# systems and by Lua on others.
pkgs.append('lua53' if have_pkg('lua53') else 'lua')

pkgs.append('libsystemd')


compiler_test_src = '''
#if __GNUC__ < 8
    #error "MAJOR"
#elif __GNUC__ == 8
    #if __GNUC_MINOR__ < 1
        #error "MINOR"
    #elif __GNUC_MINOR__ == 1
        #if __GNUC_PATCHLEVEL__ < 1
            #error "PATCHLEVEL"
        #endif
    #endif
#endif

int main() { return 0; }
'''
if not try_compile_and_link(compiler=args.cxx, source=compiler_test_src):
    print('Wrong GCC version. Scylla needs GCC >= 8.1.1 to compile.')
    sys.exit(1)

if not try_compile(compiler=args.cxx, source='#include <boost/version.hpp>'):
    print('Boost not installed.  Please install {}.'.format(pkgname("boost-devel")))
    sys.exit(1)

if not try_compile(compiler=args.cxx, source='''\
        #include <boost/version.hpp>
        #if BOOST_VERSION < 105500
        #error Boost version too low
        #endif
        '''):
    print('Installed boost version too old.  Please update {}.'.format(pkgname("boost-devel")))
    sys.exit(1)

if try_compile(args.cxx, source = textwrap.dedent('''\
        #include <lz4.h>

        void m() {
            LZ4_compress_default(static_cast<const char*>(0), static_cast<char*>(0), 0, 0);
        }
        '''), flags=args.user_cflags.split()):
    defines.append("HAVE_LZ4_COMPRESS_DEFAULT")

has_sanitize_address_use_after_scope = try_compile(compiler=args.cxx, flags=['-fsanitize-address-use-after-scope'], source='int f() {}')

defines = ' '.join(['-D' + d for d in defines])

globals().update(vars(args))

total_memory = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
link_pool_depth = max(int(total_memory / 7e9), 1)

selected_modes = args.selected_modes or modes.keys()
default_modes = args.selected_modes or ['debug', 'release', 'dev']
build_modes =  {m: modes[m] for m in selected_modes}
build_artifacts = all_artifacts if not args.artifacts else args.artifacts

status = subprocess.call("./SCYLLA-VERSION-GEN")
if status != 0:
    print('Version file generation failed')
    sys.exit(1)

file = open('build/SCYLLA-VERSION-FILE', 'r')
scylla_version = file.read().strip()
file = open('build/SCYLLA-RELEASE-FILE', 'r')
scylla_release = file.read().strip()

extra_cxxflags["release.cc"] = "-DSCYLLA_VERSION=\"\\\"" + scylla_version + "\\\"\" -DSCYLLA_RELEASE=\"\\\"" + scylla_release + "\\\"\""

for m in ['debug', 'release', 'sanitize']:
    modes[m]['cxxflags'] += ' ' + dbgflag

# The relocatable package includes its own dynamic linker. We don't
# know the path it will be installed to, so for now use a very long
# path so that patchelf doesn't need to edit the program headers.  The
# kernel imposes a limit of 4096 bytes including the null. The other
# constraint is that the build-id has to be in the first page, so we
# can't use all 4096 bytes for the dynamic linker.
# In here we just guess that 2000 extra / should be enough to cover
# any path we get installed to but not so large that the build-id is
# pushed to the second page.
# At the end of the build we check that the build-id is indeed in the
# first page. At install time we check that patchelf doesn't modify
# the program headers.

gcc_linker_output = subprocess.check_output(['gcc', '-###', '/dev/null', '-o', 't'], stderr=subprocess.STDOUT).decode('utf-8')
original_dynamic_linker = re.search('-dynamic-linker ([^ ]*)', gcc_linker_output).groups()[0]
# gdb has a SO_NAME_MAX_PATH_SIZE of 512, so limit the path size to
# that. The 512 includes the null at the end, hence the 511 bellow.
dynamic_linker = '/' * (511 - len(original_dynamic_linker)) + original_dynamic_linker

forced_ldflags = '-Wl,'

# The default build-id used by lld is xxhash, which is 8 bytes long, but RPM
# requires build-ids to be at least 16 bytes long
# (https://github.com/rpm-software-management/rpm/issues/950), so let's
# explicitly ask for SHA1 build-ids.
forced_ldflags += '--build-id=sha1,'

forced_ldflags += f'--dynamic-linker={dynamic_linker}'

args.user_ldflags = forced_ldflags + ' ' + args.user_ldflags

args.user_cflags += ' -Wno-error=stack-usage='

args.user_cflags += f"-ffile-prefix-map={curdir}=."

seastar_cflags = args.user_cflags
if args.target != '':
    seastar_cflags += ' -march=' + args.target
seastar_ldflags = args.user_ldflags

libdeflate_cflags = seastar_cflags

MODE_TO_CMAKE_BUILD_TYPE = {'release' : 'RelWithDebInfo', 'debug' : 'Debug', 'dev' : 'Dev', 'sanitize' : 'Sanitize' }

def configure_seastar(build_dir, mode):
    seastar_build_dir = os.path.join(build_dir, mode, 'seastar')

    seastar_cmake_args = [
        '-DCMAKE_BUILD_TYPE={}'.format(MODE_TO_CMAKE_BUILD_TYPE[mode]),
        '-DCMAKE_C_COMPILER={}'.format(args.cc),
        '-DCMAKE_CXX_COMPILER={}'.format(args.cxx),
        '-DCMAKE_EXPORT_NO_PACKAGE_REGISTRY=ON',
        '-DSeastar_CXX_FLAGS={}'.format((seastar_cflags + ' ' + modes[mode]['cxx_ld_flags']).replace(' ', ';')),
        '-DSeastar_LD_FLAGS={}'.format(seastar_ldflags),
        '-DSeastar_CXX_DIALECT=gnu++20',
        '-DSeastar_API_LEVEL=3',
        '-DSeastar_UNUSED_RESULT_ERROR=ON',
    ]

    if args.stack_guards is not None:
        stack_guards = 'ON' if args.stack_guards else 'OFF'
        seastar_cmake_args += ['-DSeastar_STACK_GUARDS={}'.format(stack_guards)]

    dpdk = args.dpdk
    if dpdk is None:
        dpdk = mode == 'release'
    if dpdk:
        seastar_cmake_args += ['-DSeastar_DPDK=ON', '-DSeastar_DPDK_MACHINE=wsm']
    if args.split_dwarf:
        seastar_cmake_args += ['-DSeastar_SPLIT_DWARF=ON']
    if args.alloc_failure_injector:
        seastar_cmake_args += ['-DSeastar_ALLOC_FAILURE_INJECTION=ON']

    seastar_cmd = ['cmake', '-G', 'Ninja', os.path.relpath(args.seastar_path, seastar_build_dir)] + seastar_cmake_args
    cmake_dir = seastar_build_dir
    if dpdk:
        # need to cook first
        cmake_dir = args.seastar_path # required by cooking.sh
        relative_seastar_build_dir = os.path.join('..', seastar_build_dir)  # relative to seastar/
        seastar_cmd = ['./cooking.sh', '-i', 'dpdk', '-d', relative_seastar_build_dir, '--'] + seastar_cmd[4:]

    print(seastar_cmd)
    os.makedirs(seastar_build_dir, exist_ok=True)
    subprocess.check_call(seastar_cmd, shell=False, cwd=cmake_dir)

for mode in build_modes:
    configure_seastar('build', mode)

pc = {mode: 'build/{}/seastar/seastar.pc'.format(mode) for mode in build_modes}
ninja = find_executable('ninja') or find_executable('ninja-build')
if not ninja:
    print('Ninja executable (ninja or ninja-build) not found on PATH\n')
    sys.exit(1)

def query_seastar_flags(pc_file, link_static_cxx=False):
    cflags = pkg_config(pc_file, '--cflags', '--static')
    libs = pkg_config(pc_file, '--libs', '--static')

    if link_static_cxx:
        libs = libs.replace('-lstdc++ ', '')

    return cflags, libs

for mode in build_modes:
    seastar_pc_cflags, seastar_pc_libs = query_seastar_flags(pc[mode], link_static_cxx=args.staticcxx)
    modes[mode]['seastar_cflags'] = seastar_pc_cflags
    modes[mode]['seastar_libs'] = seastar_pc_libs

def configure_abseil(build_dir, mode):
    abseil_build_dir = os.path.join(build_dir, mode, 'abseil')

    abseil_cflags = seastar_cflags + ' ' + modes[mode]['cxx_ld_flags']
    cmake_mode = MODE_TO_CMAKE_BUILD_TYPE[mode]
    abseil_cmake_args = [
        '-DCMAKE_BUILD_TYPE={}'.format(cmake_mode),
        '-DCMAKE_INSTALL_PREFIX={}'.format(build_dir + '/inst'), # just to avoid a warning from absl
        '-DCMAKE_C_COMPILER={}'.format(args.cc),
        '-DCMAKE_CXX_COMPILER={}'.format(args.cxx),
        '-DCMAKE_CXX_FLAGS_{}={}'.format(cmake_mode.upper(), abseil_cflags),
    ]

    abseil_cmd = ['cmake', '-G', 'Ninja', os.path.relpath('abseil', abseil_build_dir)] + abseil_cmake_args

    os.makedirs(abseil_build_dir, exist_ok=True)
    subprocess.check_call(abseil_cmd, shell=False, cwd=abseil_build_dir)

abseil_libs = ['absl/' + lib for lib in [
    'container/libabsl_hashtablez_sampler.a',
    'container/libabsl_raw_hash_set.a',
    'synchronization/libabsl_synchronization.a',
    'synchronization/libabsl_graphcycles_internal.a',
    'debugging/libabsl_stacktrace.a',
    'debugging/libabsl_symbolize.a',
    'debugging/libabsl_debugging_internal.a',
    'debugging/libabsl_demangle_internal.a',
    'time/libabsl_time.a',
    'time/libabsl_time_zone.a',
    'numeric/libabsl_int128.a',
    'hash/libabsl_city.a',
    'hash/libabsl_hash.a',
    'base/libabsl_malloc_internal.a',
    'base/libabsl_spinlock_wait.a',
    'base/libabsl_base.a',
    'base/libabsl_dynamic_annotations.a',
    'base/libabsl_raw_logging_internal.a',
    'base/libabsl_exponential_biased.a',
    'base/libabsl_throw_delegate.a']]

args.user_cflags += " " + pkg_config('jsoncpp', '--cflags')
args.user_cflags += ' -march=' + args.target
libs = ' '.join([maybe_static(args.staticyamlcpp, '-lyaml-cpp'), '-latomic', '-llz4', '-lz', '-lsnappy', pkg_config('jsoncpp', '--libs'),
                 ' -lstdc++fs', ' -lcrypt', ' -lcryptopp', ' -lpthread',
                 # Must link with static version of libzstd, since
                 # experimental APIs that we use are only present there.
                 maybe_static(True, '-lzstd'),
                 maybe_static(args.staticboost, '-lboost_date_time -lboost_regex -licuuc'), ])

pkgconfig_libs = [
    'libxxhash',
]

args.user_cflags += ' ' + ' '.join([pkg_config(lib, '--cflags') for lib in pkgconfig_libs])
libs += ' ' + ' '.join([pkg_config(lib, '--libs') for lib in pkgconfig_libs])

if not args.staticboost:
    args.user_cflags += ' -DBOOST_TEST_DYN_LINK'

# thrift version detection, see #4538
proc_res = subprocess.run(["thrift", "-version"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
proc_res_output = proc_res.stdout.decode("utf-8")
if proc_res.returncode != 0 and not re.search(r'^Thrift version', proc_res_output):
    raise Exception("Thrift compiler must be missing: {}".format(proc_res_output))

thrift_version = proc_res_output.split(" ")[-1]
thrift_boost_versions = ["0.{}.".format(n) for n in range(1, 11)]
if any(filter(thrift_version.startswith, thrift_boost_versions)):
    args.user_cflags += ' -DTHRIFT_USES_BOOST'

for pkg in pkgs:
    args.user_cflags += ' ' + pkg_config(pkg, '--cflags')
    libs += ' ' + pkg_config(pkg, '--libs')
args.user_cflags += '-I abseil'
user_cflags = args.user_cflags + ' -fvisibility=hidden'
user_ldflags = args.user_ldflags + ' -fvisibility=hidden'
if args.staticcxx:
    user_ldflags += " -static-libstdc++"
if args.staticthrift:
    thrift_libs = "-Wl,-Bstatic -lthrift -Wl,-Bdynamic"
else:
    thrift_libs = "-lthrift"

outdir = 'build'
buildfile = 'build.ninja'

os.makedirs(outdir, exist_ok=True)
do_sanitize = True
if args.static:
    do_sanitize = False

if args.antlr3_exec:
    antlr3_exec = args.antlr3_exec
else:
    antlr3_exec = "antlr3"

if args.ragel_exec:
    ragel_exec = args.ragel_exec
else:
    ragel_exec = "ragel"

for mode in build_modes:
    configure_abseil(outdir, mode)

# configure.py may run automatically from an already-existing build.ninja.
# If the user interrupts configure.py in the middle, we need build.ninja
# to remain in a valid state.  So we write our output to a temporary
# file, and only when done we rename it atomically to build.ninja.
buildfile_tmp = buildfile + ".tmp"

with open(buildfile_tmp, 'w') as f:
    f.write(textwrap.dedent('''\
        configure_args = {configure_args}
        builddir = {outdir}
        cxx = {cxx}
        cxxflags = {user_cflags} {warnings} {defines}
        ldflags = {linker_flags} {user_ldflags}
        ldflags_build = {linker_flags}
        libs = {libs}
        pool link_pool
            depth = {link_pool_depth}
        pool submodule_pool
            depth = 1
        rule gen
            command = echo -e $text > $out
            description = GEN $out
        rule swagger
            command = {args.seastar_path}/scripts/seastar-json2code.py --create-cc -f $in -o $out
            description = SWAGGER $out
        rule serializer
            command = {python} ./idl-compiler.py --ns ser -f $in -o $out
            description = IDL compiler $out
        rule ninja
            command = {ninja} -C $subdir $target
            restat = 1
            description = NINJA $out
        rule ragel
            # sed away a bug in ragel 7 that emits some extraneous _nfa* variables
            # (the $$ is collapsed to a single one by ninja)
            command = {ragel_exec} -G2 -o $out $in && sed -i -e '1h;2,$$H;$$!d;g' -re 's/static const char _nfa[^;]*;//g' $out
            description = RAGEL $out
        rule run
            command = $in > $out
            description = GEN $out
        rule copy
            command = cp $in $out
            description = COPY $out
        rule package
            command = scripts/create-relocatable-package.py --mode $mode $out
        rule rpmbuild
            command = reloc/build_rpm.sh --reloc-pkg $in --builddir $out
        rule debbuild
            command = reloc/build_deb.sh --reloc-pkg $in --builddir $out
        ''').format(**globals()))
    for mode in build_modes:
        modeval = modes[mode]
        fmt_lib = 'fmt'
        f.write(textwrap.dedent('''\
            cxx_ld_flags_{mode} = {cxx_ld_flags}
            ld_flags_{mode} = $cxx_ld_flags_{mode}
            cxxflags_{mode} = $cxx_ld_flags_{mode} {cxxflags} -iquote. -iquote $builddir/{mode}/gen
            libs_{mode} = -l{fmt_lib}
            seastar_libs_{mode} = {seastar_libs}
            rule cxx.{mode}
              command = $cxx -MD -MT $out -MF $out.d {seastar_cflags} $cxxflags_{mode} $cxxflags $obj_cxxflags -c -o $out $in
              description = CXX $out
              depfile = $out.d
            rule link.{mode}
              command = $cxx  $ld_flags_{mode} $ldflags -o $out $in $libs $libs_{mode}
              description = LINK $out
              pool = link_pool
            rule link_stripped.{mode}
              command = $cxx  $ld_flags_{mode} -s $ldflags -o $out $in $libs $libs_{mode}
              description = LINK (stripped) $out
              pool = link_pool
            rule link_build.{mode}
              command = $cxx  $ld_flags_{mode} $ldflags_build -o $out $in $libs $libs_{mode}
              description = LINK (build) $out
              pool = link_pool
            rule ar.{mode}
              command = rm -f $out; ar cr $out $in; ranlib $out
              description = AR $out
            rule thrift.{mode}
                command = thrift -gen cpp:cob_style -out $builddir/{mode}/gen $in
                description = THRIFT $in
            rule antlr3.{mode}
                # We replace many local `ExceptionBaseType* ex` variables with a single function-scope one.
                # Because we add such a variable to every function, and because `ExceptionBaseType` is not a global
                # name, we also add a global typedef to avoid compilation errors.
                command = sed -e '/^#if 0/,/^#endif/d' $in > $builddir/{mode}/gen/$in $
                     && {antlr3_exec} $builddir/{mode}/gen/$in $
                     && sed -i -e '/^.*On :.*$$/d' build/{mode}/gen/${{stem}}Lexer.hpp $
                     && sed -i -e '/^.*On :.*$$/d' build/{mode}/gen/${{stem}}Lexer.cpp $
                     && sed -i -e '/^.*On :.*$$/d' build/{mode}/gen/${{stem}}Parser.hpp $
                     && sed -i -e 's/^\\( *\)\\(ImplTraits::CommonTokenType\\* [a-zA-Z0-9_]* = NULL;\\)$$/\\1const \\2/' $
                        -e '/^.*On :.*$$/d' $
                        -e '1i using ExceptionBaseType = int;' $
                        -e 's/^{{/{{ ExceptionBaseType\* ex = nullptr;/; $
                            s/ExceptionBaseType\* ex = new/ex = new/; $
                            s/exceptions::syntax_exception e/exceptions::syntax_exception\& e/' $
                        build/{mode}/gen/${{stem}}Parser.cpp
                description = ANTLR3 $in
            rule checkhh.{mode}
              command = $cxx -MD -MT $out -MF $out.d {seastar_cflags} $cxxflags $cxxflags_{mode} $obj_cxxflags --include $in -c -o $out build/{mode}/gen/empty.cc
              description = CHECKHH $in
              depfile = $out.d
            rule test.{mode}
              command = ./test.py --mode={mode}
              description = TEST {mode}
            ''').format(mode=mode, antlr3_exec=antlr3_exec, fmt_lib=fmt_lib, **modeval))
        f.write(
            'build {mode}: phony {artifacts}\n'.format(
                mode=mode,
                artifacts=str.join(' ', ('$builddir/' + mode + '/' + x for x in build_artifacts))
            )
        )
        compiles = {}
        swaggers = set()
        serializers = {}
        thrifts = set()
        ragels = {}
        antlr3_grammars = set()
        seastar_dep = 'build/{}/seastar/libseastar.a'.format(mode)
        seastar_testing_dep = 'build/{}/seastar/libseastar_testing.a'.format(mode)
        for binary in build_artifacts:
            if binary in other:
                continue
            srcs = deps[binary]
            objs = ['$builddir/' + mode + '/' + src.replace('.cc', '.o')
                    for src in srcs
                    if src.endswith('.cc')]
            objs.append('$builddir/../utils/arch/powerpc/crc32-vpmsum/crc32.S')
            has_thrift = False
            for dep in deps[binary]:
                if isinstance(dep, Thrift):
                    has_thrift = True
                    objs += dep.objects('$builddir/' + mode + '/gen')
                if isinstance(dep, Antlr3Grammar):
                    objs += dep.objects('$builddir/' + mode + '/gen')
                if isinstance(dep, Json2Code):
                    objs += dep.objects('$builddir/' + mode + '/gen')
            if binary.endswith('.a'):
                f.write('build $builddir/{}/{}: ar.{} {}\n'.format(mode, binary, mode, str.join(' ', objs)))
            else:
                objs.extend(['$builddir/' + mode + '/' + artifact for artifact in [
                    'libdeflate/libdeflate.a',
                ] + [
                    'abseil/' + x for x in abseil_libs
                ]])
                objs.append('$builddir/' + mode + '/gen/utils/gz/crc_combine_table.o')
                if binary in tests:
                    local_libs = '$seastar_libs_{} $libs'.format(mode)
                    if binary in pure_boost_tests:
                        local_libs += ' ' + maybe_static(args.staticboost, '-lboost_unit_test_framework')
                    if binary not in tests_not_using_seastar_test_framework:
                        pc_path = pc[mode].replace('seastar.pc', 'seastar-testing.pc')
                        local_libs += ' ' + pkg_config(pc_path, '--libs', '--static')
                    if has_thrift:
                        local_libs += ' ' + thrift_libs + ' ' + maybe_static(args.staticboost, '-lboost_system')
                    # Our code's debugging information is huge, and multiplied
                    # by many tests yields ridiculous amounts of disk space.
                    # So we strip the tests by default; The user can very
                    # quickly re-link the test unstripped by adding a "_g"
                    # to the test name, e.g., "ninja build/release/testname_g"
                    f.write('build $builddir/{}/{}: {}.{} {} | {} {}\n'.format(mode, binary, tests_link_rule, mode, str.join(' ', objs), seastar_dep, seastar_testing_dep))
                    f.write('   libs = {}\n'.format(local_libs))
                    f.write('build $builddir/{}/{}_g: {}.{} {} | {} {}\n'.format(mode, binary, regular_link_rule, mode, str.join(' ', objs), seastar_dep, seastar_testing_dep))
                    f.write('   libs = {}\n'.format(local_libs))
                else:
                    f.write('build $builddir/{}/{}: {}.{} {} | {}\n'.format(mode, binary, regular_link_rule, mode, str.join(' ', objs), seastar_dep))
                    if has_thrift:
                        f.write('   libs =  {} {} $seastar_libs_{} $libs\n'.format(thrift_libs, maybe_static(args.staticboost, '-lboost_system'), mode))
            for src in srcs:
                if src.endswith('.cc'):
                    obj = '$builddir/' + mode + '/' + src.replace('.cc', '.o')
                    compiles[obj] = src
                elif src.endswith('.idl.hh'):
                    hh = '$builddir/' + mode + '/gen/' + src.replace('.idl.hh', '.dist.hh')
                    serializers[hh] = src
                elif src.endswith('.json'):
                    swaggers.add(src)
                elif src.endswith('.rl'):
                    hh = '$builddir/' + mode + '/gen/' + src.replace('.rl', '.hh')
                    ragels[hh] = src
                elif src.endswith('.thrift'):
                    thrifts.add(src)
                elif src.endswith('.g'):
                    antlr3_grammars.add(src)
                else:
                    raise Exception('No rule for ' + src)
        compiles['$builddir/' + mode + '/gen/utils/gz/crc_combine_table.o'] = '$builddir/' + mode + '/gen/utils/gz/crc_combine_table.cc'
        compiles['$builddir/' + mode + '/utils/gz/gen_crc_combine_table.o'] = 'utils/gz/gen_crc_combine_table.cc'
        f.write('build {}: run {}\n'.format('$builddir/' + mode + '/gen/utils/gz/crc_combine_table.cc',
                                            '$builddir/' + mode + '/utils/gz/gen_crc_combine_table'))
        f.write('build {}: link_build.{} {}\n'.format('$builddir/' + mode + '/utils/gz/gen_crc_combine_table', mode,
                                                '$builddir/' + mode + '/utils/gz/gen_crc_combine_table.o'))
        f.write('   libs = $seastar_libs_{}\n'.format(mode))
        f.write(
            'build {mode}-objects: phony {objs}\n'.format(
                mode=mode,
                objs=' '.join(compiles)
            )
        )
        f.write(
            'build {mode}-headers: phony {header_objs}\n'.format(
                mode=mode,
                header_objs=' '.join(["$builddir/{mode}/{hh}.o".format(mode=mode, hh=hh) for hh in headers])
            )
        )

        f.write(
            'build {mode}-test: test.{mode} {test_executables} $builddir/{mode}/test/tools/cql_repl\n'.format(
                mode=mode,
                test_executables=' '.join(['$builddir/{}/{}'.format(mode, binary) for binary in tests]),
            )
        )
        f.write(
            'build {mode}-check: phony {mode}-headers {mode}-test\n'.format(
                mode=mode,
            )
        )

        gen_dir = '$builddir/{}/gen'.format(mode)
        gen_headers = []
        for th in thrifts:
            gen_headers += th.headers('$builddir/{}/gen'.format(mode))
        for g in antlr3_grammars:
            gen_headers += g.headers('$builddir/{}/gen'.format(mode))
        for g in swaggers:
            gen_headers += g.headers('$builddir/{}/gen'.format(mode))
        gen_headers += list(serializers.keys())
        gen_headers += list(ragels.keys())
        gen_headers_dep = ' '.join(gen_headers)

        for obj in compiles:
            src = compiles[obj]
            f.write('build {}: cxx.{} {} || {} {}\n'.format(obj, mode, src, seastar_dep, gen_headers_dep))
            if src in extra_cxxflags:
                f.write('    cxxflags = {seastar_cflags} $cxxflags $cxxflags_{mode} {extra_cxxflags}\n'.format(mode=mode, extra_cxxflags=extra_cxxflags[src], **modeval))
        for swagger in swaggers:
            hh = swagger.headers(gen_dir)[0]
            cc = swagger.sources(gen_dir)[0]
            obj = swagger.objects(gen_dir)[0]
            src = swagger.source
            f.write('build {} | {} : swagger {} | {}/scripts/seastar-json2code.py\n'.format(hh, cc, src, args.seastar_path))
            f.write('build {}: cxx.{} {}\n'.format(obj, mode, cc))
        for hh in serializers:
            src = serializers[hh]
            f.write('build {}: serializer {} | idl-compiler.py\n'.format(hh, src))
        for hh in ragels:
            src = ragels[hh]
            f.write('build {}: ragel {}\n'.format(hh, src))
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
                f.write('build {}: cxx.{} {} || {}\n'.format(obj, mode, cc, ' '.join(serializers)))
                if cc.endswith('Parser.cpp'):
                    # Unoptimized parsers end up using huge amounts of stack space and overflowing their stack
                    flags = '-O1'
                    if has_sanitize_address_use_after_scope:
                        flags += ' -fno-sanitize-address-use-after-scope'
                    f.write('  obj_cxxflags = %s\n' % flags)
        f.write(f'build build/{mode}/gen/empty.cc: gen\n')
        for hh in headers:
            f.write('build $builddir/{mode}/{hh}.o: checkhh.{mode} {hh} | build/{mode}/gen/empty.cc || {gen_headers_dep}\n'.format(
                    mode=mode, hh=hh, gen_headers_dep=gen_headers_dep))

        f.write('build build/{mode}/seastar/libseastar.a: ninja | always\n'
                .format(**locals()))
        f.write('  pool = submodule_pool\n')
        f.write('  subdir = build/{mode}/seastar\n'.format(**locals()))
        f.write('  target = seastar\n'.format(**locals()))
        f.write('build build/{mode}/seastar/libseastar_testing.a: ninja | always\n'
                .format(**locals()))
        f.write('  pool = submodule_pool\n')
        f.write('  subdir = build/{mode}/seastar\n'.format(**locals()))
        f.write('  target = seastar_testing\n'.format(**locals()))
        f.write('build build/{mode}/seastar/apps/iotune/iotune: ninja\n'
                .format(**locals()))
        f.write('  pool = submodule_pool\n')
        f.write('  subdir = build/{mode}/seastar\n'.format(**locals()))
        f.write('  target = iotune\n'.format(**locals()))
        f.write(textwrap.dedent('''\
            build build/{mode}/iotune: copy build/{mode}/seastar/apps/iotune/iotune
            ''').format(**locals()))
        f.write('build build/{mode}/scylla-package.tar.gz: package build/{mode}/scylla build/{mode}/iotune build/SCYLLA-RELEASE-FILE build/SCYLLA-VERSION-FILE build/debian/debian | always\n'.format(**locals()))
        f.write('  pool = submodule_pool\n')
        f.write('  mode = {mode}\n'.format(**locals()))
        f.write(f'build build/dist/{mode}/redhat: rpmbuild build/{mode}/scylla-package.tar.gz\n')
        f.write(f'  pool = submodule_pool\n')
        f.write(f'  mode = {mode}\n')
        f.write(f'build build/dist/{mode}/debian: debbuild build/{mode}/scylla-package.tar.gz\n')
        f.write(f'  pool = submodule_pool\n')
        f.write(f'  mode = {mode}\n')
        f.write(f'build dist-server-{mode}: phony build/dist/{mode}/redhat build/dist/{mode}/debian\n')
        f.write('rule libdeflate.{mode}\n'.format(**locals()))
        f.write('  command = make -C libdeflate BUILD_DIR=../build/{mode}/libdeflate/ CFLAGS="{libdeflate_cflags}" CC={args.cc} ../build/{mode}/libdeflate//libdeflate.a\n'.format(**locals()))
        f.write('build build/{mode}/libdeflate/libdeflate.a: libdeflate.{mode}\n'.format(**locals()))
        f.write('  pool = submodule_pool\n')

        for lib in abseil_libs:
            f.write('build build/{mode}/abseil/{lib}: ninja\n'.format(**locals()))
            f.write('  pool = submodule_pool\n')
            f.write('  subdir = build/{mode}/abseil\n'.format(**locals()))
            f.write('  target = {lib}\n'.format(**locals()))

    mode = 'dev' if 'dev' in modes else modes[0]
    f.write('build checkheaders: phony || {}\n'.format(' '.join(['$builddir/{}/{}.o'.format(mode, hh) for hh in headers])))

    f.write(
            'build test: phony {}\n'.format(' '.join(['{mode}-test'.format(mode=mode) for mode in modes]))
    )
    f.write(
            'build check: phony {}\n'.format(' '.join(['{mode}-check'.format(mode=mode) for mode in modes]))
    )

    f.write(textwrap.dedent(f'''\
        build dist-server-deb: phony {' '.join(['build/dist/{mode}/debian'.format(mode=mode) for mode in build_modes])}
        build dist-server-rpm: phony {' '.join(['build/dist/{mode}/redhat'.format(mode=mode) for mode in build_modes])}
        build dist-server: phony dist-server-rpm dist-server-deb

        rule build-submodule-reloc
          command = cd $reloc_dir && ./reloc/build_reloc.sh
        rule build-submodule-rpm
          command = cd $dir && ./reloc/build_rpm.sh --reloc-pkg $artifact
        rule build-submodule-deb
          command = cd $dir && ./reloc/build_deb.sh --reloc-pkg $artifact

        build scylla-jmx/build/scylla-jmx-package.tar.gz: build-submodule-reloc
          reloc_dir = scylla-jmx
        build dist-jmx-rpm: build-submodule-rpm scylla-jmx/build/scylla-jmx-package.tar.gz
          dir = scylla-jmx
          artifact = build/scylla-jmx-package.tar.gz
        build dist-jmx-deb: build-submodule-deb scylla-jmx/build/scylla-jmx-package.tar.gz
          dir = scylla-jmx
          artifact = build/scylla-jmx-package.tar.gz
        build dist-jmx: phony dist-jmx-rpm dist-jmx-deb

        build scylla-tools/build/scylla-tools-package.tar.gz: build-submodule-reloc
          reloc_dir = scylla-tools
        build dist-tools-rpm: build-submodule-rpm scylla-tools/build/scylla-tools-package.tar.gz
          dir = scylla-tools
          artifact = build/scylla-tools-package.tar.gz
        build dist-tools-deb: build-submodule-deb scylla-tools/build/scylla-tools-package.tar.gz
          dir = scylla-tools
          artifact = build/scylla-tools-package.tar.gz
        build dist-tools: phony dist-tools-rpm dist-tools-deb

        rule build-python-reloc
          command = ./reloc/python3/build_reloc.sh
        rule build-python-rpm
          command = ./reloc/python3/build_rpm.sh
        rule build-python-deb
          command = ./reloc/python3/build_deb.sh

        build build/release/scylla-python3-package.tar.gz: build-python-reloc
        build dist-python-rpm: build-python-rpm build/release/scylla-python3-package.tar.gz
        build dist-python-deb: build-python-deb build/release/scylla-python3-package.tar.gz
        build dist-python: phony dist-python-rpm dist-python-deb
        build dist-deb: phony dist-server-deb dist-python-deb dist-jmx-deb dist-tools-deb
        build dist-rpm: phony dist-server-rpm dist-python-rpm dist-jmx-rpm dist-tools-rpm
        build dist: phony dist-server dist-python dist-jmx dist-tools
        '''))

    f.write(textwrap.dedent(f'''\
        build dist-check: phony {' '.join(['dist-check-{mode}'.format(mode=mode) for mode in build_modes])}
        rule dist-check
          command = ./tools/testing/dist-check/dist-check.sh --mode $mode
        '''))
    for mode in build_modes:
        f.write(textwrap.dedent(f'''\
        build dist-check-{mode}: dist-check
          mode = {mode}
            '''))

    f.write(textwrap.dedent('''\
        rule configure
          command = {python} configure.py $configure_args
          generator = 1
        build build.ninja: configure | configure.py SCYLLA-VERSION-GEN {args.seastar_path}/CMakeLists.txt
        rule cscope
            command = find -name '*.[chS]' -o -name "*.cc" -o -name "*.hh" | cscope -bq -i-
            description = CSCOPE
        build cscope: cscope
        rule clean
            command = rm -rf build
            description = CLEAN
        build clean: clean
        rule mode_list
            command = echo {modes_list}
            description = List configured modes
        build mode_list: mode_list
        default {modes_list}
        ''').format(modes_list=' '.join(default_modes), **globals()))
    f.write(textwrap.dedent('''\
        build always: phony
        rule scylla_version_gen
            command = ./SCYLLA-VERSION-GEN
        build build/SCYLLA-RELEASE-FILE build/SCYLLA-VERSION-FILE: scylla_version_gen
        rule debian_files_gen
            command = ./dist/debian/debian_files_gen.py
        build build/debian/debian: debian_files_gen | always
        ''').format(modes_list=' '.join(build_modes), **globals()))

os.rename(buildfile_tmp, buildfile)
