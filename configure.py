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


def gold_supported(compiler):
    src_main = 'int main(int argc, char **argv) { return 0; }'
    if try_compile_and_link(source=src_main, flags=['-fuse-ld=gold'], compiler=compiler):
        return '-fuse-ld=gold'
    else:
        print('Note: gold not found; using default system linker')
        return ''


def maybe_static(flag, libs):
    if flag and not args.static:
        libs = '-Wl,-Bstatic {} -Wl,-Bdynamic'.format(libs)
    return libs


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


def default_target_arch():
    if platform.machine() in ['i386', 'i686', 'x86_64']:
        return 'westmere'   # support PCLMUL
    elif platform.machine() == 'aarch64':
        return 'armv8-a+crc+crypto'
    else:
        return ''


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
        'cxxflags': '-DDEBUG -DDEBUG_LSA_SANITIZER',
        'cxx_ld_flags': '',
    },
    'release': {
        'cxxflags': '',
        'cxx_ld_flags': '-O3',
    },
    'dev': {
        'cxxflags': '',
        'cxx_ld_flags': '-O1',
    },
    'sanitize': {
        'cxxflags': '-DDEBUG -DDEBUG_LSA_SANITIZER',
        'cxx_ld_flags': '-Os',
    }
}

scylla_tests = [
    'tests/mutation_test',
    'tests/mvcc_test',
    'tests/mutation_fragment_test',
    'tests/flat_mutation_reader_test',
    'tests/schema_registry_test',
    'tests/canonical_mutation_test',
    'tests/range_test',
    'tests/types_test',
    'tests/keys_test',
    'tests/partitioner_test',
    'tests/frozen_mutation_test',
    'tests/serialized_action_test',
    'tests/hint_test',
    'tests/clustering_ranges_walker_test',
    'tests/perf/perf_mutation',
    'tests/lsa_async_eviction_test',
    'tests/lsa_sync_eviction_test',
    'tests/row_cache_alloc_stress',
    'tests/perf_row_cache_update',
    'tests/perf/perf_hash',
    'tests/perf/perf_cql_parser',
    'tests/perf/perf_simple_query',
    'tests/perf/perf_fast_forward',
    'tests/perf/perf_cache_eviction',
    'tests/cache_flat_mutation_reader_test',
    'tests/row_cache_stress_test',
    'tests/memory_footprint',
    'tests/perf/perf_sstable',
    'tests/cql_query_test',
    'tests/secondary_index_test',
    'tests/json_cql_query_test',
    'tests/filtering_test',
    'tests/storage_proxy_test',
    'tests/schema_change_test',
    'tests/mutation_reader_test',
    'tests/mutation_query_test',
    'tests/row_cache_test',
    'tests/test-serialization',
    'tests/broken_sstable_test',
    'tests/sstable_test',
    'tests/sstable_datafile_test',
    'tests/sstable_3_x_test',
    'tests/sstable_mutation_test',
    'tests/sstable_resharding_test',
    'tests/memtable_test',
    'tests/commitlog_test',
    'tests/cartesian_product_test',
    'tests/hash_test',
    'tests/map_difference_test',
    'tests/message',
    'tests/gossip',
    'tests/gossip_test',
    'tests/compound_test',
    'tests/config_test',
    'tests/gossiping_property_file_snitch_test',
    'tests/ec2_snitch_test',
    'tests/gce_snitch_test',
    'tests/snitch_reset_test',
    'tests/network_topology_strategy_test',
    'tests/query_processor_test',
    'tests/batchlog_manager_test',
    'tests/bytes_ostream_test',
    'tests/UUID_test',
    'tests/murmur_hash_test',
    'tests/allocation_strategy_test',
    'tests/logalloc_test',
    'tests/log_heap_test',
    'tests/managed_vector_test',
    'tests/crc_test',
    'tests/checksum_utils_test',
    'tests/flush_queue_test',
    'tests/dynamic_bitset_test',
    'tests/auth_test',
    'tests/idl_test',
    'tests/range_tombstone_list_test',
    'tests/anchorless_list_test',
    'tests/database_test',
    'tests/nonwrapping_range_test',
    'tests/input_stream_test',
    'tests/virtual_reader_test',
    'tests/view_schema_test',
    'tests/view_build_test',
    'tests/view_complex_test',
    'tests/counter_test',
    'tests/cell_locker_test',
    'tests/row_locker_test',
    'tests/streaming_histogram_test',
    'tests/duration_test',
    'tests/vint_serialization_test',
    'tests/continuous_data_consumer_test',
    'tests/compress_test',
    'tests/chunked_vector_test',
    'tests/loading_cache_test',
    'tests/castas_fcts_test',
    'tests/big_decimal_test',
    'tests/aggregate_fcts_test',
    'tests/role_manager_test',
    'tests/caching_options_test',
    'tests/auth_resource_test',
    'tests/cql_auth_query_test',
    'tests/enum_set_test',
    'tests/extensions_test',
    'tests/cql_auth_syntax_test',
    'tests/querier_cache',
    'tests/limiting_data_source_test',
    'tests/meta_test',
    'tests/imr_test',
    'tests/partition_data_test',
    'tests/reusable_buffer_test',
    'tests/mutation_writer_test',
    'tests/observable_test',
    'tests/transport_test',
    'tests/fragmented_temporary_buffer_test',
    'tests/json_test',
    'tests/auth_passwords_test',
    'tests/multishard_mutation_query_test',
    'tests/top_k_test',
    'tests/utf8_test',
    'tests/small_vector_test',
    'tests/data_listeners_test',
    'tests/truncation_migration_test',
    'tests/like_matcher_test',
]

perf_tests = [
    'tests/perf/perf_mutation_readers',
    'tests/perf/perf_checksum',
    'tests/perf/perf_mutation_fragment',
    'tests/perf/perf_idl',
    'tests/perf/perf_vint',
]

apps = [
    'scylla',
]

tests = scylla_tests + perf_tests

other = [
    'iotune',
]

all_artifacts = apps + tests + other

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
arg_parser.add_argument('--enable-dpdk', action='store_true', dest='dpdk', default=False,
                        help='Enable dpdk (from seastar dpdk sources)')
arg_parser.add_argument('--dpdk-target', action='store', dest='dpdk_target', default='',
                        help='Path to DPDK SDK target location (e.g. <DPDK SDK dir>/x86_64-native-linuxapp-gcc)')
arg_parser.add_argument('--debuginfo', action='store', dest='debuginfo', type=int, default=1,
                        help='Enable(1)/disable(0)compiler debug information generation')
arg_parser.add_argument('--compress-exec-debuginfo', action='store', dest='compress_exec_debuginfo', type=int, default=1,
                        help='Enable(1)/disable(0) debug information compression in executables')
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
add_tristate(arg_parser, name='hwloc', dest='hwloc', help='hwloc support')
add_tristate(arg_parser, name='xen', dest='xen', help='Xen support')
arg_parser.add_argument('--split-dwarf', dest='split_dwarf', action='store_true', default=False,
                        help='use of split dwarf (https://gcc.gnu.org/wiki/DebugFission) to speed up linking')
arg_parser.add_argument('--enable-gcc6-concepts', dest='gcc6_concepts', action='store_true', default=False,
                        help='enable experimental support for C++ Concepts as implemented in GCC 6')
arg_parser.add_argument('--enable-alloc-failure-injector', dest='alloc_failure_injector', action='store_true', default=False,
                        help='enable allocation failure injection')
arg_parser.add_argument('--with-antlr3', dest='antlr3_exec', action='store', default=None,
                        help='path to antlr3 executable')
args = arg_parser.parse_args()

defines = ['XXH_PRIVATE_API',
           'SEASTAR_TESTING_MAIN',
]

extra_cxxflags = {}

cassandra_interface = Thrift(source='interface/cassandra.thrift', service='Cassandra')

scylla_core = (['database.cc',
                'table.cc',
                'atomic_cell.cc',
                'hashers.cc',
                'schema.cc',
                'frozen_schema.cc',
                'schema_registry.cc',
                'bytes.cc',
                'mutation.cc',
                'mutation_fragment.cc',
                'partition_version.cc',
                'row_cache.cc',
                'canonical_mutation.cc',
                'frozen_mutation.cc',
                'memtable.cc',
                'schema_mutations.cc',
                'supervisor.cc',
                'utils/logalloc.cc',
                'utils/large_bitset.cc',
                'utils/buffer_input_stream.cc',
                'utils/limiting_data_source.cc',
                'mutation_partition.cc',
                'mutation_partition_view.cc',
                'mutation_partition_serializer.cc',
                'mutation_reader.cc',
                'flat_mutation_reader.cc',
                'mutation_query.cc',
                'json.cc',
                'keys.cc',
                'counters.cc',
                'compress.cc',
                'sstables/mp_row_consumer.cc',
                'sstables/sstables.cc',
                'sstables/sstables_manager.cc',
                'sstables/mc/writer.cc',
                'sstables/sstable_version.cc',
                'sstables/compress.cc',
                'sstables/partition.cc',
                'sstables/compaction.cc',
                'sstables/compaction_strategy.cc',
                'sstables/compaction_manager.cc',
                'sstables/integrity_checked_file_impl.cc',
                'sstables/prepended_input_stream.cc',
                'sstables/m_format_read_helpers.cc',
                'transport/event.cc',
                'transport/event_notifier.cc',
                'transport/server.cc',
                'transport/messages/result_message.cc',
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
                'cql3/functions/functions.cc',
                'cql3/functions/castas_fcts.cc',
                'cql3/statements/cf_prop_defs.cc',
                'cql3/statements/cf_statement.cc',
                'cql3/statements/authentication_statement.cc',
                'cql3/statements/create_keyspace_statement.cc',
                'cql3/statements/create_table_statement.cc',
                'cql3/statements/create_view_statement.cc',
                'cql3/statements/create_type_statement.cc',
                'cql3/statements/drop_index_statement.cc',
                'cql3/statements/drop_keyspace_statement.cc',
                'cql3/statements/drop_table_statement.cc',
                'cql3/statements/drop_view_statement.cc',
                'cql3/statements/drop_type_statement.cc',
                'cql3/statements/schema_altering_statement.cc',
                'cql3/statements/ks_prop_defs.cc',
                'cql3/statements/modification_statement.cc',
                'cql3/statements/parsed_statement.cc',
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
                'utils/gz/crc_combine.cc',
                'gms/version_generator.cc',
                'gms/versioned_value.cc',
                'gms/gossiper.cc',
                'gms/failure_detector.cc',
                'gms/gossip_digest_syn.cc',
                'gms/gossip_digest_ack.cc',
                'gms/gossip_digest_ack2.cc',
                'gms/endpoint_state.cc',
                'gms/application_state.cc',
                'gms/inet_address.cc',
                'dht/i_partitioner.cc',
                'dht/murmur3_partitioner.cc',
                'dht/byte_ordered_partitioner.cc',
                'dht/random_partitioner.cc',
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
                'table_helper.cc',
                'range_tombstone.cc',
                'range_tombstone_list.cc',
                'disk-error-handler.cc',
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
                'mutation_writer/timestamp_based_splitting_writer.cc',
                ] + [Antlr3Grammar('cql3/Cql.g')] + [Thrift('interface/cassandra.thrift', 'Cassandra')]
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
       'api/api-doc/stream_manager.json',
       'api/stream_manager.cc',
       'api/api-doc/system.json',
       'api/system.cc',
       'api/config.cc',
       'api/api-doc/config.json',
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
        ]

headers = find_headers('.', excluded_dirs=['idl', 'build', 'seastar', '.git'])

scylla_tests_generic_dependencies = [
    'tests/cql_test_env.cc',
    'tests/test_services.cc',
]

scylla_tests_dependencies = scylla_core + idls + scylla_tests_generic_dependencies + [
    'tests/cql_assertions.cc',
    'tests/result_set_assertions.cc',
    'tests/mutation_source_test.cc',
    'tests/data_model.cc',
    'tests/exception_utils.cc',
    'tests/random_schema.cc',
]

deps = {
    'scylla': idls + ['main.cc', 'release.cc'] + scylla_core + api,
}

pure_boost_tests = set([
    'tests/map_difference_test',
    'tests/keys_test',
    'tests/compound_test',
    'tests/range_tombstone_list_test',
    'tests/anchorless_list_test',
    'tests/nonwrapping_range_test',
    'tests/test-serialization',
    'tests/range_test',
    'tests/crc_test',
    'tests/checksum_utils_test',
    'tests/managed_vector_test',
    'tests/dynamic_bitset_test',
    'tests/idl_test',
    'tests/cartesian_product_test',
    'tests/streaming_histogram_test',
    'tests/duration_test',
    'tests/vint_serialization_test',
    'tests/compress_test',
    'tests/chunked_vector_test',
    'tests/big_decimal_test',
    'tests/caching_options_test',
    'tests/auth_resource_test',
    'tests/enum_set_test',
    'tests/cql_auth_syntax_test',
    'tests/meta_test',
    'tests/observable_test',
    'tests/json_test',
    'tests/auth_passwords_test',
    'tests/top_k_test',
    'tests/small_vector_test',
    'tests/like_matcher_test',
])

tests_not_using_seastar_test_framework = set([
    'tests/perf/perf_mutation',
    'tests/lsa_async_eviction_test',
    'tests/lsa_sync_eviction_test',
    'tests/row_cache_alloc_stress',
    'tests/perf_row_cache_update',
    'tests/perf/perf_hash',
    'tests/perf/perf_cql_parser',
    'tests/message',
    'tests/perf/perf_cache_eviction',
    'tests/row_cache_stress_test',
    'tests/memory_footprint',
    'tests/gossip',
    'tests/perf/perf_sstable',
    'tests/small_vector_test',
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

deps['tests/sstable_test'] += ['tests/sstable_utils.cc', 'tests/normalizing_reader.cc']
deps['tests/sstable_datafile_test'] += ['tests/sstable_utils.cc', 'tests/normalizing_reader.cc']
deps['tests/mutation_reader_test'] += ['tests/sstable_utils.cc']

deps['tests/bytes_ostream_test'] = ['tests/bytes_ostream_test.cc', 'utils/managed_bytes.cc', 'utils/logalloc.cc', 'utils/dynamic_bitset.cc']
deps['tests/input_stream_test'] = ['tests/input_stream_test.cc']
deps['tests/UUID_test'] = ['utils/UUID_gen.cc', 'tests/UUID_test.cc', 'utils/uuid.cc', 'utils/managed_bytes.cc', 'utils/logalloc.cc', 'utils/dynamic_bitset.cc', 'hashers.cc']
deps['tests/murmur_hash_test'] = ['bytes.cc', 'utils/murmur_hash.cc', 'tests/murmur_hash_test.cc']
deps['tests/allocation_strategy_test'] = ['tests/allocation_strategy_test.cc', 'utils/logalloc.cc', 'utils/dynamic_bitset.cc']
deps['tests/log_heap_test'] = ['tests/log_heap_test.cc']
deps['tests/anchorless_list_test'] = ['tests/anchorless_list_test.cc']
deps['tests/perf/perf_fast_forward'] += ['release.cc']
deps['tests/perf/perf_simple_query'] += ['release.cc']
deps['tests/meta_test'] = ['tests/meta_test.cc']
deps['tests/imr_test'] = ['tests/imr_test.cc', 'utils/logalloc.cc', 'utils/dynamic_bitset.cc']
deps['tests/reusable_buffer_test'] = ['tests/reusable_buffer_test.cc']
deps['tests/utf8_test'] = ['utils/utf8.cc', 'tests/utf8_test.cc']
deps['tests/small_vector_test'] = ['tests/small_vector_test.cc']
deps['tests/multishard_mutation_query_test'] += ['tests/test_table.cc']
deps['tests/vint_serialization_test'] = ['tests/vint_serialization_test.cc', 'vint-serialization.cc', 'bytes.cc']

deps['tests/duration_test'] += ['tests/exception_utils.cc']

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

gold_linker_flag = gold_supported(compiler=args.cxx)

dbgflag = '-g' if args.debuginfo else ''
tests_link_rule = 'link' if args.tests_debuginfo else 'link_stripped'

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
optional_packages = [['libsystemd', 'libsystemd-daemon']]
pkgs = []


def setup_first_pkg_of_list(pkglist):
    # The HAVE_pkg symbol is taken from the first alternative
    upkg = pkglist[0].upper().replace('-', '_')
    for pkg in pkglist:
        if have_pkg(pkg):
            pkgs.append(pkg)
            defines.append('HAVE_{}=1'.format(upkg))
            return True
    return False


for pkglist in optional_packages:
    if isinstance(pkglist, str):
        pkglist = [pkglist]
    if not setup_first_pkg_of_list(pkglist):
        if len(pkglist) == 1:
            print('Missing optional package {pkglist[0]}'.format(**locals()))
        else:
            alternatives = ':'.join(pkglist[1:])
            print('Missing optional package {pkglist[0]} (or alteratives {alternatives})'.format(**locals()))


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

seastar_flags = []
if args.dpdk:
    # fake dependencies on dpdk, so that it is built before anything else
    seastar_flags += ['--enable-dpdk']
if args.gcc6_concepts:
    seastar_flags += ['--enable-gcc6-concepts']
if args.alloc_failure_injector:
    seastar_flags += ['--enable-alloc-failure-injector']
if args.split_dwarf:
    seastar_flags += ['--split-dwarf']

# We never compress debug info in debug mode
modes['debug']['cxxflags'] += ' -gz'
# We compress it by default in release mode
flag_dest = 'cxx_ld_flags' if args.compress_exec_debuginfo else 'cxxflags'
modes['release'][flag_dest] += ' -gz'

for m in ['debug', 'release', 'sanitize']:
    modes[m]['cxxflags'] += ' ' + dbgflag

seastar_cflags = args.user_cflags
seastar_cflags += ' -Wno-error'
if args.target != '':
    seastar_cflags += ' -march=' + args.target
seastar_ldflags = args.user_ldflags
seastar_flags += ['--compiler', args.cxx, '--c-compiler', args.cc, '--cflags=%s' % (seastar_cflags), '--ldflags=%s' % (seastar_ldflags),
                  '--c++-dialect=gnu++17', '--use-std-optional-variant-stringview=1', '--optflags=%s' % (modes['release']['cxx_ld_flags']), ]

libdeflate_cflags = seastar_cflags

status = subprocess.call([args.python, './configure.py'] + seastar_flags, cwd='seastar')

if status != 0:
    print('Seastar configuration failed')
    sys.exit(1)


pc = {mode: 'build/{}/seastar.pc'.format(mode) for mode in build_modes}
ninja = find_executable('ninja') or find_executable('ninja-build')
if not ninja:
    print('Ninja executable (ninja or ninja-build) not found on PATH\n')
    sys.exit(1)

def query_seastar_flags(seastar_pc_file, link_static_cxx=False):
    pc_file = os.path.join('seastar', seastar_pc_file)
    cflags = pkg_config(pc_file, '--cflags', '--static')
    libs = pkg_config(pc_file, '--libs', '--static')

    if link_static_cxx:
        libs = libs.replace('-lstdc++ ', '')

    return cflags, libs

for mode in build_modes:
    seastar_cflags, seastar_libs = query_seastar_flags(pc[mode], link_static_cxx=args.staticcxx)
    modes[mode]['seastar_cflags'] = seastar_cflags
    modes[mode]['seastar_libs'] = seastar_libs

args.user_cflags += " " + pkg_config('jsoncpp', '--cflags')
args.user_cflags += ' -march=' + args.target
libs = ' '.join([maybe_static(args.staticyamlcpp, '-lyaml-cpp'), '-latomic', '-llz4', '-lz', '-lsnappy', pkg_config('jsoncpp', '--libs'),
                 ' -lstdc++fs', ' -lcrypt', ' -lcryptopp', ' -lpthread',
                 maybe_static(args.staticboost, '-lboost_date_time -lboost_regex -licuuc'), ])

xxhash_dir = 'xxHash'

if not os.path.exists(xxhash_dir) or not os.listdir(xxhash_dir):
    raise Exception(xxhash_dir + ' is empty. Run "git submodule update --init".')

if not args.staticboost:
    args.user_cflags += ' -DBOOST_TEST_DYN_LINK'

# thrift version detection, see #4538
thrift_version = subprocess.check_output(["thrift", "-version"]).decode("utf-8").split(" ")[-1]
thrift_boost_versions = ["0.{}.".format(n) for n in range(1, 11)]
if any(filter(thrift_version.startswith, thrift_boost_versions)):
    args.user_cflags += ' -DTHRIFT_USES_BOOST'

for pkg in pkgs:
    args.user_cflags += ' ' + pkg_config(pkg, '--cflags')
    libs += ' ' + pkg_config(pkg, '--libs')
user_cflags = args.user_cflags + ' -fvisibility=hidden'
user_ldflags = args.user_ldflags + ' -fvisibility=hidden'
if args.staticcxx:
    user_ldflags += " -static-libgcc -static-libstdc++"
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

with open(buildfile, 'w') as f:
    f.write(textwrap.dedent('''\
        configure_args = {configure_args}
        builddir = {outdir}
        cxx = {cxx}
        cxxflags = {user_cflags} {warnings} {defines}
        ldflags = {gold_linker_flag} {user_ldflags}
        libs = {libs}
        pool link_pool
            depth = {link_pool_depth}
        pool seastar_pool
            depth = 1
        rule gen
            command = echo -e $text > $out
            description = GEN $out
        rule swagger
            command = seastar/scripts/seastar-json2code.py -f $in -o $out
            description = SWAGGER $out
        rule serializer
            command = {python} ./idl-compiler.py --ns ser -f $in -o $out
            description = IDL compiler $out
        rule ninja
            command = {ninja} -C $subdir $target
            restat = 1
            description = NINJA $out
        rule run
            command = $in > $out
            description = GEN $out
        rule copy
            command = cp $in $out
            description = COPY $out
        rule package
            command = scripts/create-relocatable-package.py --mode $mode $out
        ''').format(**globals()))
    for mode in build_modes:
        modeval = modes[mode]
        fmt_lib = 'fmt'
        f.write(textwrap.dedent('''\
            cxx_ld_flags_{mode} = {cxx_ld_flags}
            ld_flags_{mode} = $cxx_ld_flags_{mode}
            cxxflags_{mode} = $cxx_ld_flags_{mode} {cxxflags} -I. -I $builddir/{mode}/gen
            libs_{mode} = -l{fmt_lib}
            seastar_libs_{mode} = {seastar_libs}
            rule cxx.{mode}
              command = $cxx -MD -MT $out -MF $out.d {seastar_cflags} $cxxflags $cxxflags_{mode} $obj_cxxflags -c -o $out $in
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
                     && sed -i -e 's/^\\( *\)\\(ImplTraits::CommonTokenType\\* [a-zA-Z0-9_]* = NULL;\\)$$/\\1const \\2/' $
                        -e '1i using ExceptionBaseType = int;' $
                        -e 's/^{{/{{ ExceptionBaseType\* ex = nullptr;/; $
                            s/ExceptionBaseType\* ex = new/ex = new/; $
                            s/exceptions::syntax_exception e/exceptions::syntax_exception\& e/' $
                        build/{mode}/gen/${{stem}}Parser.cpp
                description = ANTLR3 $in
            rule checkhh.{mode}
              command = $cxx -MD -MT $out -MF $out.d {seastar_cflags} $cxxflags $cxxflags_{mode} $obj_cxxflags -x c++ --include=$in -c -o $out /dev/null
              description = CHECKHH $in
              depfile = $out.d
            ''').format(mode=mode, antlr3_exec=antlr3_exec, fmt_lib=fmt_lib, **modeval))
        f.write(
            'build {mode}: phony {artifacts}\n'.format(
                mode=mode,
                artifacts=str.join(' ', ('$builddir/' + mode + '/' + x for x in build_artifacts))
            )
        )
        compiles = {}
        swaggers = {}
        serializers = {}
        thrifts = set()
        antlr3_grammars = set()
        seastar_dep = 'seastar/build/{}/libseastar.a'.format(mode)
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
            if binary.endswith('.a'):
                f.write('build $builddir/{}/{}: ar.{} {}\n'.format(mode, binary, mode, str.join(' ', objs)))
            else:
                objs.extend(['$builddir/' + mode + '/' + artifact for artifact in [
                    'libdeflate/libdeflate.a'
                ]])
                objs.append('$builddir/' + mode + '/gen/utils/gz/crc_combine_table.o')
                if binary.startswith('tests/'):
                    local_libs = '$seastar_libs_{} $libs'.format(mode)
                    if binary in pure_boost_tests:
                        local_libs += ' ' + maybe_static(args.staticboost, '-lboost_unit_test_framework')
                    if binary not in tests_not_using_seastar_test_framework:
                        pc_path = os.path.join('seastar', pc[mode].replace('seastar.pc', 'seastar-testing.pc'))
                        local_libs += ' ' + pkg_config(pc_path, '--libs', '--static')
                    if has_thrift:
                        local_libs += ' ' + thrift_libs + ' ' + maybe_static(args.staticboost, '-lboost_system')
                    # Our code's debugging information is huge, and multiplied
                    # by many tests yields ridiculous amounts of disk space.
                    # So we strip the tests by default; The user can very
                    # quickly re-link the test unstripped by adding a "_g"
                    # to the test name, e.g., "ninja build/release/testname_g"
                    f.write('build $builddir/{}/{}: {}.{} {} | {}\n'.format(mode, binary, tests_link_rule, mode, str.join(' ', objs), seastar_dep))
                    f.write('   libs = {}\n'.format(local_libs))
                    f.write('build $builddir/{}/{}_g: link.{} {} | {}\n'.format(mode, binary, mode, str.join(' ', objs), seastar_dep))
                    f.write('   libs = {}\n'.format(local_libs))
                else:
                    f.write('build $builddir/{}/{}: link.{} {} | {}\n'.format(mode, binary, mode, str.join(' ', objs), seastar_dep))
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
                    hh = '$builddir/' + mode + '/gen/' + src + '.hh'
                    swaggers[hh] = src
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
        f.write('build {}: link.{} {}\n'.format('$builddir/' + mode + '/utils/gz/gen_crc_combine_table', mode,
                                                '$builddir/' + mode + '/utils/gz/gen_crc_combine_table.o'))
        f.write('   libs = $seastar_libs_{}\n'.format(mode))
        f.write(
            'build {mode}-objects: phony {objs}\n'.format(
                mode=mode,
                objs=' '.join(compiles)
            )
        )


        gen_headers = []
        for th in thrifts:
            gen_headers += th.headers('$builddir/{}/gen'.format(mode))
        for g in antlr3_grammars:
            gen_headers += g.headers('$builddir/{}/gen'.format(mode))
        gen_headers += list(swaggers.keys())
        gen_headers += list(serializers.keys())
        gen_headers_dep = ' '.join(gen_headers)

        for obj in compiles:
            src = compiles[obj]
            f.write('build {}: cxx.{} {} || {} {}\n'.format(obj, mode, src, seastar_dep, gen_headers_dep))
            if src in extra_cxxflags:
                f.write('    cxxflags = {seastar_cflags} $cxxflags $cxxflags_{mode} {extra_cxxflags}\n'.format(mode=mode, extra_cxxflags=extra_cxxflags[src], **modeval))
        for hh in swaggers:
            src = swaggers[hh]
            f.write('build {}: swagger {} | seastar/scripts/seastar-json2code.py\n'.format(hh, src))
        for hh in serializers:
            src = serializers[hh]
            f.write('build {}: serializer {} | idl-compiler.py\n'.format(hh, src))
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
                if cc.endswith('Parser.cpp') and has_sanitize_address_use_after_scope:
                    # Parsers end up using huge amounts of stack space and overflowing their stack
                    f.write('  obj_cxxflags = -fno-sanitize-address-use-after-scope\n')
        for hh in headers:
            f.write('build $builddir/{mode}/{hh}.o: checkhh.{mode} {hh} || {gen_headers_dep}\n'.format(
                    mode=mode, hh=hh, gen_headers_dep=gen_headers_dep))

        f.write('build seastar/build/{mode}/libseastar.a: ninja | always\n'
                .format(**locals()))
        f.write('  pool = seastar_pool\n')
        f.write('  subdir = seastar/build/{mode}\n'.format(**locals()))
        f.write('  target = seastar seastar_testing\n'.format(**locals()))
        f.write('build seastar/build/{mode}/apps/iotune/iotune: ninja\n'
                .format(**locals()))
        f.write('  pool = seastar_pool\n')
        f.write('  subdir = seastar/build/{mode}\n'.format(**locals()))
        f.write('  target = iotune\n'.format(**locals()))
        f.write(textwrap.dedent('''\
            build build/{mode}/iotune: copy seastar/build/{mode}/apps/iotune/iotune
            ''').format(**locals()))
        f.write('build build/{mode}/scylla-package.tar.gz: package build/{mode}/scylla build/{mode}/iotune build/SCYLLA-RELEASE-FILE build/SCYLLA-VERSION-FILE | always\n'.format(**locals()))
        f.write('    mode = {mode}\n'.format(**locals()))
        f.write('rule libdeflate.{mode}\n'.format(**locals()))
        f.write('    command = make -C libdeflate BUILD_DIR=../build/{mode}/libdeflate/ CFLAGS="{libdeflate_cflags}" CC={args.cc} ../build/{mode}/libdeflate//libdeflate.a\n'.format(**locals()))
        f.write('build build/{mode}/libdeflate/libdeflate.a: libdeflate.{mode}\n'.format(**locals()))

    mode = 'dev' if 'dev' in modes else modes[0]
    f.write('build checkheaders: phony || {}\n'.format(' '.join(['$builddir/{}/{}.o'.format(mode, hh) for hh in headers])))

    f.write(textwrap.dedent('''\
        rule configure
          command = {python} configure.py $configure_args
          generator = 1
        build build.ninja: configure | configure.py seastar/configure.py
        rule cscope
            command = find -name '*.[chS]' -o -name "*.cc" -o -name "*.hh" | cscope -bq -i-
            description = CSCOPE
        build cscope: cscope
        rule clean
            command = rm -rf build
            description = CLEAN
        build clean: clean
        default {modes_list}
        ''').format(modes_list=' '.join(default_modes), **globals()))
    f.write(textwrap.dedent('''\
        build always: phony
        rule scylla_version_gen
            command = ./SCYLLA-VERSION-GEN
        build build/SCYLLA-RELEASE-FILE build/SCYLLA-VERSION-FILE: scylla_version_gen
        ''').format(modes_list=' '.join(build_modes), **globals()))
