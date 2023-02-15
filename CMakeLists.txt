cmake_minimum_required(VERSION 3.18)

project(scylla)

list(APPEND CMAKE_MODULE_PATH
  ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE
    STRING "Choose the type of build." FORCE)
# Set the possible values of build type for cmake-gui
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
  "Debug" "Release" "Dev" "Sanitize")
string(TOUPPER "${CMAKE_BUILD_TYPE}" build_mode)
include("mode.common")
include("mode.${build_mode}")
add_compile_definitions(${Seastar_DEFINITIONS_${build_mode}})
# Configure Seastar compile options to align with Scylla
set(CMAKE_CXX_STANDARD "20" CACHE INTERNAL "")
set(CMAKE_CXX_EXTENSIONS ON CACHE INTERNAL "")

add_subdirectory(seastar)

# System libraries dependencies
find_package(Boost COMPONENTS filesystem program_options system thread regex REQUIRED)
find_package(Lua REQUIRED)
find_package(ZLIB REQUIRED)
find_package(ICU COMPONENTS uc REQUIRED)
find_package(absl REQUIRED)
find_package(xxHash REQUIRED)

set(scylla_gen_build_dir "${CMAKE_BINARY_DIR}/gen")
file(MAKE_DIRECTORY "${scylla_gen_build_dir}")

# Generate C++ source files from thrift definitions
function(scylla_generate_thrift)
    set(one_value_args TARGET VAR IN_FILE OUT_DIR SERVICE)
    cmake_parse_arguments(args "" "${one_value_args}" "" ${ARGN})

    get_filename_component(in_file_name ${args_IN_FILE} NAME_WE)

    set(aux_out_file_name ${args_OUT_DIR}/${in_file_name})
    set(outputs
        ${aux_out_file_name}_types.cpp
        ${aux_out_file_name}_types.h
        ${aux_out_file_name}_constants.cpp
        ${aux_out_file_name}_constants.h
        ${args_OUT_DIR}/${args_SERVICE}.cpp
        ${args_OUT_DIR}/${args_SERVICE}.h)

    add_custom_command(
        DEPENDS
            ${args_IN_FILE}
            thrift
        OUTPUT ${outputs}
        COMMAND ${CMAKE_COMMAND} -E make_directory ${args_OUT_DIR}
        COMMAND thrift -gen cpp:cob_style,no_skeleton -out "${args_OUT_DIR}" "${args_IN_FILE}")

    add_custom_target(${args_TARGET}
        DEPENDS ${outputs})

    set(${args_VAR} ${outputs} PARENT_SCOPE)
endfunction()

scylla_generate_thrift(
    TARGET scylla_thrift_gen_cassandra
    VAR scylla_thrift_gen_cassandra_files
    IN_FILE "${CMAKE_SOURCE_DIR}/interface/cassandra.thrift"
    OUT_DIR ${scylla_gen_build_dir}
    SERVICE Cassandra)


# Generate C++ sources from ragel grammar files
seastar_generate_ragel(
    TARGET scylla_ragel_gen_protocol_parser
    VAR scylla_ragel_gen_protocol_parser_file
    IN_FILE "${CMAKE_SOURCE_DIR}/redis/protocol_parser.rl"
    OUT_FILE ${scylla_gen_build_dir}/redis/protocol_parser.hh)

# Generate C++ sources from Swagger definitions
set(swagger_files
    api/api-doc/cache_service.json
    api/api-doc/collectd.json
    api/api-doc/column_family.json
    api/api-doc/commitlog.json
    api/api-doc/compaction_manager.json
    api/api-doc/config.json
    api/api-doc/endpoint_snitch_info.json
    api/api-doc/error_injection.json
    api/api-doc/failure_detector.json
    api/api-doc/gossiper.json
    api/api-doc/hinted_handoff.json
    api/api-doc/lsa.json
    api/api-doc/messaging_service.json
    api/api-doc/storage_proxy.json
    api/api-doc/storage_service.json
    api/api-doc/stream_manager.json
    api/api-doc/system.json
    api/api-doc/task_manager.json
    api/api-doc/task_manager_test.json
    api/api-doc/utils.json)

set(swagger_gen_files)

foreach(f ${swagger_files})
    get_filename_component(fname "${f}" NAME_WE)
    get_filename_component(dir "${f}" DIRECTORY)
    seastar_generate_swagger(
        TARGET scylla_swagger_gen_${fname}
        VAR scylla_swagger_gen_${fname}_files
        IN_FILE "${CMAKE_SOURCE_DIR}/${f}"
        OUT_DIR "${scylla_gen_build_dir}/${dir}")
    list(APPEND swagger_gen_files "${scylla_swagger_gen_${fname}_files}")
endforeach()

# Create C++ bindings for IDL serializers
function(scylla_generate_idl_serializer)
    set(one_value_args TARGET VAR IN_FILE OUT_FILE)
    cmake_parse_arguments(args "" "${one_value_args}" "" ${ARGN})
    get_filename_component(out_dir ${args_OUT_FILE} DIRECTORY)
    set(idl_compiler "${CMAKE_SOURCE_DIR}/idl-compiler.py")

    find_package(Python3 COMPONENTS Interpreter)

    add_custom_command(
        DEPENDS
            ${args_IN_FILE}
            ${idl_compiler}
        OUTPUT ${args_OUT_FILE}
        COMMAND ${CMAKE_COMMAND} -E make_directory ${out_dir}
        COMMAND Python3::Interpreter ${idl_compiler} --ns ser -f ${args_IN_FILE} -o ${args_OUT_FILE})

    add_custom_target(${args_TARGET}
        DEPENDS ${args_OUT_FILE})

    set(${args_VAR} ${args_OUT_FILE} PARENT_SCOPE)
endfunction()

set(idl_serializers
    idl/cache_temperature.idl.hh
    idl/commitlog.idl.hh
    idl/consistency_level.idl.hh
    idl/frozen_mutation.idl.hh
    idl/frozen_schema.idl.hh
    idl/gossip_digest.idl.hh
    idl/hinted_handoff.idl.hh
    idl/idl_test.idl.hh
    idl/keys.idl.hh
    idl/messaging_service.idl.hh
    idl/mutation.idl.hh
    idl/paging_state.idl.hh
    idl/partition_checksum.idl.hh
    idl/paxos.idl.hh
    idl/query.idl.hh
    idl/raft.idl.hh
    idl/range.idl.hh
    idl/read_command.idl.hh
    idl/reconcilable_result.idl.hh
    idl/replay_position.idl.hh
    idl/result.idl.hh
    idl/ring_position.idl.hh
    idl/streaming.idl.hh
    idl/token.idl.hh
    idl/tracing.idl.hh
    idl/truncation_record.idl.hh
    idl/uuid.idl.hh
    idl/view.idl.hh)

set(idl_gen_files)

foreach(f ${idl_serializers})
    get_filename_component(idl_name "${f}" NAME)
    get_filename_component(idl_target "${idl_name}" NAME_WE)
    get_filename_component(idl_dir "${f}" DIRECTORY)
    string(REPLACE ".idl.hh" ".dist.hh" idl_out_hdr_name "${idl_name}")
    scylla_generate_idl_serializer(
        TARGET scylla_idl_gen_${idl_target}
        VAR scylla_idl_gen_${idl_target}_files
        IN_FILE "${CMAKE_SOURCE_DIR}/${f}"
        OUT_FILE ${scylla_gen_build_dir}/${idl_dir}/${idl_out_hdr_name})
    list(APPEND idl_gen_files "${scylla_idl_gen_${idl_target}_files}")
endforeach()

set(scylla_sources
    absl-flat_hash_map.cc
    api/api.cc
    api/cache_service.cc
    api/collectd.cc
    api/column_family.cc
    api/commitlog.cc
    api/compaction_manager.cc
    api/config.cc
    api/endpoint_snitch.cc
    api/error_injection.cc
    api/failure_detector.cc
    api/gossiper.cc
    api/hinted_handoff.cc
    api/lsa.cc
    api/messaging_service.cc
    api/storage_proxy.cc
    api/storage_service.cc
    api/stream_manager.cc
    api/system.cc
    api/task_manager.cc
    api/task_manager_test.cc
    auth/allow_all_authenticator.cc
    auth/allow_all_authorizer.cc
    auth/authenticated_user.cc
    auth/authentication_options.cc
    auth/authenticator.cc
    auth/common.cc
    auth/default_authorizer.cc
    auth/password_authenticator.cc
    auth/passwords.cc
    auth/permission.cc
    auth/permissions_cache.cc
    auth/resource.cc
    auth/role_or_anonymous.cc
    auth/roles-metadata.cc
    auth/sasl_challenge.cc
    auth/service.cc
    auth/standard_role_manager.cc
    auth/transitional.cc
    bytes.cc
    caching_options.cc
    cdc/cdc_partitioner.cc
    cdc/generation.cc
    cdc/log.cc
    cdc/metadata.cc
    cdc/split.cc
    clocks-impl.cc
    collection_mutation.cc
    compaction/compaction.cc
    compaction/compaction_manager.cc
    compaction/compaction_strategy.cc
    compaction/leveled_compaction_strategy.cc
    compaction/size_tiered_compaction_strategy.cc
    compaction/time_window_compaction_strategy.cc
    compress.cc
    converting_mutation_partition_applier.cc
    counters.cc
    data_dictionary/data_dictionary.cc
    db/batchlog_manager.cc
    db/commitlog/commitlog.cc
    db/commitlog/commitlog_entry.cc
    db/commitlog/commitlog_replayer.cc
    db/config.cc
    db/consistency_level.cc
    db/cql_type_parser.cc
    db/data_listeners.cc
    db/extensions.cc
    db/heat_load_balance.cc
    db/hints/host_filter.cc
    db/hints/manager.cc
    db/hints/resource_manager.cc
    db/hints/sync_point.cc
    db/large_data_handler.cc
    db/legacy_schema_migrator.cc
    db/marshal/type_parser.cc
    db/rate_limiter.cc
    db/schema_tables.cc
    db/size_estimates_virtual_reader.cc
    db/snapshot-ctl.cc
    db/sstables-format-selector.cc
    db/system_distributed_keyspace.cc
    db/system_keyspace.cc
    db/view/row_locking.cc
    db/view/view.cc
    db/view/view_update_generator.cc
    db/virtual_table.cc
    dht/boot_strapper.cc
    dht/i_partitioner.cc
    dht/murmur3_partitioner.cc
    dht/range_streamer.cc
    dht/token.cc
    replica/distributed_loader.cc
    duration.cc
    exceptions/exceptions.cc
    readers/mutation_readers.cc
    frozen_schema.cc
    generic_server.cc
    gms/application_state.cc
    gms/endpoint_state.cc
    gms/failure_detector.cc
    gms/feature_service.cc
    gms/gossip_digest_ack2.cc
    gms/gossip_digest_ack.cc
    gms/gossip_digest_syn.cc
    gms/gossiper.cc
    gms/inet_address.cc
    gms/versioned_value.cc
    gms/version_generator.cc
    hashers.cc
    index/secondary_index.cc
    index/secondary_index_manager.cc
    init.cc
    keys.cc
    utils/lister.cc
    locator/abstract_replication_strategy.cc
    locator/azure_snitch.cc
    locator/ec2_multi_region_snitch.cc
    locator/ec2_snitch.cc
    locator/everywhere_replication_strategy.cc
    locator/gce_snitch.cc
    locator/gossiping_property_file_snitch.cc
    locator/local_strategy.cc
    locator/network_topology_strategy.cc
    locator/production_snitch_base.cc
    locator/rack_inferring_snitch.cc
    locator/simple_snitch.cc
    locator/simple_strategy.cc
    locator/snitch_base.cc
    locator/token_metadata.cc
    lang/lua.cc
    main.cc
    replica/memtable.cc
    message/messaging_service.cc
    multishard_mutation_query.cc
    mutation/atomic_cell.cc
    mutation/canonical_mutation.cc
    mutation/frozen_mutation.cc
    mutation/mutation.cc
    mutation/mutation_fragment.cc
    mutation/mutation_partition.cc
    mutation/mutation_partition_serializer.cc
    mutation/mutation_partition_v2.cc
    mutation/mutation_partition_view.cc
    mutation/partition_version.cc
    mutation/range_tombstone.cc
    mutation/range_tombstone_list.cc
    mutation_query.cc
    readers/mutation_reader.cc
    mutation_writer/feed_writers.cc
    mutation_writer/multishard_writer.cc
    mutation_writer/partition_based_splitting_writer.cc
    mutation_writer/shard_based_splitting_writer.cc
    mutation_writer/timestamp_based_splitting_writer.cc
    partition_slice_builder.cc
    querier.cc
    query.cc
    query_ranges_to_vnodes.cc
    query-result-set.cc
    raft/fsm.cc
    raft/log.cc
    raft/raft.cc
    raft/server.cc
    raft/tracker.cc
    service/broadcast_tables/experimental/lang.cc
    tombstone_gc_options.cc
    tombstone_gc.cc
    reader_concurrency_semaphore.cc
    redis/controller.cc
    redis/server.cc
    redis/query_processor.cc
    redis/protocol_parser.rl
    redis/keyspace_utils.cc
    redis/options.cc
    redis/stats.cc
    redis/mutation_utils.cc
    redis/query_utils.cc
    redis/abstract_command.cc
    redis/command_factory.cc
    redis/commands.cc
    redis/lolwut.cc
    release.cc
    repair/repair.cc
    repair/row_level.cc
    replica/database.cc
    replica/table.cc
    row_cache.cc
    schema.cc
    schema_mutations.cc
    schema_registry.cc
    serializer.cc
    service/client_state.cc
    service/forward_service.cc
    service/migration_manager.cc
    service/misc_services.cc
    service/pager/paging_state.cc
    service/pager/query_pagers.cc
    service/paxos/paxos_state.cc
    service/paxos/prepare_response.cc
    service/paxos/prepare_summary.cc
    service/paxos/proposal.cc
    service/priority_manager.cc
    service/qos/qos_common.cc
    service/qos/service_level_controller.cc
    service/qos/standard_service_level_distributed_data_accessor.cc
    service/raft/raft_group_registry.cc
    service/raft/raft_rpc.cc
    service/raft/raft_sys_table_storage.cc
    service/raft/group0_state_machine.cc
    service/storage_proxy.cc
    service/storage_service.cc
    sstables/compress.cc
    sstables/integrity_checked_file_impl.cc
    sstables/kl/reader.cc
    sstables/metadata_collector.cc
    sstables/m_format_read_helpers.cc
    sstables/mx/reader.cc
    sstables/mx/writer.cc
    sstables/prepended_input_stream.cc
    sstables/random_access_reader.cc
    sstables/sstable_directory.cc
    sstables/sstable_mutation_reader.cc
    sstables/sstables.cc
    sstables/sstable_set.cc
    sstables/sstables_manager.cc
    sstables/sstable_version.cc
    sstables/writer.cc
    streaming/consumer.cc
    streaming/progress_info.cc
    streaming/session_info.cc
    streaming/stream_coordinator.cc
    streaming/stream_manager.cc
    streaming/stream_plan.cc
    streaming/stream_reason.cc
    streaming/stream_receive_task.cc
    streaming/stream_request.cc
    streaming/stream_result_future.cc
    streaming/stream_session.cc
    streaming/stream_session_state.cc
    streaming/stream_summary.cc
    streaming/stream_task.cc
    streaming/stream_transfer_task.cc
    table_helper.cc
    tasks/task_manager.cc
    thrift/controller.cc
    thrift/handler.cc
    thrift/server.cc
    thrift/thrift_validation.cc
    timeout_config.cc
    tools/scylla-types.cc
    tools/scylla-sstable.cc
    tools/schema_loader.cc
    tools/utils.cc
    tools/lua_sstable_consumer.cc
    tracing/tracing.cc
    tracing/trace_keyspace_helper.cc
    tracing/trace_state.cc
    tracing/traced_file.cc
    transport/controller.cc
    transport/cql_protocol_extension.cc
    transport/event.cc
    transport/event_notifier.cc
    transport/messages/result_message.cc
    transport/server.cc
    types.cc
    unimplemented.cc
    utils/arch/powerpc/crc32-vpmsum/crc32_wrapper.cc
    utils/array-search.cc
    utils/ascii.cc
    utils/base64.cc
    utils/big_decimal.cc
    utils/bloom_calculations.cc
    utils/bloom_filter.cc
    utils/buffer_input_stream.cc
    utils/build_id.cc
    utils/config_file.cc
    utils/directories.cc
    utils/disk-error-handler.cc
    utils/dynamic_bitset.cc
    utils/error_injection.cc
    utils/exceptions.cc
    utils/file_lock.cc
    utils/generation-number.cc
    utils/gz/crc_combine.cc
    utils/gz/crc_combine_table.cc
    utils/human_readable.cc
    utils/i_filter.cc
    utils/large_bitset.cc
    utils/like_matcher.cc
    utils/limiting_data_source.cc
    utils/logalloc.cc
    utils/managed_bytes.cc
    utils/multiprecision_int.cc
    utils/murmur_hash.cc
    utils/rate_limiter.cc
    utils/rjson.cc
    utils/runtime.cc
    utils/updateable_value.cc
    utils/utf8.cc
    utils/uuid.cc
    utils/UUID_gen.cc
    validation.cc
    vint-serialization.cc
    zstd.cc)

set(scylla_gen_sources
    "${scylla_thrift_gen_cassandra_files}"
    "${scylla_ragel_gen_protocol_parser_file}"
    "${swagger_gen_files}"
    "${idl_gen_files}")

add_subdirectory(alternator)
add_subdirectory(cql3)
add_subdirectory(rust)

add_executable(scylla
    ${scylla_sources}
    ${scylla_gen_sources})

target_link_libraries(scylla PRIVATE
    alternator
    cql3
    wasmtime_bindings)

target_link_libraries(scylla PRIVATE
    seastar
    # Boost dependencies
    Boost::filesystem
    Boost::program_options
    Boost::system
    Boost::thread
    Boost::regex
    Boost::headers
    # Abseil libs
    absl::hashtablez_sampler
    absl::raw_hash_set
    absl::synchronization
    absl::graphcycles_internal
    absl::stacktrace
    absl::symbolize
    absl::debugging_internal
    absl::demangle_internal
    absl::time
    absl::time_zone
    absl::int128
    absl::city
    absl::hash
    absl::malloc_internal
    absl::spinlock_wait
    absl::base
    absl::dynamic_annotations
    absl::raw_logging_internal
    absl::exponential_biased
    absl::throw_delegate
    # System libs
    ZLIB::ZLIB
    ICU::uc
    systemd
    zstd
    snappy
    ${LUA_LIBRARIES}
    thrift
    crypt)

target_link_libraries(scylla PRIVATE
    -Wl,--build-id=sha1 # Force SHA1 build-id generation
    # TODO: Use lld linker if it's available, otherwise gold, else bfd
    -fuse-ld=lld)
# TODO: patch dynamic linker to match configure.py behavior

# Hacks needed to expose internal APIs for xxhash dependencies
target_compile_definitions(scylla PRIVATE XXH_PRIVATE_API HAVE_LZ4_COMPRESS_DEFAULT)

find_package(libdeflate REQUIRED)
target_include_directories(scylla PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
    libdeflate::libdeflate
    "${scylla_gen_build_dir}")

###
### Generate version file and supply appropriate compile definitions for release.cc
###
execute_process(COMMAND ${CMAKE_SOURCE_DIR}/SCYLLA-VERSION-GEN --output-dir "${CMAKE_BINARY_DIR}/gen" RESULT_VARIABLE scylla_version_gen_res)
if(scylla_version_gen_res)
    message(SEND_ERROR "Version file generation failed. Return code: ${scylla_version_gen_res}")
endif()

file(READ "${CMAKE_BINARY_DIR}/gen/SCYLLA-VERSION-FILE" scylla_version)
string(STRIP "${scylla_version}" scylla_version)

file(READ "${CMAKE_BINARY_DIR}/gen/SCYLLA-RELEASE-FILE" scylla_release)
string(STRIP "${scylla_release}" scylla_release)

get_property(release_cdefs SOURCE "${CMAKE_SOURCE_DIR}/release.cc" PROPERTY COMPILE_DEFINITIONS)
list(APPEND release_cdefs "SCYLLA_VERSION=\"${scylla_version}\"" "SCYLLA_RELEASE=\"${scylla_release}\"")
set_source_files_properties("${CMAKE_SOURCE_DIR}/release.cc" PROPERTIES COMPILE_DEFINITIONS "${release_cdefs}")

# TODO: create cmake/ directory and move utilities (generate functions etc) there
# TODO: Build tests if BUILD_TESTING=on (using CTest module)
