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

set(Seastar_TESTING ON CACHE BOOL "" FORCE)
add_subdirectory(seastar)

# System libraries dependencies
find_package(Boost REQUIRED
    COMPONENTS filesystem program_options system thread regex unit_test_framework)
find_package(Lua REQUIRED)
find_package(ZLIB REQUIRED)
find_package(ICU COMPONENTS uc i18n REQUIRED)
find_package(absl REQUIRED)
find_package(libdeflate REQUIRED)
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

set(scylla_sources
    absl-flat_hash_map.cc
    bytes.cc
    cdc/cdc_partitioner.cc
    cdc/generation.cc
    cdc/log.cc
    cdc/metadata.cc
    cdc/split.cc
    client_data.cc
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
    db/consistency_level.cc
    db/system_keyspace.cc
    db/virtual_table.cc
    db/system_distributed_keyspace.cc
    db/size_estimates_virtual_reader.cc
    db/schema_tables.cc
    db/cql_type_parser.cc
    db/legacy_schema_migrator.cc
    db/commitlog/commitlog.cc
    db/commitlog/commitlog_replayer.cc
    db/commitlog/commitlog_entry.cc
    db/data_listeners.cc
    db/hints/manager.cc
    db/hints/resource_manager.cc
    db/hints/host_filter.cc
    db/hints/sync_point.cc
    db/config.cc
    db/extensions.cc
    db/heat_load_balance.cc
    db/large_data_handler.cc
    db/marshal/type_parser.cc
    db/batchlog_manager.cc
    db/tags/utils.cc
    db/view/view.cc
    db/view/view_update_generator.cc
    db/view/row_locking.cc
    db/sstables-format-selector.cc
    db/snapshot-ctl.cc
    db/rate_limiter.cc
    db/per_partition_rate_limit_options.cc
    dht/boot_strapper.cc
    dht/i_partitioner.cc
    dht/murmur3_partitioner.cc
    dht/range_streamer.cc
    dht/token.cc
    direct_failure_detector/failure_detector.cc
    replica/distributed_loader.cc
    duration.cc
    exceptions/exceptions.cc
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
    debug.cc
    index/secondary_index.cc
    index/secondary_index_manager.cc
    init.cc
    keys.cc
    locator/abstract_replication_strategy.cc
    locator/azure_snitch.cc
    locator/simple_strategy.cc
    locator/local_strategy.cc
    locator/network_topology_strategy.cc
    locator/everywhere_replication_strategy.cc
    locator/token_metadata.cc
    locator/snitch_base.cc
    locator/simple_snitch.cc
    locator/rack_inferring_snitch.cc
    locator/gossiping_property_file_snitch.cc
    locator/production_snitch_base.cc
    locator/ec2_snitch.cc
    locator/ec2_multi_region_snitch.cc
    locator/gce_snitch.cc
    locator/topology.cc
    locator/util.cc
    lang/lua.cc
    lang/wasm.cc
    lang/wasm_instance_cache.cc
    main.cc
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
    readers/combined.cc
    readers/multishard.cc
    readers/mutation_reader.cc
    readers/mutation_readers.cc
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
    replica/distributed_loader.cc
    replica/memtable.cc
    replica/exceptions.cc
    replica/dirty_memory_manager.cc
    row_cache.cc
    schema_mutations.cc
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
    service/raft/discovery.cc
    service/raft/group0_state_machine.cc
    service/raft/raft_group0.cc
    service/raft/raft_group0_client.cc
    service/raft/raft_group_registry.cc
    service/raft/raft_rpc.cc
    service/raft/raft_sys_table_storage.cc
    service/storage_proxy.cc
    service/storage_service.cc
    sstables_loader.cc
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
    unimplemented.cc
    validation.cc
    vint-serialization.cc
    zstd.cc)
set(scylla_gen_sources
    "${scylla_thrift_gen_cassandra_files}"
    "${scylla_ragel_gen_protocol_parser_file}"
    "${swagger_gen_files}")

add_subdirectory(api)
add_subdirectory(alternator)
add_subdirectory(auth)
add_subdirectory(cql3)
add_subdirectory(idl)
add_subdirectory(rust)
add_subdirectory(schema)
add_subdirectory(sstables)
add_subdirectory(test)
add_subdirectory(types)
add_subdirectory(utils)

add_executable(scylla
    ${scylla_sources}
    ${scylla_gen_sources})

target_link_libraries(scylla PRIVATE
    api
    auth
    alternator
    cql3
    idl
    schema
    sstables
    test-perf
    types
    utils
    wasmtime_bindings)
target_link_libraries(Boost::regex
  INTERFACE
    ICU::i18n)

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
    libdeflate::libdeflate
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

target_include_directories(scylla PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
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
