cmake_minimum_required(VERSION 3.18)

project(scylla)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to 'Release' as none was specified.")
  set(CMAKE_BUILD_TYPE "Release" CACHE
      STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
    "Debug" "Release" "Dev" "Sanitize")
endif()

if(CMAKE_BUILD_TYPE)
    string(TOLOWER "${CMAKE_BUILD_TYPE}" BUILD_TYPE)
else()
    set(BUILD_TYPE "release")
endif()

function(default_target_arch arch)
    set(x86_instruction_sets i386 i686 x86_64)
    if(CMAKE_SYSTEM_PROCESSOR IN_LIST x86_instruction_sets)
        set(${arch} "westmere" PARENT_SCOPE)
    elseif(CMAKE_SYSTEM_PROCESSOR EQUAL "aarch64")
        set(${arch} "armv8-a+crc+crypto" PARENT_SCOPE)
    else()
        set(${arch} "" PARENT_SCOPE)
    endif()
endfunction()
default_target_arch(target_arch)
if(target_arch)
    set(target_arch_flag "-march=${target_arch}")
endif()

# Configure Seastar compile options to align with Scylla
set(Seastar_CXX_FLAGS -fcoroutines ${target_arch_flag} CACHE INTERNAL "" FORCE)
set(Seastar_CXX_DIALECT gnu++20 CACHE INTERNAL "" FORCE)

add_subdirectory(seastar)
add_subdirectory(abseil)
# Exclude absl::strerror from the default "all" target since it's not
# used in Scylla build and, moreover, makes use of deprecated glibc APIs,
# such as sys_nerr, which are not exposed from "stdio.h" since glibc 2.32,
# which happens to be the case for recent Fedora distribution versions.
#
# Need to use the internal "absl_strerror" target name instead of namespaced
# variant because `set_target_properties` does not understand the latter form,
# unfortunately.
set_target_properties(absl_strerror PROPERTIES EXCLUDE_FROM_ALL TRUE)

# System libraries dependencies
find_package(Boost COMPONENTS filesystem program_options system thread regex REQUIRED)
find_package(Lua REQUIRED)
find_package(ZLIB REQUIRED)
find_package(ICU COMPONENTS uc REQUIRED)

set(scylla_build_dir "${CMAKE_BINARY_DIR}/build/${BUILD_TYPE}")
set(scylla_gen_build_dir "${scylla_build_dir}/gen")
file(MAKE_DIRECTORY "${scylla_build_dir}" "${scylla_gen_build_dir}")

# Place libraries, executables and archives in ${buildroot}/build/${mode}/
foreach(mode RUNTIME LIBRARY ARCHIVE)
    set(CMAKE_${mode}_OUTPUT_DIRECTORY "${scylla_build_dir}")
endforeach()

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
    IN_FILE interface/cassandra.thrift
    OUT_DIR ${scylla_gen_build_dir}
    SERVICE Cassandra)

# Parse antlr3 grammar files and generate C++ sources
function(scylla_generate_antlr3)
    set(one_value_args TARGET VAR IN_FILE OUT_DIR)
    cmake_parse_arguments(args "" "${one_value_args}" "" ${ARGN})

    get_filename_component(in_file_pure_name ${args_IN_FILE} NAME)
    get_filename_component(stem ${in_file_pure_name} NAME_WE)

    set(outputs
        "${args_OUT_DIR}/${stem}Lexer.hpp"
        "${args_OUT_DIR}/${stem}Lexer.cpp"
        "${args_OUT_DIR}/${stem}Parser.hpp"
        "${args_OUT_DIR}/${stem}Parser.cpp")

    add_custom_command(
        DEPENDS
            ${args_IN_FILE}
        OUTPUT ${outputs}
        # Remove #ifdef'ed code from the grammar source code
        COMMAND sed -e "/^#if 0/,/^#endif/d" "${args_IN_FILE}" > "${args_OUT_DIR}/${in_file_pure_name}"
        COMMAND antlr3 "${args_OUT_DIR}/${in_file_pure_name}"
        # We replace many local `ExceptionBaseType* ex` variables with a single function-scope one.
        # Because we add such a variable to every function, and because `ExceptionBaseType` is not a global
        # name, we also add a global typedef to avoid compilation errors.
        COMMAND sed -i -e "/^.*On :.*$/d" "${args_OUT_DIR}/${stem}Lexer.hpp"
        COMMAND sed -i -e "/^.*On :.*$/d" "${args_OUT_DIR}/${stem}Lexer.cpp"
        COMMAND sed -i -e "/^.*On :.*$/d" "${args_OUT_DIR}/${stem}Parser.hpp"
        COMMAND sed -i
            -e "s/^\\( *\\)\\(ImplTraits::CommonTokenType\\* [a-zA-Z0-9_]* = NULL;\\)$/\\1const \\2/"
            -e "/^.*On :.*$/d"
            -e "1i using ExceptionBaseType = int;"
            -e "s/^{/{ ExceptionBaseType\\* ex = nullptr;/; s/ExceptionBaseType\\* ex = new/ex = new/; s/exceptions::syntax_exception e/exceptions::syntax_exception\\& e/"
            "${args_OUT_DIR}/${stem}Parser.cpp"
        VERBATIM)

    add_custom_target(${args_TARGET}
        DEPENDS ${outputs})

    set(${args_VAR} ${outputs} PARENT_SCOPE)
endfunction()

set(antlr3_grammar_files
    cql3/Cql.g
    alternator/expressions.g)

set(antlr3_gen_files)

foreach(f ${antlr3_grammar_files})
    get_filename_component(grammar_file_name "${f}" NAME_WE)
    get_filename_component(f_dir "${f}" DIRECTORY)
    scylla_generate_antlr3(
        TARGET scylla_antlr3_gen_${grammar_file_name}
        VAR scylla_antlr3_gen_${grammar_file_name}_files
        IN_FILE ${f}
        OUT_DIR ${scylla_gen_build_dir}/${f_dir})
    list(APPEND antlr3_gen_files "${scylla_antlr3_gen_${grammar_file_name}_files}")
endforeach()

# Generate C++ sources from ragel grammar files
seastar_generate_ragel(
    TARGET scylla_ragel_gen_protocol_parser
    VAR scylla_ragel_gen_protocol_parser_file
    IN_FILE redis/protocol_parser.rl
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
    api/api-doc/utils.json)

set(swagger_gen_files)

foreach(f ${swagger_files})
    get_filename_component(fname "${f}" NAME_WE)
    get_filename_component(dir "${f}" DIRECTORY)
    seastar_generate_swagger(
        TARGET scylla_swagger_gen_${fname}
        VAR scylla_swagger_gen_${fname}_files
        IN_FILE "${f}"
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
    idl/idl_test.idl.hh
    idl/keys.idl.hh
    idl/messaging_service.idl.hh
    idl/mutation.idl.hh
    idl/paging_state.idl.hh
    idl/partition_checksum.idl.hh
    idl/paxos.idl.hh
    idl/query.idl.hh
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
        IN_FILE ${f}
        OUT_FILE ${scylla_gen_build_dir}/${idl_dir}/${idl_out_hdr_name})
    list(APPEND idl_gen_files "${scylla_idl_gen_${idl_target}_files}")
endforeach()

set(scylla_sources
    absl-flat_hash_map.cc
    alternator/auth.cc
    alternator/base64.cc
    alternator/conditions.cc
    alternator/executor.cc
    alternator/expressions.cc
    alternator/serialization.cc
    alternator/server.cc
    alternator/stats.cc
    alternator/streams.cc
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
    atomic_cell.cc
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
    canonical_mutation.cc
    cdc/cdc_partitioner.cc
    cdc/generation.cc
    cdc/log.cc
    cdc/metadata.cc
    cdc/split.cc
    clocks-impl.cc
    collection_mutation.cc
    compress.cc
    connection_notifier.cc
    converting_mutation_partition_applier.cc
    counters.cc
    cql3/abstract_marker.cc
    cql3/attributes.cc
    cql3/cf_name.cc
    cql3/column_condition.cc
    cql3/column_identifier.cc
    cql3/column_specification.cc
    cql3/constants.cc
    cql3/cql3_type.cc
    cql3/expr/expression.cc
    cql3/functions/aggregate_fcts.cc
    cql3/functions/castas_fcts.cc
    cql3/functions/error_injection_fcts.cc
    cql3/functions/functions.cc
    cql3/functions/user_function.cc
    cql3/index_name.cc
    cql3/keyspace_element_name.cc
    cql3/lists.cc
    cql3/maps.cc
    cql3/operation.cc
    cql3/query_options.cc
    cql3/query_processor.cc
    cql3/relation.cc
    cql3/restrictions/statement_restrictions.cc
    cql3/result_set.cc
    cql3/role_name.cc
    cql3/selection/abstract_function_selector.cc
    cql3/selection/selectable.cc
    cql3/selection/selection.cc
    cql3/selection/selector.cc
    cql3/selection/selector_factories.cc
    cql3/selection/simple_selector.cc
    cql3/sets.cc
    cql3/single_column_relation.cc
    cql3/statements/alter_keyspace_statement.cc
    cql3/statements/alter_table_statement.cc
    cql3/statements/alter_type_statement.cc
    cql3/statements/alter_view_statement.cc
    cql3/statements/authentication_statement.cc
    cql3/statements/authorization_statement.cc
    cql3/statements/batch_statement.cc
    cql3/statements/cas_request.cc
    cql3/statements/cf_prop_defs.cc
    cql3/statements/cf_statement.cc
    cql3/statements/create_function_statement.cc
    cql3/statements/create_index_statement.cc
    cql3/statements/create_keyspace_statement.cc
    cql3/statements/create_table_statement.cc
    cql3/statements/create_type_statement.cc
    cql3/statements/create_view_statement.cc
    cql3/statements/delete_statement.cc
    cql3/statements/drop_function_statement.cc
    cql3/statements/drop_index_statement.cc
    cql3/statements/drop_keyspace_statement.cc
    cql3/statements/drop_table_statement.cc
    cql3/statements/drop_type_statement.cc
    cql3/statements/drop_view_statement.cc
    cql3/statements/function_statement.cc
    cql3/statements/grant_statement.cc
    cql3/statements/index_prop_defs.cc
    cql3/statements/index_target.cc
    cql3/statements/ks_prop_defs.cc
    cql3/statements/list_permissions_statement.cc
    cql3/statements/list_users_statement.cc
    cql3/statements/modification_statement.cc
    cql3/statements/permission_altering_statement.cc
    cql3/statements/property_definitions.cc
    cql3/statements/raw/parsed_statement.cc
    cql3/statements/revoke_statement.cc
    cql3/statements/role-management-statements.cc
    cql3/statements/schema_altering_statement.cc
    cql3/statements/select_statement.cc
    cql3/statements/truncate_statement.cc
    cql3/statements/update_statement.cc
    cql3/statements/use_statement.cc
    cql3/token_relation.cc
    cql3/tuples.cc
    cql3/type_json.cc
    cql3/untyped_result_set.cc
    cql3/update_parameters.cc
    cql3/user_types.cc
    cql3/ut_name.cc
    cql3/util.cc
    cql3/values.cc
    cql3/variable_specifications.cc
    data/cell.cc
    database.cc
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
    db/hints/manager.cc
    db/hints/resource_manager.cc
    db/large_data_handler.cc
    db/legacy_schema_migrator.cc
    db/marshal/type_parser.cc
    db/schema_tables.cc
    db/size_estimates_virtual_reader.cc
    db/snapshot-ctl.cc
    db/sstables-format-selector.cc
    db/system_distributed_keyspace.cc
    db/system_keyspace.cc
    db/view/row_locking.cc
    db/view/view.cc
    db/view/view_update_generator.cc
    dht/boot_strapper.cc
    dht/i_partitioner.cc
    dht/murmur3_partitioner.cc
    dht/range_streamer.cc
    dht/token.cc
    distributed_loader.cc
    duration.cc
    exceptions/exceptions.cc
    flat_mutation_reader.cc
    frozen_mutation.cc
    frozen_schema.cc
    gms/application_state.cc
    gms/endpoint_state.cc
    gms/failure_detector.cc
    gms/feature_service.cc
    gms/gossip_digest_ack.cc
    gms/gossip_digest_ack2.cc
    gms/gossip_digest_syn.cc
    gms/gossiper.cc
    gms/inet_address.cc
    gms/version_generator.cc
    gms/versioned_value.cc
    hashers.cc
    index/secondary_index.cc
    index/secondary_index_manager.cc
    init.cc
    keys.cc
    lister.cc
    locator/abstract_replication_strategy.cc
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
    lua.cc
    main.cc
    memtable.cc
    message/messaging_service.cc
    multishard_mutation_query.cc
    mutation.cc
    raft/fsm.cc
    raft/log.cc
    raft/progress.cc
    raft/raft.cc
    raft/server.cc
    mutation_fragment.cc
    mutation_partition.cc
    mutation_partition_serializer.cc
    mutation_partition_view.cc
    mutation_query.cc
    mutation_reader.cc
    mutation_writer/multishard_writer.cc
    mutation_writer/shard_based_splitting_writer.cc
    mutation_writer/timestamp_based_splitting_writer.cc
    partition_slice_builder.cc
    partition_version.cc
    querier.cc
    query-result-set.cc
    query.cc
    range_tombstone.cc
    range_tombstone_list.cc
    reader_concurrency_semaphore.cc
    redis/abstract_command.cc
    redis/command_factory.cc
    redis/commands.cc
    redis/keyspace_utils.cc
    redis/lolwut.cc
    redis/mutation_utils.cc
    redis/options.cc
    redis/query_processor.cc
    redis/query_utils.cc
    redis/server.cc
    redis/service.cc
    redis/stats.cc
    repair/repair.cc
    repair/row_level.cc
    row_cache.cc
    schema.cc
    schema_mutations.cc
    schema_registry.cc
    service/client_state.cc
    service/migration_manager.cc
    service/migration_task.cc
    service/misc_services.cc
    service/pager/paging_state.cc
    service/pager/query_pagers.cc
    service/paxos/paxos_state.cc
    service/paxos/prepare_response.cc
    service/paxos/prepare_summary.cc
    service/paxos/proposal.cc
    service/priority_manager.cc
    service/storage_proxy.cc
    service/storage_service.cc
    sstables/compaction.cc
    sstables/compaction_manager.cc
    sstables/compaction_strategy.cc
    sstables/compress.cc
    sstables/integrity_checked_file_impl.cc
    sstables/kl/writer.cc
    sstables/leveled_compaction_strategy.cc
    sstables/m_format_read_helpers.cc
    sstables/metadata_collector.cc
    sstables/mp_row_consumer.cc
    sstables/mx/writer.cc
    sstables/partition.cc
    sstables/prepended_input_stream.cc
    sstables/random_access_reader.cc
    sstables/size_tiered_compaction_strategy.cc
    sstables/sstable_directory.cc
    sstables/sstable_version.cc
    sstables/sstables.cc
    sstables/sstables_manager.cc
    sstables/time_window_compaction_strategy.cc
    sstables/writer.cc
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
    table.cc
    table_helper.cc
    thrift/controller.cc
    thrift/handler.cc
    thrift/server.cc
    thrift/thrift_validation.cc
    timeout_config.cc
    tracing/trace_keyspace_helper.cc
    tracing/trace_state.cc
    tracing/traced_file.cc
    tracing/tracing.cc
    tracing/tracing_backend_registry.cc
    transport/controller.cc
    transport/cql_protocol_extension.cc
    transport/event.cc
    transport/event_notifier.cc
    transport/messages/result_message.cc
    transport/server.cc
    types.cc
    unimplemented.cc
    utils/UUID_gen.cc
    utils/arch/powerpc/crc32-vpmsum/crc32_wrapper.cc
    utils/array-search.cc
    utils/ascii.cc
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
    validation.cc
    vint-serialization.cc
    zstd.cc
    release.cc)

set(scylla_gen_sources
    "${scylla_thrift_gen_cassandra_files}"
    "${scylla_ragel_gen_protocol_parser_file}"
    "${swagger_gen_files}"
    "${idl_gen_files}"
    "${antlr3_gen_files}")

add_executable(scylla
    ${scylla_sources}
    ${scylla_gen_sources})

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

target_compile_options(scylla PRIVATE
    -std=gnu++20
    -fcoroutines # TODO: Clang does not have this flag, adjust to both variants
    ${target_arch_flag})
# Hacks needed to expose internal APIs for xxhash dependencies
target_compile_definitions(scylla PRIVATE XXH_PRIVATE_API HAVE_LZ4_COMPRESS_DEFAULT)

target_include_directories(scylla PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
    libdeflate
    abseil
    "${scylla_gen_build_dir}")

###
### Create crc_combine_table helper executable.
### Use it to generate crc_combine_table.cc to be used in scylla at build time.
###
add_executable(crc_combine_table utils/gz/gen_crc_combine_table.cc)
target_link_libraries(crc_combine_table PRIVATE seastar)
target_include_directories(crc_combine_table PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_compile_options(crc_combine_table PRIVATE
    -std=gnu++20
    -fcoroutines
    ${target_arch_flag})
add_dependencies(scylla crc_combine_table)

# Generate an additional source file at build time that is needed for Scylla compilation
add_custom_command(OUTPUT "${scylla_gen_build_dir}/utils/gz/crc_combine_table.cc"
    COMMAND $<TARGET_FILE:crc_combine_table> > "${scylla_gen_build_dir}/utils/gz/crc_combine_table.cc"
    DEPENDS crc_combine_table)
target_sources(scylla PRIVATE "${scylla_gen_build_dir}/utils/gz/crc_combine_table.cc")

###
### Generate version file and supply appropriate compile definitions for release.cc
###
execute_process(COMMAND ${CMAKE_SOURCE_DIR}/SCYLLA-VERSION-GEN RESULT_VARIABLE scylla_version_gen_res)
if(scylla_version_gen_res)
    message(SEND_ERROR "Version file generation failed. Return code: ${scylla_version_gen_res}")
endif()

file(READ build/SCYLLA-VERSION-FILE scylla_version)
string(STRIP "${scylla_version}" scylla_version)

file(READ build/SCYLLA-RELEASE-FILE scylla_release)
string(STRIP "${scylla_release}" scylla_release)

get_property(release_cdefs SOURCE "${CMAKE_SOURCE_DIR}/release.cc" PROPERTY COMPILE_DEFINITIONS)
list(APPEND release_cdefs "SCYLLA_VERSION=\"${scylla_version}\"" "SCYLLA_RELEASE=\"${scylla_release}\"")
set_source_files_properties("${CMAKE_SOURCE_DIR}/release.cc" PROPERTIES COMPILE_DEFINITIONS "${release_cdefs}")

###
### Custom command for building libdeflate. Link the library to scylla.
###
set(libdeflate_lib "${scylla_build_dir}/libdeflate/libdeflate.a")
add_custom_command(OUTPUT "${libdeflate_lib}"
    COMMAND make -C libdeflate
        BUILD_DIR=../build/${BUILD_TYPE}/libdeflate/
        CC=${CMAKE_C_COMPILER}
        "CFLAGS=${target_arch_flag}"
        ../build/${BUILD_TYPE}/libdeflate//libdeflate.a) # Two backslashes are important!
# Hack to force generating custom command to produce libdeflate.a
add_custom_target(libdeflate DEPENDS "${libdeflate_lib}")
target_link_libraries(scylla PRIVATE "${libdeflate_lib}")

# TODO: create cmake/ directory and move utilities (generate functions etc) there
# TODO: Build tests if BUILD_TESTING=on (using CTest module)
