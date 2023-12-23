cmake_minimum_required(VERSION 3.27)

project(scylla)

include(CTest)

list(APPEND CMAKE_MODULE_PATH
  ${CMAKE_CURRENT_SOURCE_DIR}/cmake
  ${CMAKE_CURRENT_SOURCE_DIR}/seastar/cmake)

# Set the possible values of build type for cmake-gui
set(scylla_build_types
    "Debug" "RelWithDebInfo" "Dev" "Sanitize" "Coverage")
if(DEFINED CMAKE_BUILD_TYPE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
     ${scylla_build_types})
    if(NOT CMAKE_BUILD_TYPE)
        set(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE
            STRING "Choose the type of build." FORCE)
        message(WARNING "CMAKE_BUILD_TYPE not specified, Using 'RelWithDebInfo'")
    elseif(NOT CMAKE_BUILD_TYPE IN_LIST scylla_build_types)
        message(FATAL_ERROR "Unknown CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}. "
            "Following types are supported: ${scylla_build_types}")
    endif()
endif(DEFINED CMAKE_BUILD_TYPE)

include(mode.common)
if(CMAKE_CONFIGURATION_TYPES)
    foreach(config ${CMAKE_CONFIGURATION_TYPES})
        include(mode.${config})
        list(APPEND scylla_build_modes ${scylla_build_mode})
    endforeach()
    add_custom_target(mode_list
        COMMAND ${CMAKE_COMMAND} -E echo "$<JOIN:${scylla_build_modes}, >"
        COMMENT "List configured modes"
        BYPRODUCTS mode-list.phony.stamp
        COMMAND_EXPAND_LISTS)
else()
    include(mode.${CMAKE_BUILD_TYPE})
    add_custom_target(mode_list
        ${CMAKE_COMMAND} -E echo "${scylla_build_mode}"
        COMMENT "List configured modes")
endif()

add_compile_definitions(
    FMT_DEPRECATED_OSTREAM)
include(limit_jobs)
# Configure Seastar compile options to align with Scylla
set(CMAKE_CXX_STANDARD "20" CACHE INTERNAL "")
set(CMAKE_CXX_EXTENSIONS ON CACHE INTERNAL "")
set(CMAKE_CXX_VISIBILITY_PRESET hidden)

set(Seastar_TESTING ON CACHE BOOL "" FORCE)
set(Seastar_API_LEVEL 7 CACHE STRING "" FORCE)
set(Seastar_APPS ON CACHE BOOL "" FORCE)
set(Seastar_EXCLUDE_APPS_FROM_ALL ON CACHE BOOL "" FORCE)
set(Seastar_UNUSED_RESULT_ERROR ON CACHE BOOL "" FORCE)
add_subdirectory(seastar)

# System libraries dependencies
find_package(Boost REQUIRED
    COMPONENTS filesystem program_options system thread regex unit_test_framework)
target_link_libraries(Boost::regex
  INTERFACE
    ICU::i18n
    ICU::uc)
find_package(Lua REQUIRED)
find_package(ZLIB REQUIRED)
find_package(ICU COMPONENTS uc i18n REQUIRED)
find_package(absl COMPONENTS hash raw_hash_set REQUIRED)
find_package(libdeflate REQUIRED)
find_package(libxcrypt REQUIRED)
find_package(Snappy REQUIRED)
find_package(RapidJSON REQUIRED)
find_package(Thrift REQUIRED)
find_package(xxHash REQUIRED)

set(scylla_gen_build_dir "${CMAKE_BINARY_DIR}/gen")
file(MAKE_DIRECTORY "${scylla_gen_build_dir}")

include(add_version_library)
generate_scylla_version()

add_library(scylla-main STATIC)
target_sources(scylla-main
  PRIVATE
    absl-flat_hash_map.cc
    bytes.cc
    client_data.cc
    clocks-impl.cc
    collection_mutation.cc
    compress.cc
    converting_mutation_partition_applier.cc
    counters.cc
    direct_failure_detector/failure_detector.cc
    duration.cc
    exceptions/exceptions.cc
    frozen_schema.cc
    generic_server.cc
    debug.cc
    init.cc
    keys.cc
    multishard_mutation_query.cc
    mutation_query.cc
    partition_slice_builder.cc
    querier.cc
    query.cc
    query_ranges_to_vnodes.cc
    query-result-set.cc
    tombstone_gc_options.cc
    tombstone_gc.cc
    reader_concurrency_semaphore.cc
    row_cache.cc
    schema_mutations.cc
    serializer.cc
    sstables_loader.cc
    table_helper.cc
    tasks/task_manager.cc
    timeout_config.cc
    unimplemented.cc
    validation.cc
    vint-serialization.cc
    zstd.cc)
target_link_libraries(scylla-main
  PRIVATE
    db
    absl::hash
    absl::raw_hash_set
    Seastar::seastar
    Snappy::snappy
    systemd
    ZLIB::ZLIB)

option(Scylla_CHECK_HEADERS
  "Add check-headers target for checking the self-containness of headers")
if(Scylla_CHECK_HEADERS)
    add_custom_target(check-headers)
    # compatibility target used by CI, which builds "dev-headers" for building
    # "headers" of the "dev" mode. but we don't have such an alias. what we
    # have is check-headers:Dev, and we don't want to build check-headers
    # for all "CMAKE_DEFAULT_CONFIGS". so, before we , let's add an target
    # which only build build check-headers for Dev, and skip it otherwise.
    add_custom_target(dev-headers
        COMMAND ${CMAKE_COMMAND}
          "$<IF:$<CONFIG:Dev>,--build;${CMAKE_BINARY_DIR};--config;$<CONFIG>;--target;check-headers,-E;echo;skipping;dev-headers;in;$<CONFIG>>"
        COMMAND_EXPAND_LISTS)
endif()

include(check_headers)
check_headers(check-headers scylla-main
  GLOB ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)

add_subdirectory(api)
add_subdirectory(alternator)
add_subdirectory(db)
add_subdirectory(auth)
add_subdirectory(cdc)
add_subdirectory(compaction)
add_subdirectory(cql3)
add_subdirectory(data_dictionary)
add_subdirectory(dht)
add_subdirectory(gms)
add_subdirectory(idl)
add_subdirectory(index)
add_subdirectory(interface)
add_subdirectory(lang)
add_subdirectory(locator)
add_subdirectory(message)
add_subdirectory(mutation)
add_subdirectory(mutation_writer)
add_subdirectory(node_ops)
add_subdirectory(readers)
add_subdirectory(redis)
add_subdirectory(replica)
add_subdirectory(raft)
add_subdirectory(repair)
add_subdirectory(rust)
add_subdirectory(schema)
add_subdirectory(service)
add_subdirectory(sstables)
add_subdirectory(streaming)
add_subdirectory(test)
add_subdirectory(thrift)
add_subdirectory(tools)
add_subdirectory(tracing)
add_subdirectory(transport)
add_subdirectory(types)
add_subdirectory(utils)
add_version_library(scylla_version
    release.cc)

add_executable(scylla
  main.cc)
target_link_libraries(scylla PRIVATE
    scylla-main
    api
    auth
    alternator
    db
    cdc
    compaction
    cql3
    data_dictionary
    dht
    gms
    idl
    index
    lang
    locator
    message
    mutation
    mutation_writer
    raft
    readers
    redis
    repair
    replica
    schema
    scylla_version
    service
    sstables
    streaming
    test-perf
    thrift
    tools
    tracing
    transport
    types
    utils)

target_link_libraries(scylla PRIVATE
    seastar
    Boost::program_options)

target_include_directories(scylla PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${scylla_gen_build_dir}")

add_subdirectory(dist)
