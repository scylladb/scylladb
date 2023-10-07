cmake_minimum_required(VERSION 3.27)

project(scylla)

include(CTest)

list(APPEND CMAKE_MODULE_PATH
  ${CMAKE_CURRENT_SOURCE_DIR}/cmake
  ${CMAKE_CURRENT_SOURCE_DIR}/seastar/cmake)

# Set the possible values of build type for cmake-gui
set(scylla_build_types
    "Debug" "Release" "Dev" "Sanitize" "Coverage")
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
  ${scylla_build_types})
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE
        STRING "Choose the type of build." FORCE)
    message(WARNING "CMAKE_BUILD_TYPE not specified, Using 'Release'")
elseif(NOT CMAKE_BUILD_TYPE IN_LIST scylla_build_types)
    message(FATAL_ERROR "Unknown CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}. "
        "Following types are supported: ${scylla_build_types}")
endif()
string(TOUPPER "${CMAKE_BUILD_TYPE}" build_mode)
include(mode.${build_mode})
include(mode.common)
add_compile_definitions(
    ${Seastar_DEFINITIONS_${build_mode}}
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
add_subdirectory(seastar)

# System libraries dependencies
find_package(Boost REQUIRED
    COMPONENTS filesystem program_options system thread regex unit_test_framework)
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
target_link_libraries(Boost::regex
  INTERFACE
    ICU::i18n
    ICU::uc)

target_link_libraries(scylla PRIVATE
    seastar
    Boost::program_options)

# Force SHA1 build-id generation
set(default_linker_flags "-Wl,--build-id=sha1")
include(CheckLinkerFlag)
set(Scylla_USE_LINKER
    ""
    CACHE
    STRING
    "Use specified linker instead of the default one")
if(Scylla_USE_LINKER)
    set(linkers "${Scylla_USE_LINKER}")
else()
    set(linkers "lld" "gold")
endif()

foreach(linker ${linkers})
    set(linker_flag "-fuse-ld=${linker}")
    check_linker_flag(CXX ${linker_flag} "CXX_LINKER_HAVE_${linker}")
    if(CXX_LINKER_HAVE_${linker})
        string(APPEND default_linker_flags " ${linker_flag}")
        break()
    elseif(Scylla_USE_LINKER)
        message(FATAL_ERROR "${Scylla_USE_LINKER} is not supported.")
    endif()
endforeach()

set(CMAKE_EXE_LINKER_FLAGS "${default_linker_flags}" CACHE INTERNAL "")

# TODO: patch dynamic linker to match configure.py behavior

target_include_directories(scylla PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${scylla_gen_build_dir}")

add_subdirectory(dist)
