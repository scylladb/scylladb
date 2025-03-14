cmake_minimum_required(VERSION 3.27)

project(scylla)

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

option(Scylla_ENABLE_LTO "Turn on link-time optimization for the 'release' mode." ON)

include(mode.common)
get_property(is_multi_config GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)
if(is_multi_config)
    foreach(config ${CMAKE_CONFIGURATION_TYPES})
        include(mode.${config})
        list(APPEND scylla_build_modes ${scylla_build_mode_${config}})
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

include(limit_jobs)

# Configure Seastar compile options to align with Scylla
set(CMAKE_CXX_STANDARD "23" CACHE INTERNAL "")
set(CMAKE_CXX_EXTENSIONS ON CACHE INTERNAL "")
set(CMAKE_CXX_SCAN_FOR_MODULES OFF CACHE INTERNAL "")
set(CMAKE_CXX_VISIBILITY_PRESET hidden)

if(is_multi_config)
    find_package(Seastar)
    # this is atypical compared to standard ExternalProject usage:
    # - Seastar's build system should already be configured at this point.
    # - We maintain separate project variants for each configuration type.
    #
    # Benefits of this approach:
    # - Allows the parent project to consume the compile options exposed by
    #   .pc file. as the compile options vary from one config to another.
    # - Allows application of config-specific settings
    # - Enables building Seastar within the parent project's build system
    # - Facilitates linking of artifacts with the external project target,
    #   establishing proper dependencies between them
    include(ExternalProject)

    # should be consistent with configure_seastar() in configure.py
    set(seastar_build_dir "${CMAKE_BINARY_DIR}/$<CONFIG>/seastar")
    ExternalProject_Add(Seastar
        SOURCE_DIR "${PROJECT_SOURCE_DIR}/seastar"
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ${CMAKE_COMMAND} --build "${seastar_build_dir}"
          --target seastar
          --target seastar_testing
          --target seastar_perf_testing
          --target app_iotune
        BUILD_ALWAYS ON
        BUILD_BYPRODUCTS
          ${seastar_build_dir}/libseastar.$<IF:$<CONFIG:Debug,Dev>,so,a>
          ${seastar_build_dir}/libseastar_testing.$<IF:$<CONFIG:Debug,Dev>,so,a>
          ${seastar_build_dir}/libseastar_perf_testing.$<IF:$<CONFIG:Debug,Dev>,so,a>
          ${seastar_build_dir}/apps/iotune/iotune
          ${seastar_build_dir}/gen/include/seastar/http/chunk_parsers.hh
          ${seastar_build_dir}/gen/include/seastar/http/request_parser.hh
          ${seastar_build_dir}/gen/include/seastar/http/response_parser.hh
        INSTALL_COMMAND "")
    add_dependencies(Seastar::seastar Seastar)
    add_dependencies(Seastar::seastar_testing Seastar)
else()
    set(Seastar_TESTING ON CACHE BOOL "" FORCE)
    set(Seastar_API_LEVEL 7 CACHE STRING "" FORCE)
    set(Seastar_DEPRECATED_OSTREAM_FORMATTERS OFF CACHE BOOL "" FORCE)
    set(Seastar_APPS ON CACHE BOOL "" FORCE)
    set(Seastar_EXCLUDE_APPS_FROM_ALL ON CACHE BOOL "" FORCE)
    set(Seastar_EXCLUDE_TESTS_FROM_ALL ON CACHE BOOL "" FORCE)
    set(Seastar_IO_URING ON CACHE BOOL "" FORCE)
    set(Seastar_SCHEDULING_GROUPS_COUNT 19 CACHE STRING "" FORCE)
    set(Seastar_UNUSED_RESULT_ERROR ON CACHE BOOL "" FORCE)
    add_subdirectory(seastar)
    target_compile_definitions (seastar
      PRIVATE
        SEASTAR_NO_EXCEPTION_HACK)
endif()

set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "" FORCE)

if(Scylla_ENABLE_LTO)
    list(APPEND absl_cxx_flags $<$<CONFIG:RelWithDebInfo>:${CMAKE_CXX_COMPILE_OPTIONS_IPO};-ffat-lto-objects>)
endif()

find_package(Sanitizers QUIET)
list(APPEND absl_cxx_flags
    $<$<CONFIG:Debug,Sanitize>:$<TARGET_PROPERTY:Sanitizers::address,INTERFACE_COMPILE_OPTIONS>;$<TARGET_PROPERTY:Sanitizers::undefined_behavior,INTERFACE_COMPILE_OPTIONS>>)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    list(APPEND ABSL_GCC_FLAGS ${absl_cxx_flags})
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    list(APPEND ABSL_LLVM_FLAGS ${absl_cxx_flags})
endif()
set(ABSL_DEFAULT_LINKOPTS
    $<$<CONFIG:Debug,Sanitize>:$<TARGET_PROPERTY:Sanitizers::address,INTERFACE_LINK_LIBRARIES>;$<TARGET_PROPERTY:Sanitizers::undefined_behavior,INTERFACE_LINK_LIBRARIES>>)
add_subdirectory(abseil)
add_library(absl-headers INTERFACE)
target_include_directories(absl-headers SYSTEM INTERFACE
    "${PROJECT_SOURCE_DIR}/abseil")
add_library(absl::headers ALIAS absl-headers)

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
find_package(Boost REQUIRED
    COMPONENTS filesystem program_options system thread regex unit_test_framework)
target_link_libraries(Boost::regex
  INTERFACE
    ICU::i18n
    ICU::uc)
find_package(Lua REQUIRED)
find_package(ZLIB REQUIRED)
find_package(ICU COMPONENTS uc i18n REQUIRED)
find_package(fmt 10.0.0 REQUIRED)
find_package(libdeflate REQUIRED)
find_package(libxcrypt REQUIRED)
find_package(p11-kit REQUIRED)
find_package(Snappy REQUIRED)
find_package(RapidJSON REQUIRED)
find_package(xxHash REQUIRED)
find_package(yaml-cpp REQUIRED)
find_package(zstd REQUIRED)
find_package(lz4 REQUIRED)

set(scylla_gen_build_dir "${CMAKE_BINARY_DIR}/gen")
file(MAKE_DIRECTORY "${scylla_gen_build_dir}")

include(add_version_library)
generate_scylla_version()

add_library(scylla-zstd STATIC
    zstd.cc)
target_link_libraries(scylla-zstd
  PRIVATE
    db
    Seastar::seastar
    zstd::libzstd)

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
    service/direct_failure_detector/failure_detector.cc
    duration.cc
    exceptions/exceptions.cc
    frozen_schema.cc
    generic_server.cc
    debug.cc
    init.cc
    keys.cc
    multishard_mutation_query.cc
    mutation_query.cc
    node_ops/task_manager_module.cc
    partition_slice_builder.cc
    querier.cc
    query.cc
    query_ranges_to_vnodes.cc
    query-result-set.cc
    tombstone_gc_options.cc
    tombstone_gc.cc
    reader_concurrency_semaphore.cc
    reader_concurrency_semaphore_group.cc
    schema_mutations.cc
    serializer.cc
    sstables_loader.cc
    table_helper.cc
    tasks/task_handler.cc
    tasks/task_manager.cc
    timeout_config.cc
    unimplemented.cc
    validation.cc
    vint-serialization.cc)
target_link_libraries(scylla-main
  PRIVATE
    "$<LINK_LIBRARY:WHOLE_ARCHIVE,scylla-zstd>"
    db
    absl::headers
    absl::btree
    absl::hash
    absl::raw_hash_set
    Seastar::seastar
    Snappy::snappy
    systemd
    ZLIB::ZLIB
    lz4::lz4_static
    zstd::zstd_static
)

option(Scylla_CHECK_HEADERS
  "Add check-headers target for checking the self-containness of headers")
if(Scylla_CHECK_HEADERS)
    add_custom_target(check-headers)
    # compatibility target used by CI, which builds "check-headers" only for
    # the "Dev" mode.
    # our CI currently builds "dev-headers" using ninja without specify a build
    # mode. where "dev" is actually a prefix encoded in the target name for the
    # underlying "headers" target. while we don't have this convention in CMake
    # targets. in contrast, the "check-headers" which is built for all
    # configurations defined by "CMAKE_DEFAULT_CONFIGS". however, we only need
    # to build "check-headers" for the "Dev" configuration. Therefore, before
    # updating the CI to use build "check-headers:Dev", let's add a new target
    # that specifically builds "check-headers" only for Dev configuration. The
    # new target will do nothing for other configurations.
    add_custom_target(dev-headers
        COMMAND ${CMAKE_COMMAND}
          "$<IF:$<CONFIG:Dev>,--build;${CMAKE_BINARY_DIR};--config;$<CONFIG>;--target;check-headers,-E;echo;skipping;dev-headers;in;$<CONFIG>>"
        COMMAND_EXPAND_LISTS)
endif()

include(check_headers)
check_headers(check-headers scylla-main
  GLOB ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)

option(Scylla_DIST
  "Build dist targets"
  ON)

add_custom_target(compiler-training)

add_subdirectory(api)
add_subdirectory(alternator)
add_subdirectory(audit)
add_subdirectory(db)
add_subdirectory(auth)
add_subdirectory(cdc)
add_subdirectory(compaction)
add_subdirectory(cql3)
add_subdirectory(data_dictionary)
add_subdirectory(dht)
add_subdirectory(ent)
add_subdirectory(gms)
add_subdirectory(idl)
add_subdirectory(index)
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
add_subdirectory(tools)
add_subdirectory(tracing)
add_subdirectory(transport)
add_subdirectory(types)
add_subdirectory(utils)
add_version_library(scylla_version
    release.cc)

add_executable(scylla
  main.cc)
set(scylla_libs
    audit
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
    encryption
    gms
    idl
    index
    lang
    ldap
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
    tools
    tracing
    transport
    types
    utils)
target_link_libraries(scylla PRIVATE
    ${scylla_libs})

if(Scylla_ENABLE_LTO)
  include(enable_lto)
  foreach(target scylla ${scylla_libs})
    enable_lto(${target})
  endforeach()
endif()

target_link_libraries(scylla PRIVATE
    p11-kit::p11-kit
    Seastar::seastar
    absl::headers
    yaml-cpp::yaml-cpp
    Boost::program_options)

target_include_directories(scylla PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${scylla_gen_build_dir}")

add_custom_target(maybe-scylla
  DEPENDS $<$<CONFIG:Dev>:$<TARGET_FILE:scylla>>)
add_dependencies(compiler-training
  maybe-scylla)

if(Scylla_DIST)
  add_subdirectory(dist)
endif()

if(Scylla_BUILD_INSTRUMENTED)
  add_subdirectory(pgo)
endif()
