add_library(replica STATIC)
target_sources(replica
  PRIVATE
    distributed_loader.cc
    database.cc
    table.cc
    tablets.cc
    distributed_loader.cc
    memtable.cc
    exceptions.cc
    dirty_memory_manager.cc
    multishard_query.cc
    mutation_dump.cc
    schema_describe_helper.cc
    querier.cc)
target_include_directories(replica
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(replica
  PUBLIC
    db
    absl::headers
    wasmtime_bindings
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    absl::raw_hash_set)
if (Scylla_USE_PRECOMPILED_HEADER_USE)
  target_precompile_headers(replica REUSE_FROM scylla-precompiled-header)
endif()

check_headers(check-headers replica
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
