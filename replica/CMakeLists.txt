add_library(replica STATIC)
target_sources(replica
  PRIVATE
    database.cc
    dirty_memory_manager.cc
    distributed_loader.cc
    distributed_loader.cc
    exceptions.cc
    memtable.cc
    mutation_dump.cc
    table.cc
    tablets.cc)
target_include_directories(replica
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(replica
  PUBLIC
    absl::headers
    db
    Seastar::seastar
    wasmtime_bindings
    xxHash::xxhash
  PRIVATE
    absl::raw_hash_set)

check_headers(check-headers replica
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
