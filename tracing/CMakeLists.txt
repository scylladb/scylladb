include(add_whole_archive)

add_library(scylla_tracing STATIC)
target_sources(scylla_tracing
  PRIVATE
    tracing.cc
    trace_keyspace_helper.cc
    trace_state.cc
    traced_file.cc)
target_include_directories(scylla_tracing
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(scylla_tracing
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    service
    scylla-main)

add_whole_archive(tracing scylla_tracing)

check_headers(check-headers scylla_tracing
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
