add_library(tracing STATIC)
target_sources(tracing
  PRIVATE
    tracing.cc
    trace_keyspace_helper.cc
    trace_state.cc
    traced_file.cc)
target_include_directories(tracing
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(tracing
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    service)
