add_library(fts STATIC)
target_sources(fts
  PRIVATE
    fts_cdc_consumer.cc)
target_include_directories(fts
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(fts
  PUBLIC
    Seastar::seastar
  PRIVATE
    cdc
    cql3
    index
    fts_bindings)

if (Scylla_USE_PRECOMPILED_HEADER_USE)
  target_precompile_headers(fts REUSE_FROM scylla-precompiled-header)
endif()
check_headers(check-headers fts
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
