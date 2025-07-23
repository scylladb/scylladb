add_library(types STATIC)
target_sources(types
  PRIVATE
    comparable_bytes.cc
    types.cc)
target_include_directories(types
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(types
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    cql3
    idl
    scylla-precompiled-header)

check_headers(check-headers types
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
