add_library(types STATIC)
target_sources(types
  PRIVATE
    types.cc)
target_include_directories(types
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(types
    cql3
    idl
    Seastar::seastar
    xxHash::xxhash)

check_headers(check-headers types
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
