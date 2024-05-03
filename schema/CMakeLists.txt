add_library(schema STATIC)
target_sources(schema
  PRIVATE
    caching_options.cc
    schema.cc
    schema_registry.cc)
target_include_directories(schema
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(schema
  PUBLIC
    cql3
    idl
    Seastar::seastar
    xxHash::xxhash
    absl::headers)

check_headers(check-headers schema
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
