add_library(data_dictionary STATIC)
target_sources(data_dictionary
  PRIVATE
    data_dictionary.cc)
target_include_directories(data_dictionary
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(data_dictionary
  PRIVATE
    cql3
  PUBLIC
    Seastar::seastar
    xxHash::xxhash)

if (Scylla_USE_PRECOMPILED_HEADER_USE)
  target_precompile_headers(data_dictionary REUSE_FROM scylla-precompiled-header)
endif()
check_headers(check-headers data_dictionary
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
