add_library(repair STATIC)
target_sources(repair
  PRIVATE
    repair.cc
    row_level.cc
    table_check.cc)
target_include_directories(repair
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(repair
  PUBLIC
    idl
    absl::headers
    Seastar::seastar
    xxHash::xxhash)

check_headers(check-headers repair
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
