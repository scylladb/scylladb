add_library(cdc STATIC)
target_sources(cdc
  PRIVATE
    cdc_partitioner.cc
    generation.cc
    log.cc
    metadata.cc
    split.cc)
target_include_directories(cdc
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(cdc
  PUBLIC
    absl::headers
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    replica)

check_headers(check-headers cdc
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
