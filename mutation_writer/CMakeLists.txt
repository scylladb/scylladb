add_library(mutation_writer STATIC)
target_sources(mutation_writer
  PRIVATE
    feed_writers.cc
    multishard_writer.cc
    partition_based_splitting_writer.cc
    shard_based_splitting_writer.cc
    timestamp_based_splitting_writer.cc
    token_group_based_splitting_writer.cc)
target_include_directories(mutation_writer
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(mutation_writer
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    schema)

check_headers(check-headers mutation_writer
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
