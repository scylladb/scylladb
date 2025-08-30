add_library(readers STATIC)
target_sources(readers
  PRIVATE
    combined.cc
    multishard.cc
    mutation_reader.cc
    mutation_readers.cc)
target_include_directories(readers
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(readers
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    schema)

check_headers(check-headers readers
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
