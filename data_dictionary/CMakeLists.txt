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
