find_package(cryptopp REQUIRED)

add_library(index STATIC)
target_sources(index
  PRIVATE
    secondary_index.cc
    secondary_index_manager.cc)
target_include_directories(index
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(index
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    cql3)

check_headers(check-headers index
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
