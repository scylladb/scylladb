add_library(thrift STATIC)
target_sources(thrift
  PRIVATE
    controller.cc
    handler.cc
    server.cc
    thrift_validation.cc)
target_include_directories(thrift
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(thrift
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    interface
    Thrift::thrift)

check_headers(check-headers thrift
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
