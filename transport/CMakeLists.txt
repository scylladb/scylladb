add_library(transport STATIC)
target_sources(transport
  PRIVATE
    controller.cc
    cql_protocol_extension.cc
    event.cc
    event_notifier.cc
    messages/result_message.cc
    server.cc)
target_include_directories(transport
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(transport
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    cql3
    Snappy::snappy)

check_headers(check-headers transport
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
