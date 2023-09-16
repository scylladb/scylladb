add_library(node_ops STATIC)
target_sources(node_ops
  PRIVATE
    node_ops_ctl.cc)
target_include_directories(node_ops
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(node_ops
  PUBLIC
    utils
    Seastar::seastar
  PRIVATE
    service)
