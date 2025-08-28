add_library(raft STATIC)
target_sources(raft
  PRIVATE
    fsm.cc
    log.cc
    raft.cc
    server.cc
    tracker.cc)
target_include_directories(raft
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(raft
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    absl::headers)

check_headers(check-headers raft
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
