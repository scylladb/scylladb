add_library(test-raft)
target_sources(test-raft
  PRIVATE
    helpers.cc
    replication.cc)
target_link_libraries(test-raft
  test-lib
  Seastar::seastar_testing)

add_scylla_test(discovery_test
  KIND BOOST
  LIBRARIES test-raft)
add_scylla_test(etcd_test
  KIND BOOST
  LIBRARIES test-raft)
add_scylla_test(failure_detector_test
  KIND SEASTAR
  LIBRARIES test-raft)
add_scylla_test(fsm_test
  KIND BOOST
  LIBRARIES test-raft)
add_scylla_test(many_test
  KIND SEASTAR
  LIBRARIES test-raft)
add_scylla_test(raft_address_map_test
  KIND SEASTAR)
add_scylla_test(raft_server_test
  KIND SEASTAR
  LIBRARIES test-raft)
add_scylla_test(raft_sys_table_storage_test
  KIND SEASTAR)
add_scylla_test(randomized_nemesis_test
  KIND SEASTAR
  LIBRARIES test-raft)
add_scylla_test(replication_test
  KIND SEASTAR
  LIBRARIES test-raft)

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/suite.yaml")
  set(scylla_tests "${scylla_tests}" PARENT_SCOPE)
endif()
