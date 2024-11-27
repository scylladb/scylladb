add_library(test-raft-helper OBJECT)
target_sources(test-raft-helper
  PRIVATE
    helpers.cc)
target_link_libraries(test-raft-helper
  test-lib
  Seastar::seastar_testing)

add_library(test-raft STATIC)
target_sources(test-raft
  PRIVATE
    replication.cc)
target_link_libraries(test-raft
  test-raft-helper
  test-lib
  Seastar::seastar_testing)

add_scylla_test(raft_boost_combined_tests
  KIND BOOST
  LIBRARIES test-raft
  SOURCES
    raft_boost_combined_tests.cc
    discovery_test.cc
    etcd_test.cc
    fsm_test.cc)
add_scylla_test(raft_seastar_combined_tests
  KIND SEASTAR
  LIBRARIES
    test-raft
    test-raft-helper
  SOURCES
    raft_seastar_combined_tests.cc
    failure_detector_test.cc
    many_test.cc
    raft_address_map_test.cc
    raft_server_test.cc
    raft_sys_table_storage_test.cc
    randomized_nemesis_test.cc
    replication_test.cc)


if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/suite.yaml")
  set(scylla_tests "${scylla_tests}" PARENT_SCOPE)
endif()
