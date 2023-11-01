include(add_whole_archive)

add_library(scylla_locator STATIC)
target_sources(scylla_locator
  PRIVATE
    abstract_replication_strategy.cc
    azure_snitch.cc
    simple_strategy.cc
    local_strategy.cc
    network_topology_strategy.cc
    everywhere_replication_strategy.cc
    token_metadata.cc
    snitch_base.cc
    simple_snitch.cc
    rack_inferring_snitch.cc
    gossiping_property_file_snitch.cc
    production_snitch_base.cc
    ec2_snitch.cc
    ec2_multi_region_snitch.cc
    gce_snitch.cc
    tablets.cc
    topology.cc
    util.cc)
target_include_directories(scylla_locator
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(scylla_locator
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    db)

add_whole_archive(locator scylla_locator)

check_headers(check-headers scylla_locator
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
