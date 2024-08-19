add_library(service STATIC)
target_sources(service
  PRIVATE
    broadcast_tables/experimental/lang.cc
    client_state.cc
    mapreduce_service.cc
    migration_manager.cc
    misc_services.cc
    pager/paging_state.cc
    pager/query_pagers.cc
    paxos/paxos_state.cc
    paxos/prepare_response.cc
    paxos/prepare_summary.cc
    paxos/proposal.cc
    qos/qos_common.cc
    qos/service_level_controller.cc
    qos/standard_service_level_distributed_data_accessor.cc
    qos/raft_service_level_distributed_data_accessor.cc
    raft/discovery.cc
    raft/group0_state_machine.cc
    raft/group0_state_machine_merger.cc
    raft/raft_group0.cc
    raft/raft_group0_client.cc
    raft/raft_group_registry.cc
    raft/raft_rpc.cc
    raft/raft_sys_table_storage.cc
    session.cc
    storage_proxy.cc
    storage_service.cc
    tablet_allocator.cc
    topology_coordinator.cc
    topology_mutation.cc
    topology_state_machine.cc)
target_include_directories(service
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(service
  PUBLIC
    db
    absl::headers
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    mutation
    node_ops
    raft
    repair
    streaming
    systemd)

check_headers(check-headers service
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)

add_library(storage_proxy.o OBJECT EXCLUDE_FROM_ALL
    storage_proxy.cc)
target_include_directories(storage_proxy.o
  PRIVATE
    ${CMAKE_SOURCE_DIR})
target_link_libraries(storage_proxy.o
  absl::headers
  Seastar::seastar
  xxHash::xxhash
  idl)
add_custom_target(maybe-storage-proxy
  DEPENDS $<$<CONFIG:RelWithDebInfo,Debug>:storage_proxy.o>)
add_dependencies(compiler-training
  maybe-storage-proxy)
