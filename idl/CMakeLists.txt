find_package(Python3 COMPONENTS QUIET REQUIRED Interpreter)

# Create C++ bindings for IDL serializers
function(compile_idl input)
  cmake_parse_arguments(parsed_args "" "SOURCES;OUT_DIR" "" ${ARGN})
  get_filename_component(basename ${input} NAME_WE)
  get_filename_component(directory ${input} DIRECTORY)
  set(input "${CMAKE_CURRENT_SOURCE_DIR}/${input}")
  if(directory)
    set(directory "${parsed_args_OUT_DIR}/${directory}")
    file(MAKE_DIRECTORY "${directory}")
  else()
    set(directory "${parsed_args_OUT_DIR}")
  endif()
  set(idl_compiler "${CMAKE_SOURCE_DIR}/idl-compiler.py")
  set(output "${directory}/${basename}.dist.hh")
  add_custom_command(
    OUTPUT ${output}
    COMMAND ${Python3_EXECUTABLE} ${idl_compiler} --ns ser -f ${input} -o ${output}
    DEPENDS ${idle_compiler} ${input})
  set(${parsed_args_SOURCES} ${output} PARENT_SCOPE)
endfunction(compile_idl)

set(idl_headers
  gossip_digest.idl.hh
  uuid.idl.hh
  range.idl.hh
  keys.idl.hh
  read_command.idl.hh
  token.idl.hh
  ring_position.idl.hh
  result.idl.hh
  frozen_mutation.idl.hh
  reconcilable_result.idl.hh
  streaming.idl.hh
  paging_state.idl.hh
  frozen_schema.idl.hh
  partition_checksum.idl.hh
  replay_position.idl.hh
  mutation.idl.hh
  query.idl.hh
  idl_test.idl.hh
  commitlog.idl.hh
  tracing.idl.hh
  consistency_level.idl.hh
  cache_temperature.idl.hh
  view.idl.hh
  messaging_service.idl.hh
  paxos.idl.hh
  raft.idl.hh
  raft_storage.idl.hh
  group0.idl.hh
  hinted_handoff.idl.hh
  storage_proxy.idl.hh
  storage_service.idl.hh
  group0_state_machine.idl.hh
  mapreduce_request.idl.hh
  replica_exception.idl.hh
  per_partition_rate_limit_info.idl.hh
  position_in_partition.idl.hh
  experimental/broadcast_tables_lang.idl.hh
  join_node.idl.hh
  utils.idl.hh
  gossip.idl.hh
  )

foreach(idl_header ${idl_headers})
  compile_idl(${idl_header}
    OUT_DIR "${scylla_gen_build_dir}/idl"
    SOURCES srcs)
  list(APPEND idl_sources ${srcs})
endforeach()

add_custom_target(idl-sources
  DEPENDS ${idl_sources})
add_library(idl INTERFACE)
add_dependencies(idl idl-sources)
target_link_directories(idl
  INTERFACE
    absl::headers)
target_include_directories(idl
  INTERFACE
    ${scylla_gen_build_dir})

# *.idl.hh headers are not C++ headers but the ones
# to be processed by the idl compiler, so we don't
# check their self-containness.
