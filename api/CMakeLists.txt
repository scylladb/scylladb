# Generate C++ sources from Swagger definitions

function(generate_swagger)
  set(one_value_args TARGET VAR IN_FILE OUT_DIR)
  cmake_parse_arguments(args "" "${one_value_args}" "" ${ARGN})
  get_filename_component(in_file_name ${args_IN_FILE} NAME)
  set(generator ${PROJECT_SOURCE_DIR}/seastar/scripts/seastar-json2code.py)
  set(header_out ${args_OUT_DIR}/${in_file_name}.hh)
  set(source_out ${args_OUT_DIR}/${in_file_name}.cc)

  add_custom_command(
    DEPENDS
      ${args_IN_FILE}
      ${generator}
    OUTPUT ${header_out} ${source_out}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${args_OUT_DIR}
    COMMAND ${generator} --create-cc -f ${args_IN_FILE} -o ${header_out})

  add_custom_target(${args_TARGET}
    DEPENDS
      ${header_out}
      ${source_out})

  set(${args_VAR} ${header_out} ${source_out} PARENT_SCOPE)
endfunction()

set(swagger_files
  api-doc/authorization_cache.json
  api-doc/cache_service.json
  api-doc/collectd.json
  api-doc/column_family.json
  api-doc/commitlog.json
  api-doc/compaction_manager.json
  api-doc/config.json
  api-doc/cql_server_test.json
  api-doc/endpoint_snitch_info.json
  api-doc/error_injection.json
  api-doc/failure_detector.json
  api-doc/gossiper.json
  api-doc/hinted_handoff.json
  api-doc/lsa.json
  api-doc/messaging_service.json
  api-doc/metrics.json
  api-doc/raft.json
  api-doc/service_levels.json
  api-doc/storage_proxy.json
  api-doc/storage_service.json
  api-doc/stream_manager.json
  api-doc/system.json
  api-doc/tasks.json
  api-doc/task_manager.json
  api-doc/task_manager_test.json
  api-doc/utils.json)

foreach(f ${swagger_files})
  get_filename_component(fname "${f}" NAME_WE)
  get_filename_component(dir "${f}" DIRECTORY)
  generate_swagger(
    TARGET scylla_swagger_gen_${fname}
    VAR scylla_swagger_gen_${fname}_files
    IN_FILE "${CMAKE_CURRENT_SOURCE_DIR}/${f}"
    OUT_DIR "${scylla_gen_build_dir}/api/${dir}")
  list(APPEND swagger_gen_files "${scylla_swagger_gen_${fname}_files}")
endforeach()

add_library(api STATIC)
target_sources(api
  PRIVATE
    api.cc
    cache_service.cc
    collectd.cc
    column_family.cc
    commitlog.cc
    compaction_manager.cc
    config.cc
    cql_server_test.cc
    endpoint_snitch.cc
    error_injection.cc
    authorization_cache.cc
    failure_detector.cc
    gossiper.cc
    hinted_handoff.cc
    lsa.cc
    messaging_service.cc
    raft.cc
    service_levels.cc
    storage_proxy.cc
    storage_service.cc
    stream_manager.cc
    system.cc
    tasks.cc
    task_manager.cc
    task_manager_test.cc
    token_metadata.cc
    ${swagger_gen_files})
target_include_directories(api
  PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${scylla_gen_build_dir})
target_link_libraries(api
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    idl
    wasmtime_bindings
    absl::headers)

check_headers(check-headers api
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
