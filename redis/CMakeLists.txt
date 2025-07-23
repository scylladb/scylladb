# Generate C++ sources from ragel grammar files

find_package(ragel 6.10 REQUIRED)

function(generate_ragel)
  set(one_value_args TARGET VAR IN_FILE OUT_FILE)
  cmake_parse_arguments(args "" "${one_value_args}" "" ${ARGN})
  get_filename_component(out_dir ${args_OUT_FILE} DIRECTORY)

  add_custom_command(
    DEPENDS ${args_IN_FILE}
    OUTPUT ${args_OUT_FILE}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${out_dir}
    COMMAND ${ragel_RAGEL_EXECUTABLE} -G2 -o ${args_OUT_FILE} ${args_IN_FILE}
    COMMAND sed -i -e "'1h;2,$$H;$$!d;g'" -re "'s/static const char _nfa[^;]*;//g'" ${args_OUT_FILE})

  add_custom_target(${args_TARGET}
    DEPENDS ${args_OUT_FILE})

  set(${args_VAR} ${args_OUT_FILE} PARENT_SCOPE)
endfunction()

generate_ragel(
    TARGET scylla_ragel_gen_protocol_parser
    VAR scylla_ragel_gen_protocol_parser_file
    IN_FILE "${CMAKE_CURRENT_SOURCE_DIR}/protocol_parser.rl"
    OUT_FILE ${scylla_gen_build_dir}/redis/protocol_parser.hh)

add_library(redis STATIC)
add_dependencies(redis
  scylla_ragel_gen_protocol_parser)
target_sources(redis
  PRIVATE
    controller.cc
    server.cc
    query_processor.cc
    keyspace_utils.cc
    options.cc
    stats.cc
    mutation_utils.cc
    query_utils.cc
    abstract_command.cc
    command_factory.cc
    commands.cc
    lolwut.cc)
target_include_directories(redis
  PUBLIC
    ${CMAKE_SOURCE_DIR}
  PRIVATE
    ${scylla_gen_build_dir})
target_link_libraries(redis
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    db
    scylla-precompiled-header)

check_headers(check-headers redis
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
