# Generate C++ sources from ragel grammar files
seastar_generate_ragel(
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
    db)

check_headers(check-headers redis
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
