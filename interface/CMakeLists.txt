# Generate C++ source files from thrift definitions
function(scylla_generate_thrift)
    set(one_value_args TARGET VAR IN_FILE OUT_DIR SERVICE)
    cmake_parse_arguments(args "" "${one_value_args}" "" ${ARGN})

    get_filename_component(in_file_name ${args_IN_FILE} NAME_WE)

    set(aux_out_file_name ${args_OUT_DIR}/${in_file_name})
    set(outputs
        ${aux_out_file_name}_types.cpp
        ${aux_out_file_name}_types.h
        ${aux_out_file_name}_constants.cpp
        ${aux_out_file_name}_constants.h
        ${args_OUT_DIR}/${args_SERVICE}.cpp
        ${args_OUT_DIR}/${args_SERVICE}.h)

    find_program(THRIFT thrift)
    if(NOT THRIFT)
      message(FATAL "thrift is required")
    endif()
    add_custom_command(
        DEPENDS ${args_IN_FILE}
        OUTPUT ${outputs}
        COMMAND ${CMAKE_COMMAND} -E make_directory ${args_OUT_DIR}
        COMMAND ${THRIFT} -gen cpp:cob_style,no_skeleton -out "${args_OUT_DIR}" "${args_IN_FILE}")

    add_custom_target(${args_TARGET}
        DEPENDS ${outputs})

    set(${args_VAR} ${outputs} PARENT_SCOPE)
endfunction()

scylla_generate_thrift(
    TARGET scylla_thrift_gen_cassandra
    VAR scylla_thrift_gen_cassandra_files
    IN_FILE "${CMAKE_CURRENT_SOURCE_DIR}/cassandra.thrift"
    OUT_DIR ${scylla_gen_build_dir}
    SERVICE Cassandra)

add_library(interface STATIC)
target_sources(interface
  PRIVATE
    ${scylla_thrift_gen_cassandra_files})
