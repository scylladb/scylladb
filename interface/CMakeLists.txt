# Generate C++ source files from thrift definitions
function(scylla_generate_thrift)
    set(one_value_args TARGET VAR THRIFT_VERSION IN_FILE OUT_DIR SERVICE)
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

    find_program(THRIFT thrift
      REQUIRED)
    execute_process(
        COMMAND "${THRIFT}" -version
        OUTPUT_VARIABLE thrift_version_output
        OUTPUT_STRIP_TRAILING_WHITESPACE)
    string(REGEX MATCH "[0-9]+\.[0-9]+\.[0-9]+$"
        thrift_version "${thrift_version_output}")
    set(${args_THRIFT_VERSION} ${thrift_version} PARENT_SCOPE)

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
    THRIFT_VERSION thrift_version
    IN_FILE "${CMAKE_CURRENT_SOURCE_DIR}/cassandra.thrift"
    OUT_DIR ${scylla_gen_build_dir}
    SERVICE Cassandra)

add_library(interface STATIC)
target_sources(interface
  PRIVATE
    ${scylla_thrift_gen_cassandra_files})
target_include_directories(interface
  PUBLIC
    ${scylla_gen_build_dir})
if(thrift_version VERSION_LESS 0.11.0)
    target_compile_definitions(interface
      PUBLIC
        THRIFT_USES_BOOST)
endif()
