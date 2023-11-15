function (check_headers check_headers_target target)
  if(NOT TARGET ${check_headers_target})
    return()
  endif()

  # "target" is required, so we can get the compiling options for compiling
  # the headers.
  cmake_parse_arguments (
    parsed_args
    ""
    "GLOB;GLOB_RECURSE;EXCLUDE;INCLUDE"
    ""
    ${ARGN})

  set(sources "")
  if(DEFINED parsed_args_GLOB)
    file(GLOB sources
      LIST_DIRECTORIES false
      RELATIVE ${CMAKE_SOURCE_DIR}
      "${parsed_args_GLOB}")
  elseif(DEFINED parsed_args_GLOB_RECURSE)
    file(GLOB_RECURSE sources
      LIST_DIRECTORIES false
      RELATIVE ${CMAKE_SOURCE_DIR}
      "${parsed_args_RECURSIVE}")
  else()
    message(FATAL_ERROR "Please specify GLOB or GLOB_RECURSE.")
  endif()
  if(DEFINED parsed_args_INCLUDE)
    list(FILTER sources INCLUDE REGEX "${parsed_args_INCLUDE}")
  endif()
  if(DEFINED parsed_args_EXCLUDE)
    list(FILTER sources EXCLUDE REGEX "${parsed_args_EXCLUDE}")
  endif()

  foreach(fn ${sources})
    get_filename_component(file_dir ${fn} DIRECTORY)
    list(APPEND includes "${CMAKE_SOURCE_DIR}/${file_dir}")
    set(src_dir "${CMAKE_BINARY_DIR}/check-headers/${file_dir}")
    file(MAKE_DIRECTORY "${src_dir}")
    get_filename_component(file_name ${fn} NAME)
    set(src "${src_dir}/${file_name}.cc")
    # CMake refuses to compile .hh files, so we need to rename them first.
    add_custom_command(
      OUTPUT ${src}
      DEPENDS ${CMAKE_SOURCE_DIR}/${fn}
      # silence "-Wpragma-once-outside-header"
      COMMAND sed
            -e "s/^#pragma once//"
            "${fn}" > "${src}"
      WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
      VERBATIM)
    list(APPEND srcs "${src}")
  endforeach()

  if(NOT srcs)
    # not headers to checks
    return()
  endif()

  set(check_lib "${check_headers_target}-${target}")
  add_library(${check_lib} EXCLUDE_FROM_ALL)
  target_sources(${check_lib}
    PRIVATE ${srcs})
  # use ${target} as an interface library by consuming all of its
  # compile time options
  get_target_property(libraries ${target} LINK_LIBRARIES)
  if (libraries)
    # as a side effect, the libraries linked by the ${target} are also built as
    # the dependencies of ${check_lib}. some of the libraries are scylla
    # libraries. we could tell them from the 3rd party libraries, but we would
    # have to recursively pull in their compiling options. so, since that
    # we always build "check-headers" target along with "scylla", this does
    # not incur overhead.
    target_link_libraries(${check_lib}
      PRIVATE ${libraries})
  endif()

  # if header includes other header files with relative path,
  # we should satisfy it.
  list(REMOVE_DUPLICATES includes)
  target_include_directories(${check_lib}
    PRIVATE ${includes})
  get_target_property(includes ${target} INCLUDE_DIRECTORIES)
  if(includes)
    target_include_directories(${check_lib}
      PRIVATE ${includes})
  endif()

  get_target_property(compile_options ${target} COMPILE_OPTIONS)
  if(compile_options)
    target_compile_options(${check_lib}
      PRIVATE ${compile_options})
  endif ()
  # symbols in header file should always be referenced, but these
  # are just pure headers, so unused variables should be tolerated.
  target_compile_options(${check_lib}
    PRIVATE
      -Wno-unused-const-variable
      -Wno-unused-function
      -Wno-unused-variable)

  get_target_property(compile_definitions ${target} COMPILE_DEFINITIONS)
  if(compile_definitions)
    target_compile_definitions(${check_lib}
      PRIVATE ${compile_definitions})
  endif()

  add_dependencies(${check_headers_target} ${check_lib})
endfunction ()
