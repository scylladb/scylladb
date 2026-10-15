#
# Copyright (C) 2026-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Besides stdafx.hh, which has only headers of external libraries, we have
# precompiled headers that also contain a heavy header of our own, whose
# parsing and template instantiations would otherwise be repeated by every
# source that includes it. A source uses such a precompiled header only if it
# already includes that header, so the precompiled header doesn't make any
# object depend on a header it didn't depend on before. Which source uses
# which precompiled header is decided by scripts/choose_precompiled_header.py,
# which configure.py uses too.
#
# CMake's target_precompile_headers() can only give the same precompiled
# header to all the sources of a target, so we build these precompiled
# headers ourselves, each in an OBJECT library whose only "object" is the
# precompiled header, and point the chosen sources at it.

find_package(Python3 COMPONENTS Interpreter REQUIRED)

# scylla_add_heavy_precompiled_header(<target> <header>)
#
# Builds the precompiled header <header> (relative to the top of the source
# tree) in the OBJECT library <target>. It gets the same libraries as
# scylla-precompiled-header, plus our own headers, including the generated
# ones. It must not link our libraries, because their sources may use it.
function(scylla_add_heavy_precompiled_header target header)
  set(header "${CMAKE_SOURCE_DIR}/${header}")
  add_library(${target} OBJECT ${header})
  set_source_files_properties(${header} PROPERTIES
    LANGUAGE CXX
    COMPILE_OPTIONS "-x;c++-header;-Xclang;-emit-pch;-fpch-instantiate-templates")
  target_compile_definitions(${target} PRIVATE SCYLLA_USE_PRECOMPILED_HEADER)
  target_include_directories(${target} PRIVATE ${CMAKE_SOURCE_DIR})
  get_target_property(libraries scylla-precompiled-header LINK_LIBRARIES)
  target_link_libraries(${target} PRIVATE ${libraries} idl)
  # With Ninja, compiling a target's sources waits for the custom targets it
  # depends on, but not for the other targets, so the sources using the
  # precompiled header depend on this custom target.
  add_custom_target(${target}-built DEPENDS $<TARGET_OBJECTS:${target}>)
endfunction()

# Sets out_var to the targets defined in dir and its subdirectories.
function(_scylla_get_targets dir out_var)
  get_property(targets DIRECTORY ${dir} PROPERTY BUILDSYSTEM_TARGETS)
  get_property(subdirs DIRECTORY ${dir} PROPERTY SUBDIRECTORIES)
  foreach(subdir ${subdirs})
    _scylla_get_targets(${subdir} subdir_targets)
    list(APPEND targets ${subdir_targets})
  endforeach()
  set(${out_var} ${targets} PARENT_SCOPE)
endfunction()

# scylla_use_heavy_precompiled_headers(<header> <target> [<header> <target>]...)
#
# Points every source of our targets that should use one of the heavy
# precompiled headers (named as in scripts/choose_precompiled_header.py) at
# the OBJECT library <target> that builds it. Must run after all targets are
# defined.
function(scylla_use_heavy_precompiled_headers)
  set(pch_targets ${ARGN})
  _scylla_get_targets(${CMAKE_SOURCE_DIR} targets)
  set(sources)
  foreach(target ${targets})
    if(target IN_LIST pch_targets)
      continue()
    endif()
    get_target_property(type ${target} TYPE)
    if(NOT type MATCHES "^(OBJECT_LIBRARY|STATIC_LIBRARY|SHARED_LIBRARY|EXECUTABLE)$")
      continue()
    endif()
    get_target_property(source_dir ${target} SOURCE_DIR)
    cmake_path(IS_PREFIX CMAKE_SOURCE_DIR "${source_dir}" NORMALIZE in_tree)
    foreach(submodule abseil c-ares fmt seastar)
      set(submodule_dir "${CMAKE_SOURCE_DIR}/${submodule}")
      cmake_path(IS_PREFIX submodule_dir "${source_dir}" NORMALIZE in_submodule)
      if(in_submodule)
        set(in_tree FALSE)
      endif()
    endforeach()
    if(NOT in_tree)
      continue()
    endif()
    get_target_property(target_sources ${target} SOURCES)
    foreach(src ${target_sources})
      if(src MATCHES "\\$<" OR NOT src MATCHES "\\.cc$")
        continue()
      endif()
      cmake_path(ABSOLUTE_PATH src BASE_DIRECTORY "${source_dir}" NORMALIZE)
      # Generated sources, such as those that check-headers compiles to
      # check that each header is self-contained, must not get one.
      cmake_path(IS_PREFIX CMAKE_BINARY_DIR "${src}" NORMALIZE generated)
      if(generated)
        continue()
      endif()
      list(APPEND sources "${target}|${src}")
    endforeach()
  endforeach()
  set(paths ${sources})
  list(TRANSFORM paths REPLACE "^[^|]*\\|" "")
  list(REMOVE_DUPLICATES paths)
  execute_process(
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/choose_precompiled_header.py
      --source-dir ${CMAKE_SOURCE_DIR} ${paths}
    OUTPUT_VARIABLE choices
    COMMAND_ERROR_IS_FATAL ANY)
  string(REPLACE "\n" ";" choices "${choices}")
  foreach(choice ${choices})
    string(REPLACE " " ";" choice "${choice}")
    list(GET choice 0 header)
    list(GET choice 1 src)
    list(FIND pch_targets ${header} index)
    if(index EQUAL -1)
      message(FATAL_ERROR "No target builds the precompiled header ${header}")
    endif()
    math(EXPR index "${index} + 1")
    list(GET pch_targets ${index} pch_target)
    foreach(entry ${sources})
      string(REGEX REPLACE "^[^|]*\\|" "" entry_src "${entry}")
      if(NOT entry_src STREQUAL src)
        continue()
      endif()
      string(REGEX REPLACE "\\|.*" "" target "${entry}")
      set_property(SOURCE ${src} TARGET_DIRECTORY ${target} PROPERTY SKIP_PRECOMPILE_HEADERS ON)
      set_property(SOURCE ${src} TARGET_DIRECTORY ${target} APPEND PROPERTY COMPILE_OPTIONS
        "-Winvalid-pch;-Xclang;-include-pch;-Xclang;$<TARGET_OBJECTS:${pch_target}>")
      add_dependencies(${target} ${pch_target}-built)
    endforeach()
  endforeach()
endfunction()
