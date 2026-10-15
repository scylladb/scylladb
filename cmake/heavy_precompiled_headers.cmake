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
# object depend on a header of ours it didn't depend on before.
#
# Which precompiled headers exist, which one each is built on top of, and
# which source uses which, are all decided by
# scripts/choose_precompiled_header.py, which configure.py uses too; see its
# HEAVY_PRECOMPILED_HEADERS list. There is nothing to configure here.
#
# CMake's target_precompile_headers() can only give the same precompiled
# header to all the sources of a target, so we build these precompiled
# headers ourselves, each in an OBJECT library whose only "object" is the
# precompiled header, and point the chosen sources at it.

find_package(Python3 COMPONENTS Interpreter REQUIRED)

# Configure again when the list of heavy precompiled headers changes.
set_property(DIRECTORY ${CMAKE_SOURCE_DIR} APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
  ${CMAKE_SOURCE_DIR}/scripts/choose_precompiled_header.py)

function(_scylla_choose_precompiled_header out_var)
  execute_process(
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/choose_precompiled_header.py
      --source-dir ${CMAKE_SOURCE_DIR} ${ARGN}
    OUTPUT_VARIABLE out
    COMMAND_ERROR_IS_FATAL ANY)
  # The script prints one item per line.
  string(REGEX REPLACE "\n$" "" out "${out}")
  string(REPLACE "\n" ";" out "${out}")
  set(${out_var} "${out}" PARENT_SCOPE)
endfunction()

# Builds the heavy precompiled header <header> (relative to the top of the
# source tree) in the OBJECT library <target>, on top of its parent: the
# precompiled header of stdafx.hh (scylla-precompiled-header), or a heavy
# precompiled header that was already added. It gets the same libraries as
# scylla-precompiled-header, plus our own headers, including the generated
# ones. It must not link our libraries, because their sources may use it.
function(_scylla_add_heavy_precompiled_header target header)
  _scylla_choose_precompiled_header(parent --parent ${header})
  set(path "${CMAKE_SOURCE_DIR}/${header}")
  add_library(${target} OBJECT ${path})
  set(options "-x;c++-header;-Xclang;-emit-pch;-fpch-instantiate-templates")
  if(parent STREQUAL "stdafx.hh")
    target_precompile_headers(${target} REUSE_FROM scylla-precompiled-header)
  else()
    string(MAKE_C_IDENTIFIER "${parent}" parent_id)
    get_property(parent_target GLOBAL PROPERTY SCYLLA_HEAVY_PRECOMPILED_HEADER_${parent_id})
    list(APPEND options "-Xclang;-include-pch;-Xclang;$<TARGET_OBJECTS:${parent_target}>")
    add_dependencies(${target} ${parent_target}-built)
  endif()
  set_source_files_properties(${path} PROPERTIES
    LANGUAGE CXX
    COMPILE_OPTIONS "${options}")
  target_compile_definitions(${target} PRIVATE SCYLLA_USE_PRECOMPILED_HEADER)
  target_include_directories(${target} PRIVATE ${CMAKE_SOURCE_DIR})
  get_target_property(libraries scylla-precompiled-header LINK_LIBRARIES)
  target_link_libraries(${target} PRIVATE ${libraries} idl)
  # With Ninja, compiling a target's sources waits for the custom targets it
  # depends on, but not for the other targets, so whatever uses this
  # precompiled header depends on this custom target.
  add_custom_target(${target}-built DEPENDS $<TARGET_OBJECTS:${target}>)
  string(MAKE_C_IDENTIFIER "${header}" header_id)
  set_property(GLOBAL PROPERTY SCYLLA_HEAVY_PRECOMPILED_HEADER_${header_id} ${target})
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

# scylla_add_heavy_precompiled_headers()
#
# Adds the heavy precompiled headers listed by
# scripts/choose_precompiled_header.py, and points every source of our
# targets that should use one of them at it. The latter must see all the
# targets, so it is deferred to the end of configuring the top-level
# directory, after all the targets have been defined, wherever this is
# called from.
function(scylla_add_heavy_precompiled_headers)
  # --list lists every precompiled header after the one it is built on.
  _scylla_choose_precompiled_header(headers --list)
  foreach(header ${headers})
    # e.g., test/lib/stdafx_cql_test_env.hh -> scylla-precompiled-header-cql_test_env
    get_filename_component(name ${header} NAME_WE)
    string(REGEX REPLACE "^stdafx_" "" name ${name})
    set(target scylla-precompiled-header-${name})
    if(TARGET ${target})
      message(FATAL_ERROR "Two heavy precompiled headers are named ${name}")
    endif()
    _scylla_add_heavy_precompiled_header(${target} ${header})
  endforeach()
  cmake_language(DEFER DIRECTORY ${CMAKE_SOURCE_DIR} CALL _scylla_use_heavy_precompiled_headers)
endfunction()

function(_scylla_use_heavy_precompiled_headers)
  _scylla_get_targets(${CMAKE_SOURCE_DIR} targets)
  set(sources)
  foreach(target ${targets})
    if(target MATCHES "^scylla-precompiled-header-")
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
  # One line for each source in ${paths}, in the same order: the precompiled
  # header that source should use, or "-".
  _scylla_choose_precompiled_header(choices ${paths})
  list(LENGTH paths npaths)
  list(LENGTH choices nchoices)
  if(NOT npaths EQUAL nchoices)
    message(FATAL_ERROR "choose_precompiled_header.py printed ${nchoices} lines for ${npaths} sources")
  endif()
  foreach(src header IN ZIP_LISTS paths choices)
    if(header STREQUAL "-")
      continue()
    endif()
    string(MAKE_C_IDENTIFIER "${header}" header_id)
    get_property(pch_target GLOBAL PROPERTY SCYLLA_HEAVY_PRECOMPILED_HEADER_${header_id})
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
