#
# Copyright 2026-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Embed the contents of files listed in a .resources manifest into the
# executable. scripts/resources.py turns the manifest into a header holding the
# gzip-compressed content of each file plus a manifest listing them; see
# resources.hh for how to use the result.
#
# The header is generated into ${scylla_gen_build_dir}/resources/ under the
# manifest's path relative to the source root, so that a source can include it
# the same way in both build systems:
#
#   generate_resources(MANIFEST webshell/webshell.resources VAR gen_files)
#   -> #include "resources/tools/webshell/webshell.resources.hh"
#
# Arguments:
#   MANIFEST - the .resources file, absolute or relative to the current dir.
#   VAR      - name of the variable to receive the generated header, to be added
#              to the consuming target's sources.
#   TARGET   - optional name for a custom target depending on the header, for
#              consumers that cannot list it as a source.

function(generate_resources)
  set(one_value_args MANIFEST VAR TARGET)
  cmake_parse_arguments(args "" "${one_value_args}" "" ${ARGN})

  if(IS_ABSOLUTE "${args_MANIFEST}")
    set(manifest "${args_MANIFEST}")
  else()
    set(manifest "${CMAKE_CURRENT_SOURCE_DIR}/${args_MANIFEST}")
  endif()

  if(NOT args_MANIFEST)
    message(FATAL_ERROR "generate_resources: MANIFEST is required")
  endif()
  if(NOT DEFINED scylla_gen_build_dir)
    message(FATAL_ERROR "generate_resources: scylla_gen_build_dir is not set")
  endif()

  set(generator "${PROJECT_SOURCE_DIR}/scripts/resources.py")

  file(RELATIVE_PATH manifest_relative "${PROJECT_SOURCE_DIR}" "${manifest}")
  set(header_out "${scylla_gen_build_dir}/resources/${manifest_relative}.hh")

  # The header embeds the files the manifest lists, so it has to be regenerated
  # when any of them changes and not only when the manifest itself does --
  # otherwise a build silently ships stale resources. resources.py resolves each
  # "file" relative to the manifest.
  get_filename_component(manifest_dir "${manifest}" DIRECTORY)
  file(READ "${manifest}" manifest_json)
  string(JSON resource_count LENGTH "${manifest_json}")
  set(resource_files "")
  if(resource_count GREATER 0)
    math(EXPR last_resource "${resource_count} - 1")
    foreach(i RANGE ${last_resource})
      string(JSON resource_file GET "${manifest_json}" ${i} "file")
      list(APPEND resource_files "${manifest_dir}/${resource_file}")
    endforeach()
  endif()

  # That dependency list is computed here, at configure time, so adding or
  # removing an entry in the manifest has to re-run CMake.
  set_property(DIRECTORY APPEND
    PROPERTY CMAKE_CONFIGURE_DEPENDS "${manifest}")

  add_custom_command(
    DEPENDS
      ${manifest}
      ${generator}
      ${resource_files}
    OUTPUT ${header_out}
    COMMAND ${generator} --input-file ${manifest} --output-file ${header_out}
    COMMENT "Generating resources from ${manifest_relative}"
    VERBATIM)

  if(args_TARGET)
    add_custom_target(${args_TARGET}
      DEPENDS ${header_out})
  endif()

  if(args_VAR)
    set(${args_VAR} ${header_out} PARENT_SCOPE)
  endif()
endfunction()
