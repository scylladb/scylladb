#
# Copyright (C) 2026 Scylladb, Ltd.
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Resolve c-ares to the bundled submodule that the top-level CMakeLists.txt
# pulls in with add_subdirectory(c-ares), rather than to whatever the host
# happens to have installed. Scylla's cmake/ directory precedes seastar/cmake
# in CMAKE_MODULE_PATH, so this module shadows Seastar's own Findc-ares.cmake
# and satisfies the in-tree Seastar build's find_package(c-ares) too.
#
# The configure.py build has no equivalent of this file: there, Seastar's own
# Findc-ares.cmake does the finding, and configure.py just points it at the
# submodule with -Dc-ares_ROOT (see configure_c_ares()).

if(NOT TARGET c-ares::cares)
  message(FATAL_ERROR
    "The c-ares submodule has not been added yet. add_subdirectory(c-ares) "
    "must run before anything looks for the c-ares package.")
endif()

foreach(v MAJOR MINOR PATCH)
  file(STRINGS "${CMAKE_SOURCE_DIR}/c-ares/include/ares_version.h" ares_VERSION_LINE
    REGEX "^#define[ \t]+ARES_VERSION_${v}[ \t]+[0-9]+$")
  if(ares_VERSION_LINE MATCHES "ARES_VERSION_${v} ([0-9]+)")
    set(c-ares_VERSION_${v} "${CMAKE_MATCH_1}")
  endif()
  unset(ares_VERSION_LINE)
endforeach()
set(c-ares_VERSION ${c-ares_VERSION_MAJOR}.${c-ares_VERSION_MINOR}.${c-ares_VERSION_PATCH})

# Spelled as generator expressions so that Seastar's seastar.pc.in, which
# interpolates both of these, names real paths rather than a CMake target.
set(c-ares_LIBRARIES "$<TARGET_FILE:c-ares::cares>")
set(c-ares_INCLUDE_DIRS "$<TARGET_PROPERTY:c-ares::cares,INTERFACE_INCLUDE_DIRECTORIES>")

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(c-ares
  REQUIRED_VARS
    c-ares_LIBRARIES
    c-ares_INCLUDE_DIRS
  VERSION_VAR c-ares_VERSION)
