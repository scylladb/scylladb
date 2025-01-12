#
# Copyright 2023-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
find_package(PkgConfig REQUIRED)

pkg_check_modules(PC_p11_kit QUIET p11-kit-1)

find_library(p11-kit_LIBRARY
  NAMES p11-kit
  PATH_SUFFIXES p11-kit-1
  HINTS
    ${PC_p11_kit_LIBDIR}
    ${PC_p11_kit_LIBRARY_DIRS})

find_path(p11-kit_INCLUDE_DIR
  NAMES p11-kit/p11-kit.h
  HINTS
    ${PC_p11_kit_INCLUDEDIR}
    ${PC_p11_kit_INCLUDE_DIRS})

mark_as_advanced(
  p11-kit_LIBRARY
  p11-kit_INCLUDE_DIR)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(p11-kit
  REQUIRED_VARS
    p11-kit_LIBRARY
    p11-kit_INCLUDE_DIR
  VERSION_VAR PC_p11_kit_VERSION)

if(p11-kit_FOUND)
  set(p11-kit_LIBRARIES ${p11-kit_LIBRARY})
  set(p11-kit_INCLUDE_DIRS ${p11-kit_INCLUDE_DIR})
  if(NOT(TARGET p11-kit::p11-kit))
    add_library(p11-kit::p11-kit UNKNOWN IMPORTED)

    set_target_properties(p11-kit::p11-kit
      PROPERTIES
        IMPORTED_LOCATION ${p11-kit_LIBRARY}
        INTERFACE_INCLUDE_DIRECTORIES ${p11-kit_INCLUDE_DIRS})
  endif()
endif()
