#
# Copyright 2023-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
find_package(PkgConfig REQUIRED)

pkg_check_modules(PC_thrift QUIET thrift)

find_library(thrift_LIBRARY
  NAMES thrift
  HINTS
    ${PC_thrift_LIBDIR}
    ${PC_thrift_LIBRARY_DIRS})

find_path(thrift_INCLUDE_DIR
  NAMES thrift/Thrift.h
  HINTS
    ${PC_thrift_INCLUDEDIR}
    ${PC_thrift_INCLUDE_DIRS})

mark_as_advanced(
  thrift_LIBRARY
  thrift_INCLUDE_DIR)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(Thrift
  REQUIRED_VARS
    thrift_LIBRARY
    thrift_INCLUDE_DIR
  VERSION_VAR PC_thrift_VERSION)

if(Thrift_FOUND)
  set(thrift_LIBRARIES ${thrift_LIBRARY})
  set(thrift_INCLUDE_DIRS ${thrift_INCLUDE_DIR})
  if(NOT(TARGET Thrift::thrift))
    add_library(Thrift::thrift UNKNOWN IMPORTED)

    set_target_properties(Thrift::thrift
      PROPERTIES
        IMPORTED_LOCATION ${thrift_LIBRARY}
        INTERFACE_INCLUDE_DIRECTORIES ${thrift_INCLUDE_DIRS})
  endif()
endif()
