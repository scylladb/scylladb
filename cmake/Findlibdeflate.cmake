#
# Copyright 2022-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

find_package (PkgConfig REQUIRED)

pkg_check_modules (PC_deflate QUIET libdeflate)

find_library (deflate_LIBRARY
  NAMES deflate
  HINTS
    ${PC_deflate_LIBDIR}
    ${PC_deflate_LIBRARY_DIRS})

find_path (deflate_INCLUDE_DIR
  NAMES libdeflate.h
  HINTS
    ${PC_deflate_INCLUDEDIR}
    ${PC_deflate_INCLUDE_DIRS})

mark_as_advanced (
  deflate_LIBRARY
  deflate_INCLUDE_DIR)

include (FindPackageHandleStandardArgs)

find_package_handle_standard_args (libdeflate
  REQUIRED_VARS
    deflate_LIBRARY
    deflate_INCLUDE_DIR
  VERSION_VAR PC_deflate_VERSION)

if (libdeflate_FOUND)
  set (deflate_LIBRARIES ${deflate_LIBRARY})
  set (deflate_INCLUDE_DIRS ${deflate_INCLUDE_DIR})
  if (NOT (TARGET libdeflate::libdeflate))
    add_library (libdeflate::libdeflate UNKNOWN IMPORTED)

    set_target_properties (libdeflate::libdeflate
      PROPERTIES
        IMPORTED_LOCATION ${deflate_LIBRARY}
        INTERFACE_INCLUDE_DIRECTORIES ${deflate_INCLUDE_DIRS})
  endif ()
endif ()
