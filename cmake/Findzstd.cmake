#
# Copyright 2024-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

find_package (PkgConfig REQUIRED)

pkg_check_modules (PC_zstd QUIET libzstd)

find_library (zstd_LIBRARY
  NAMES zstd
  HINTS
    ${PC_zstd_LIBDIR}
    ${PC_zstd_LIBRARY_DIRS})

find_path (zstd_INCLUDE_DIR
  NAMES zstd.h
  HINTS
    ${PC_zstd_INCLUDEDIR}
    ${PC_zstd_INCLUDE_DIRS})

mark_as_advanced (
  zstd_LIBRARY
  zstd_INCLUDE_DIR)

include (FindPackageHandleStandardArgs)

find_package_handle_standard_args (zstd
  REQUIRED_VARS
    zstd_LIBRARY
    zstd_INCLUDE_DIR
  VERSION_VAR PC_zstd_VERSION)

if (zstd_FOUND)
  set (zstd_LIBRARIES ${zstd_LIBRARY})
  set (zstd_INCLUDE_DIRS ${zstd_INCLUDE_DIR})
  if (NOT (TARGET zstd::libzstd))
    add_library (zstd::libzstd UNKNOWN IMPORTED)

    set_target_properties (zstd::libzstd
      PROPERTIES
        IMPORTED_LOCATION ${zstd_LIBRARY}
        INTERFACE_INCLUDE_DIRECTORIES ${zstd_INCLUDE_DIR})
  endif ()
endif ()
