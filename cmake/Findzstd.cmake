#
# Copyright 2024-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

find_package (PkgConfig REQUIRED)

pkg_search_module (PC_zstd QUIET libzstd)

find_library (zstd_STATIC_LIBRARY
  NAMES libzstd.a
  HINTS
    ${PC_zstd_STATIC_LIBDIR}
    ${PC_zstd_STATIC_LIBRARY_DIRS})

find_library (zstd_LIBRARY
  NAMES zstd
  HINTS
    ${PC_zstd_LIBDIR}
    ${PC_zstd_LIBRARY_DIRS})

find_path (zstd_INCLUDE_DIR
  NAMES zstd.h
  HINTS
    ${PC_zstd_STATIC_INCLUDEDIR}
    ${PC_zstd_STATIC_INCLUDE_DIRS})

mark_as_advanced (
  zstd_STATIC_LIBRARY
  zstd_LIBRARY
  zstd_INCLUDE_DIR)

include (FindPackageHandleStandardArgs)

find_package_handle_standard_args (zstd
  REQUIRED_VARS
    zstd_STATIC_LIBRARY
    zstd_LIBRARY
    zstd_INCLUDE_DIR
  VERSION_VAR PC_zstd_STATIC_VERSION)

if (zstd_FOUND)
  if (NOT (TARGET zstd::zstd_static))
    add_library (zstd::zstd_static UNKNOWN IMPORTED)

    set_target_properties (zstd::zstd_static
      PROPERTIES
        IMPORTED_LOCATION ${zstd_STATIC_LIBRARY}
        INTERFACE_INCLUDE_DIRECTORIES ${zstd_INCLUDE_DIR})
  endif ()
  if (NOT (TARGET zstd::libzstd))
    add_library (zstd::libzstd UNKNOWN IMPORTED)

    set_target_properties (zstd::libzstd
      PROPERTIES
        IMPORTED_LOCATION ${zstd_LIBRARY}
        INTERFACE_INCLUDE_DIRECTORIES ${zstd_INCLUDE_DIR})
  endif ()
endif ()
