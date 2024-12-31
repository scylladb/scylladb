#
# Copyright 2024-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

find_package (PkgConfig REQUIRED)

pkg_search_module (PC_lz4 QUIET liblz4)

find_library (lz4_STATIC_LIBRARY
  NAMES liblz4.a
  HINTS
    ${PC_lz4_STATIC_LIBDIR}
    ${PC_lz4_STATIC_LIBRARY_DIRS})

find_library (lz4_LIBRARY
  NAMES lz4
  HINTS
    ${PC_lz4_LIBDIR}
    ${PC_lz4_LIBRARY_DIRS})

find_path (lz4_INCLUDE_DIR
  NAMES lz4.h
  HINTS
    ${PC_lz4_STATIC_INCLUDEDIR}
    ${PC_lz4_STATIC_INCLUDE_DIRS})

mark_as_advanced (
  lz4_STATIC_LIBRARY
  lz4_LIBRARY
  lz4_INCLUDE_DIR)

include (FindPackageHandleStandardArgs)

find_package_handle_standard_args (lz4
  REQUIRED_VARS
    lz4_STATIC_LIBRARY
    lz4_LIBRARY
    lz4_INCLUDE_DIR
    VERSION_VAR PC_lz4_STATIC_VERSION)

if (lz4_FOUND)
  if (NOT (TARGET lz4::lz4_static))
    add_library (lz4::lz4_static UNKNOWN IMPORTED)
    set_target_properties (lz4::lz4_static
      PROPERTIES
        IMPORTED_LOCATION ${lz4_STATIC_LIBRARY}
        INTERFACE_INCLUDE_DIRECTORIES ${lz4_INCLUDE_DIR})
  endif ()
  if (NOT (TARGET lz4::lz4))
    add_library (lz4::lz4 UNKNOWN IMPORTED)
    set_target_properties (lz4::lz4
      PROPERTIES
        IMPORTED_LOCATION ${lz4_LIBRARY}
        INTERFACE_INCLUDE_DIRECTORIES ${lz4_INCLUDE_DIR})
  endif ()
endif ()
