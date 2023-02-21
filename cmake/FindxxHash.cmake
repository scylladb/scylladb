#
# Copyright 2023-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
find_package(PkgConfig REQUIRED)

pkg_check_modules(PC_xxhash QUIET libxxhash)

find_library(xxhash_LIBRARY
  NAMES xxhash
  HINTS
    ${PC_xxhash_LIBDIR}
    ${PC_xxhash_LIBRARY_DIRS})

find_path(xxhash_INCLUDE_DIR
  NAMES xxhash.h
  HINTS
    ${PC_xxhash_INCLUDEDIR}
    ${PC_xxhash_INCLUDE_DIRS})

mark_as_advanced(
  xxhash_LIBRARY
  xxhash_INCLUDE_DIR)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(xxHash
  REQUIRED_VARS
    xxhash_LIBRARY
    xxhash_INCLUDE_DIR
  VERSION_VAR PC_xxhash_VERSION)

if(xxHash_FOUND)
  set(xxhash_LIBRARIES ${xxhash_LIBRARY})
  set(xxhash_INCLUDE_DIRS ${xxhash_INCLUDE_DIR})
  if(NOT(TARGET xxHash::xxhash))
    add_library(xxHash::xxhash UNKNOWN IMPORTED)

    set_target_properties(xxHash::xxhash
      PROPERTIES
        IMPORTED_LOCATION ${xxhash_LIBRARY}
        INTERFACE_INCLUDE_DIRECTORIES ${xxhash_INCLUDE_DIRS}
        # Hacks needed to expose internal APIs for xxhash dependencies
        INTERFACE_COMPILE_DEFINITIONS XXH_PRIVATE_API)
  endif()
endif()
