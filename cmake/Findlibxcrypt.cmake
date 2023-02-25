#
# Copyright 2023-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
find_package(PkgConfig REQUIRED)

pkg_check_modules(PC_xcrypt QUIET libxcrypt)

find_library(xcrypt_LIBRARY
  NAMES crypt
  HINTS
    ${PC_xcrypt_LIBDIR}
    ${PC_xcrypt_LIBRARY_DIRS})

find_path(xcrypt_INCLUDE_DIR
  NAMES crypt.h
  HINTS
    ${PC_xcrypt_INCLUDEDIR}
    ${PC_xcrypt_INCLUDE_DIRS})

mark_as_advanced(
  xcrypt_LIBRARY
  xcrypt_INCLUDE_DIR)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(libxcrypt
  REQUIRED_VARS
    xcrypt_LIBRARY
    xcrypt_INCLUDE_DIR
  VERSION_VAR PC_xcrypt_VERSION)

if(libxcrypt_FOUND)
  set(xcrypt_LIBRARIES ${xcrypt_LIBRARY})
  set(xcrypt_INCLUDE_DIRS ${xcrypt_INCLUDE_DIR})
  if(NOT(TARGET libxcrypt::libxcrypt))
    add_library(libxcrypt::libxcrypt UNKNOWN IMPORTED)

    set_target_properties(libxcrypt::libxcrypt
      PROPERTIES
        IMPORTED_LOCATION ${xcrypt_LIBRARY}
        INTERFACE_INCLUDE_DIRECTORIES ${xcrypt_INCLUDE_DIRS})
  endif()
endif()
