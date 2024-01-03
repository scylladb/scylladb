#
# Copyright (C) 2018-present ScyllaDB
#
# SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
#

find_library (cryptopp_LIBRARY
  NAMES cryptopp)

find_path (cryptopp_INCLUDE_DIR
  NAMES cryptopp/aes.h
  PATH_SUFFIXES cryptopp)

mark_as_advanced (
  cryptopp_LIBRARY
  cryptopp_INCLUDE_DIR)

include (FindPackageHandleStandardArgs)

find_package_handle_standard_args (cryptopp
  REQUIRED_VARS
    cryptopp_LIBRARY
    cryptopp_INCLUDE_DIR)

set (cryptopp_LIBRARIES ${cryptopp_LIBRARY})
set (cryptopp_INCLUDE_DIRS ${cryptopp_INCLUDE_DIR})

if (cryptopp_FOUND AND NOT (TARGET cryptopp::cryptopp))
  add_library (cryptopp::cryptopp UNKNOWN IMPORTED)

  set_target_properties (cryptopp::cryptopp
    PROPERTIES
      IMPORTED_LOCATION ${cryptopp_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${cryptopp_INCLUDE_DIRS})
endif ()
