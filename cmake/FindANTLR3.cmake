#
# Copyright 2023-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
find_path(ANTLR3_INCLUDE_DIR
  NAMES antlr3.hpp
  PATHS
    /opt/scylladb/include)

mark_as_advanced(
  ANTLR3_INCLUDE_DIR)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(ANTLR3
  REQUIRED_VARS
    ANTLR3_INCLUDE_DIR)

if(ANTLR3_FOUND)
  set(ANTLR3_INCLUDE_DIRS ${ANTLR3_INCLUDE_DIR})
  if(NOT(TARGET ANTLR3::antlr3))
    add_library(ANTLR3::antlr3 INTERFACE IMPORTED)

    set_target_properties(ANTLR3::antlr3
      PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${ANTLR3_INCLUDE_DIRS})
  endif()
endif()
