#
# Copyright 2023-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
find_path(rapidxml_INCLUDE_DIR
  NAMES rapidxml.h rapidxml/rapidxml.hpp)

mark_as_advanced(
  rapidxml_INCLUDE_DIR)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(rapidxml
  REQUIRED_VARS
    rapidxml_INCLUDE_DIR)

if(rapidxml_FOUND)
  if(NOT TARGET rapidxml::rapidxml)
    add_library(rapidxml::rapidxml INTERFACE IMPORTED)
    set_target_properties(rapidxml::rapidxml
      PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${rapidxml_INCLUDE_DIR})
  endif()
endif()
