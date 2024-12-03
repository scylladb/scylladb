#
# Copyright 2024-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
find_package(PkgConfig REQUIRED)

foreach(component ${OpenLDAP_FIND_COMPONENTS})
  pkg_search_module(PC_${component} QUIET ${component})
  find_path (OpenLDAP_${component}_INCLUDE_DIR
    NAMES lber.h
    HINTS
      ${PC_${component}_INCLUDEDIR}
      ${PC_${component}_INCLUDE_DIRS})
  find_library(OpenLDAP_${component}_LIBRARY
    NAMES ${component}
    HINTS
      ${PC_${component}_LIBDIR}
      ${PC_${component}_LIBRARY_DIRS})
  list(APPEND OpenLDAP_INCLUDE_DIRS OpenLDAP_${component}_INCLUDE_DIR)
  list(APPEND OpenLDAP_LIBRARIES OpenLDAP_${component}_LIBRARY)
endforeach()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(OpenLDAP
  DEFAULT_MSG
  ${OpenLDAP_INCLUDE_DIRS}
  ${OpenLDAP_LIBRARIES})

mark_as_advanced(
  ${OpenLDAP_INCLUDE_DIRS}
  ${OpenLDAP_LIBRARIES})

if(OpenLDAP_FOUND)
  foreach(component ${OpenLDAP_FIND_COMPONENTS})
    if(NOT TARGET OpenLDAP::${component})
      add_library(OpenLDAP::${component} UNKNOWN IMPORTED)
      set_target_properties(OpenLDAP::${component} PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${OpenLDAP_${component}_INCLUDE_DIR}"
        IMPORTED_LINK_INTERFACE_LANGUAGES "C"
        IMPORTED_LOCATION "${OpenLDAP_${component}_LIBRARY}")
    endif()
  endforeach()
endif()
