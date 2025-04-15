#
# Copyright 2024-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

find_package(PkgConfig QUIET REQUIRED)

set(saved_pkg_config_path "$ENV{PKG_CONFIG_PATH}")
foreach(config ${CMAKE_CONFIGURATION_TYPES})
  # this path should be consistent with the path used by configure.py, which configures
  # seastar's building system for each enabled build mode / configuration type
  set(binary_dir "${CMAKE_BINARY_DIR}/${config}/seastar")
  set(ENV{PKG_CONFIG_PATH} "${saved_pkg_config_path}:${CMAKE_BINARY_DIR}/${config}/seastar")

  pkg_search_module(seastar-${config}
    REQUIRED QUIET
    NO_CMAKE_PATH
    IMPORTED_TARGET GLOBAL
    seastar)
  find_path(seastar_INCLUDE_DIR
    NAMES seastar/core/seastar.hh
    HINTS
      ${seastar-${config}_INCLUDE_DIRS})
  pkg_search_module(seastar_testing-${config}
    REQUIRED QUIET
    NO_CMAKE_PATH
    IMPORTED_TARGET GLOBAL
    seastar-testing)
  find_path(seastar_testing_INCLUDE_DIR
    NAMES seastar/testing/seastar_test.hh
    HINTS
      ${seastar_testing-${config}_INCLUDE_DIRS})

  mark_as_advanced(
    seastar_INCLUDE_DIR
    seastar_testing_INCLUDE_DIR)
endforeach(config ${CMAKE_CONFIGURATION_TYPES})
set(ENV{PKG_CONFIG_PATH} "${saved_pkg_config_path}")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Seastar
  REQUIRED_VARS
    seastar_INCLUDE_DIR
    seastar_testing_INCLUDE_DIR)

foreach(config ${CMAKE_CONFIGURATION_TYPES})
  # this directory is created when building Seastar, but we create it manually.
  # otherwise CMake would complain when configuring targets which link against the
  # "Seastar::seastar" library. it is imported library. and CMake assures that
  # all INTERFACE_INCLUDE_DIRECTORIES of an imported library should exist.
  file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/${config}/seastar/gen/include)
endforeach()

foreach(component "seastar" "seastar_testing" "seastar_perf_testing")
  if(NOT TARGET Seastar::${component})
    add_library(Seastar::${component} UNKNOWN IMPORTED)
    foreach(config ${CMAKE_CONFIGURATION_TYPES})
      set_property(TARGET Seastar::${component} APPEND
        PROPERTY
          IMPORTED_CONFIGURATIONS ${config})
      string (TOUPPER ${config} CONFIG)
      if(config MATCHES "Debug|Dev")
        set_property(TARGET Seastar::${component}
          PROPERTY
            IMPORTED_LOCATION_${CONFIG} "${CMAKE_BINARY_DIR}/${config}/seastar/lib${component}.so")
        set(prefix ${component}-${config})
      else()
        set_property(TARGET Seastar::${component}
          PROPERTY
            IMPORTED_LOCATION_${CONFIG} "${CMAKE_BINARY_DIR}/${config}/seastar/lib${component}.a")
        set(prefix ${component}-${config}_STATIC)
      endif()
      # these two libraries provide .pc, so set the properties retrieved with
      # pkg-config.
      if(component MATCHES "^(seastar|seastar_testing)$")
        set_property(TARGET Seastar::${component} APPEND
          PROPERTY
            INTERFACE_INCLUDE_DIRECTORIES $<$<CONFIG:${config}>:${${prefix}_INCLUDE_DIRS}>)
        set_property(TARGET Seastar::${component} APPEND
          PROPERTY
            INTERFACE_LINK_LIBRARIES $<$<CONFIG:${config}>:${${prefix}_LINK_LIBRARIES}>)
        set_property(TARGET Seastar::${component} APPEND
          PROPERTY
            INTERFACE_LINK_OPTIONS $<$<CONFIG:${config}>:${${prefix}_LDFLAGS}>)
        set_property(TARGET Seastar::${component} APPEND
          PROPERTY
            INTERFACE_COMPILE_OPTIONS $<$<CONFIG:${config}>:${${prefix}_CFLAGS_OTHER}>)
      endif()
    endforeach()

    # seastar_perf_testing does not provide its .pc, so let's just hardwire its
    # properties
    if(component STREQUAL "seastar_perf_testing")
      set_target_properties(Seastar::seastar_perf_testing PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${Seastar_INCLUDE_DIR}"
        INTERFACE_LINK_LIBRARIES Seastar::seastar)
    endif()
  endif()
endforeach()

if(NOT TARGET Seastar::iotune)
  add_executable(Seastar::iotune IMPORTED)
  foreach(config ${CMAKE_CONFIGURATION_TYPES})
    set_property(TARGET Seastar::iotune APPEND
      PROPERTY
        IMPORTED_CONFIGURATIONS ${config})
    string (TOUPPER ${config} CONFIG)
    set_property(TARGET Seastar::iotune
      PROPERTY
        IMPORTED_LOCATION_${CONFIG} ${CMAKE_BINARY_DIR}/${config}/seastar/apps/iotune/iotune)
  endforeach()
endif()
