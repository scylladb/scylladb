#
# Copyright 2024-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

set(kmip_ver "2.1.0t")

cmake_host_system_information(
  RESULT distrib_id QUERY DISTRIB_ID)
if(distrib_id MATCHES "centos|fedora|rhel")
  set(kmip_distrib "rhel84")
else()
  message(FATAL_ERROR "Could not locate kmipc library for ${distrib_id}")
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64")
  set(kmip_arch "aarch64")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "amd64|x86_64")
  set(kmip_arch "64")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "(powerpc|ppc)64le")
  set(kmip_arch "ppc64le")
endif()

set(kmip_ROOT "${PROJECT_SOURCE_DIR}/kmipc/kmipc-${kmip_ver}-${kmip_distrib}_${kmip_arch}")
find_library(kmip_LIBRARY
  NAMES kmip
  HINTS ${kmip_ROOT}/lib)

find_path(kmip_INCLUDE_DIR
  NAMES kmip.h
  HINTS ${kmip_ROOT}/include)

mark_as_advanced(
  kmip_LIBRARY
  kmip_INCLUDE_DIR)

find_package_handle_standard_args(kmip
  REQUIRED_VARS
    kmip_LIBRARY
    kmip_INCLUDE_DIR)

if(kmip_FOUND)
  if (NOT TARGET KMIP::kmipc)
    add_library(KMIP::kmipc UNKNOWN IMPORTED)
    set_target_properties(KMIP::kmipc PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${kmip_INCLUDE_DIR}"
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      IMPORTED_LOCATION "${kmip_LIBRARY}")
  endif()
endif()
