#
# Copyright 2024-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
function(enable_lto name)
  get_target_property(type ${name} TYPE)
  if(type MATCHES "OBJECT_LIBRARY|STATIC_LIBRARY|SHARED_LIBRARY|EXECUTABLE")
    target_compile_options(${name} PRIVATE
      $<$<CONFIG:RelWithDebInfo>:-ffat-lto-objects>)
    set_property(TARGET ${name} PROPERTY
      INTERPROCEDURAL_OPTIMIZATION_RELWITHDEBINFO ON)
    if(type MATCHES "SHARED_LIBRARY|EXECUTABLE")
      target_link_options(${name}
        PRIVATE $<$<CONFIG:RelWithDebInfo>:-ffat-lto-objects>)
    endif()
  elseif(type STREQUAL "INTERFACE_LIBRARY")
    if (name MATCHES "^scylla_(.*)$")
      # Special handling for scylla_* libraries with whole archive linking
      set(library "${CMAKE_MATCH_1}")
      enable_lto(${library})
      # For non-scylla_* INTERFACE libraries, we don't compile them,
      # hence no need to set the LTO compile options or property
    endif()
  else()
    message(FATAL_ERROR "Unsupported TYPE: ${name}:${type}")
  endif()
endfunction()
