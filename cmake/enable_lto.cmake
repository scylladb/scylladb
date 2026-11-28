#
# Copyright 2024-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Reuse the code generated for unchanged modules from earlier ThinLTO links.
# Only lld takes these options, and only RelWithDebInfo links use LTO.
set(_scylla_thinlto_cache_flags)
if(CMAKE_CONFIGURATION_TYPES)
  set(_scylla_configs ${CMAKE_CONFIGURATION_TYPES})
else()
  set(_scylla_configs ${CMAKE_BUILD_TYPE})
endif()
if(Scylla_THINLTO_CACHE_DIR AND Scylla_LINKER MATCHES "lld"
    AND "RelWithDebInfo" IN_LIST _scylla_configs)
  if(Scylla_THINLTO_CACHE_DIR MATCHES ",")
    # LINKER: would split the path at the comma.
    message(FATAL_ERROR "Scylla_THINLTO_CACHE_DIR must not contain a comma: ${Scylla_THINLTO_CACHE_DIR}")
  endif()
  # lld fails the link when it cannot write a cache entry, so check that a
  # file can be created there.
  set(_scylla_probe "${Scylla_THINLTO_CACHE_DIR}/.scylla-write-probe")
  execute_process(
    COMMAND ${CMAKE_COMMAND} -E make_directory "${Scylla_THINLTO_CACHE_DIR}"
    RESULT_VARIABLE _scylla_mkdir_result
    ERROR_QUIET)
  execute_process(
    COMMAND ${CMAKE_COMMAND} -E touch "${_scylla_probe}"
    RESULT_VARIABLE _scylla_probe_result
    ERROR_QUIET)
  file(REMOVE "${_scylla_probe}")
  if(_scylla_mkdir_result EQUAL 0 AND _scylla_probe_result EQUAL 0)
    set(_scylla_thinlto_cache_flags
      "$<$<CONFIG:RelWithDebInfo>:LINKER:--thinlto-cache-dir=${Scylla_THINLTO_CACHE_DIR}>"
      "$<$<CONFIG:RelWithDebInfo>:LINKER:--thinlto-cache-policy=${Scylla_THINLTO_CACHE_POLICY}>")
  else()
    message(WARNING "Cannot create or write the ThinLTO cache directory ${Scylla_THINLTO_CACHE_DIR}, linking without it")
  endif()
endif()

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
      if(_slp_vectorize_supported)
        target_link_options(${name}
          PRIVATE $<$<CONFIG:RelWithDebInfo>:LINKER:-mllvm,-vectorize-slp=false>)
      endif()
      if(_scylla_thinlto_cache_flags)
        target_link_options(${name} PRIVATE ${_scylla_thinlto_cache_flags})
      endif()
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
