if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
  # -fasan -Og breaks some coroutines on aarch64, use -O0 instead
  set(default_Seastar_OptimizationLevel_DEBUG "0")
else()
  set(default_Seastar_OptimizationLevel_DEBUG "g")
endif()
set(Seastar_OptimizationLevel_DEBUG
  ${default_Seastar_OptimizationLevel_DEBUG}
  CACHE
  INTERNAL
  "")

set(Seastar_DEFINITIONS_DEBUG
  SCYLLA_BUILD_MODE=debug
  DEBUG
  SANITIZE
  DEBUG_LSA_SANITIZER
  SCYLLA_ENABLE_ERROR_INJECTION)
foreach(definition ${Seastar_DEFINITIONS_DEBUG})
  add_compile_definitions(
    $<$<CONFIG:Debug>:${definition}>)
endforeach()

update_cxx_flags(CMAKE_CXX_FLAGS_DEBUG
  WITH_DEBUG_INFO
  OPTIMIZATION_LEVEL ${Seastar_OptimizationLevel_DEBUG})

maybe_limit_stack_usage_in_KB(40 Debug)
