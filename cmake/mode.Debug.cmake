if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
  # -fasan -Og breaks some coroutines on aarch64, use -O0 instead
  set(OptimizationLevel "0")
else()
  set(OptimizationLevel "g")
endif()

update_cxx_flags(CMAKE_CXX_FLAGS_DEBUG
  WITH_DEBUG_INFO
  OPTIMIZATION_LEVEL ${OptimizationLevel})

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

maybe_limit_stack_usage_in_KB(40 Debug)
