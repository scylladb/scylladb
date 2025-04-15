set(OptimizationLevel "g")

update_cxx_flags(CMAKE_CXX_FLAGS_DEBUG
  WITH_DEBUG_INFO
  OPTIMIZATION_LEVEL ${OptimizationLevel})

set(scylla_build_mode_Debug "debug")
set(Seastar_DEFINITIONS_DEBUG
  SCYLLA_BUILD_MODE=${scylla_build_mode_Debug}
  DEBUG
  SANITIZE
  DEBUG_LSA_SANITIZER
  SCYLLA_ENABLE_ERROR_INJECTION)
foreach(definition ${Seastar_DEFINITIONS_DEBUG})
  add_compile_definitions(
    $<$<CONFIG:Debug>:${definition}>)
endforeach()

maybe_limit_stack_usage_in_KB(40 Debug)
