set(CMAKE_CXX_FLAGS_SANITIZE
  ""
  CACHE
  INTERNAL
  "")
update_cxx_flags(CMAKE_CXX_FLAGS_SANITIZE
  WITH_DEBUG_INFO
  OPTIMIZATION_LEVEL "s")

set(scylla_build_mode_Sanitize "sanitize")
set(Seastar_DEFINITIONS_SANITIZE
  SCYLLA_BUILD_MODE=${scylla_build_mode_Sanitize}
  DEBUG
  SANITIZE
  DEBUG_LSA_SANITIZER
  SCYLLA_ENABLE_ERROR_INJECTION)
foreach(definition ${Seastar_DEFINITIONS_SANITIZE})
  add_compile_definitions(
    $<$<CONFIG:Sanitize>:${definition}>)
endforeach()

maybe_limit_stack_usage_in_KB(50 Sanitize)
