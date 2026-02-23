set(CMAKE_CXX_FLAGS_SANITIZE
  ""
  CACHE
  INTERNAL
  "")
update_build_flags(Sanitize
  WITH_DEBUG_INFO
  DEBUG_INFO_LEVEL "1"
  OPTIMIZATION_LEVEL "2")

set(scylla_build_mode_Sanitize "sanitize")
set(Seastar_DEFINITIONS_SANITIZE
  SCYLLA_BUILD_MODE=${scylla_build_mode_Sanitize}
  DEBUG
  SANITIZE
  DEBUG_LSA_SANITIZER
  SCYLLA_ENABLE_ERROR_INJECTION
  SCYLLA_ENABLE_PREEMPTION_SOURCE)
foreach(definition ${Seastar_DEFINITIONS_SANITIZE})
  add_compile_definitions(
    $<$<CONFIG:Sanitize>:${definition}>)
endforeach()

maybe_limit_stack_usage_in_KB(50 Sanitize)
