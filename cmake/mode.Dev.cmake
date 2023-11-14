set(Seastar_OptimizationLevel_DEV "2")
set(CMAKE_CXX_FLAGS_DEV
  ""
  CACHE
  INTERNAL
  "")
update_cxx_flags(CMAKE_CXX_FLAGS_RELEASE
  OPTIMIZATION_LEVEL ${Seastar_OptimizationLevel_Dev})

set(Seastar_DEFINITIONS_DEV
  SCYLLA_BUILD_MODE=devel
  DEVEL
  SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
  SCYLLA_ENABLE_ERROR_INJECTION)
foreach(definition ${Seastar_DEFINITIONS_DEV})
  add_compile_definitions(
    $<$<CONFIG:Dev>:${definition}>)
endforeach()

maybe_limit_stack_usage_in_KB(21 Dev)
