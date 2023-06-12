set(default_Seastar_OptimizationLevel_DEBUG "0")
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

set(CMAKE_CXX_FLAGS_DEBUG
  " -O${Seastar_OptimizationLevel_DEBUG} -g -gz")

set(stack_usage_threshold_in_KB 40)
