set(Seastar_OptimizationLevel_SANITIZE "s")
set(CMAKE_CXX_FLAGS_SANITIZE
  ""
  CACHE
  INTERNAL
  "")
string(APPEND CMAKE_CXX_FLAGS_SANITIZE
  " -O${Seastar_OptimizationLevel_SANITIZE}")

set(Seastar_DEFINITIONS_SANITIZE
  SCYLLA_BUILD_MODE=sanitize
  DEBUG
  SANITIZE
  DEBUG_LSA_SANITIZER
  SCYLLA_ENABLE_ERROR_INJECTION)

set(stack_usage_threshold_in_KB 50)
