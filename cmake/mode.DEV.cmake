set(Seastar_OptimizationLevel_DEV "2")
set(CMAKE_CXX_FLAGS_DEV
  ""
  CACHE
  INTERNAL
  "")
string(APPEND CMAKE_CXX_FLAGS_DEV
  " -O${Seastar_OptimizationLevel_DEV}")

set(Seastar_DEFINITIONS_DEV
  SCYLLA_BUILD_MODE=devel
  DEVEL
  SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
  SCYLLA_ENABLE_ERROR_INJECTION)

set(stack_usage_threshold_in_KB 21)
