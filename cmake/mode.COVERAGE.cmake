set(Seastar_OptimizationLevel_COVERAGE "g")
set(CMAKE_CXX_FLAGS_COVERAGE
  ""
  CACHE
  INTERNAL
  "")
string(APPEND CMAKE_CXX_FLAGS_COVERAGE
  " -O${Seastar_OptimizationLevel_SANITIZE}")

set(Seastar_DEFINITIONS_COVERAGE
  SCYLLA_BUILD_MODE=debug
  DEBUG
  SANITIZE
  DEBUG_LSA_SANITIZER
  SCYLLA_ENABLE_ERROR_INJECTION)

set(CMAKE_CXX_FLAGS_COVERAGE
  " -O${Seastar_OptimizationLevel_COVERAGE} -fprofile-instr-generate -fcoverage-mapping -g -gz")

set(CMAKE_STATIC_LINKER_FLAGS_COVERAGE
  "-fprofile-instr-generate -fcoverage-mapping")

set(stack_usage_threshold_in_KB 40)
