set(CMAKE_CXX_FLAGS_COVERAGE
  "-fprofile-instr-generate -fcoverage-mapping"
  CACHE
  INTERNAL
  "")
update_cxx_flags(CMAKE_CXX_FLAGS_COVERAGE
  WITH_DEBUG_INFO
  OPTIMIZATION_LEVEL "g")

set(Seastar_DEFINITIONS_COVERAGE
  SCYLLA_BUILD_MODE=coverage
  DEBUG
  SANITIZE
  DEBUG_LSA_SANITIZER
  SCYLLA_ENABLE_ERROR_INJECTION)
foreach(definition ${Seastar_DEFINITIONS_COVERAGE})
  add_compile_definitions(
    $<$<CONFIG:Coverage>:${definition}>)
endforeach()

set(CMAKE_STATIC_LINKER_FLAGS_COVERAGE
  "-fprofile-instr-generate -fcoverage-mapping")

maybe_limit_stack_usage_in_KB(40 Coverage)
