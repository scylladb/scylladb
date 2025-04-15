set(CMAKE_CXX_FLAGS_COVERAGE
  "-fprofile-instr-generate -fcoverage-mapping -fprofile-list=${CMAKE_SOURCE_DIR}/coverage_sources.list"
  CACHE
  INTERNAL
  "")
update_cxx_flags(CMAKE_CXX_FLAGS_COVERAGE
  WITH_DEBUG_INFO
  OPTIMIZATION_LEVEL "g")

set(scylla_build_mode_Coverage "coverage")
set(Seastar_DEFINITIONS_COVERAGE
  SCYLLA_BUILD_MODE=${scylla_build_mode_Coverage}
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
