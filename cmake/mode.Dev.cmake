set(CMAKE_CXX_FLAGS_DEV
  ""
  CACHE
  INTERNAL
  "")
update_build_flags(Dev
  OPTIMIZATION_LEVEL "2")

set(scylla_build_mode_Dev "dev")
set(Seastar_DEFINITIONS_DEV
  SCYLLA_BUILD_MODE=${scylla_build_mode_Dev}
  DEVEL
  SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
  SCYLLA_ENABLE_ERROR_INJECTION
  SCYLLA_ENABLE_PREEMPTION_SOURCE)
foreach(definition ${Seastar_DEFINITIONS_DEV})
  add_compile_definitions(
    $<$<CONFIG:Dev>:${definition}>)
endforeach()

maybe_limit_stack_usage_in_KB(21 Dev)

# Trims backend compile time; Dev doesn't need release's vectorization.
check_cxx_compiler_flag("-fno-vectorize" _vectorize_supported)
if(_vectorize_supported)
  add_compile_options($<$<CONFIG:Dev>:-fno-vectorize>)
endif()
check_cxx_compiler_flag("-fno-slp-vectorize" _slp_vectorize_supported)
if(_slp_vectorize_supported)
  add_compile_options($<$<CONFIG:Dev>:-fno-slp-vectorize>)
endif()
