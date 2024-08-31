# it's important to *set* CMAKE_CXX_FLAGS_RELWITHDEBINFO here. otherwise,
# CMAKE_CXX_FLAGS_RELWITHDEBINFO would be initialized with
# CMAKE_CXX_FLAGS_RELWITHDEBUGINF_INIT, which is "-O2 -g -DNDEBUG",
# in CMake 3.27, but we need to enable "assert()" even with the release
# builds. so let's set this flagset instead of appending to it.
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
  "-ffunction-sections -fdata-sections"
  CACHE
  INTERNAL
  "")
update_cxx_flags(CMAKE_CXX_FLAGS_RELWITHDEBINFO
  WITH_DEBUG_INFO
  OPTIMIZATION_LEVEL "3")

set(scylla_build_mode_RelWithDebInfo "release")
add_compile_definitions(
    $<$<CONFIG:RelWithDebInfo>:SCYLLA_BUILD_MODE=${scylla_build_mode_RelWithDebInfo}>)

set(Scylla_CLANG_INLINE_THRESHOLD "2500" CACHE STRING
  "LLVM-specific inline threshold compilation parameter")
add_compile_options(
  "$<$<AND:$<CONFIG:RelWithDebInfo>,$<CXX_COMPILER_ID:GNU>>:--param;inline-unit-growth=300>"
  "$<$<AND:$<CONFIG:RelWithDebInfo>,$<CXX_COMPILER_ID:Clang>>:-mllvm;-inline-threshold=${Scylla_CLANG_INLINE_THRESHOLD}>")
# clang generates 16-byte loads that break store-to-load forwarding
# gcc also has some trouble: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=103554
check_cxx_compiler_flag("-fno-slp-vectorize" _slp_vectorize_supported)
if(_slp_vectorize_supported)
  add_compile_options(
    $<$<CONFIG:RelWithDebInfo>:-fno-slp-vectorize>)
endif()

add_link_options($<$<CONFIG:RelWithDebInfo>:LINKER:--gc-sections>)

maybe_limit_stack_usage_in_KB(13 RelWithDebInfo)
