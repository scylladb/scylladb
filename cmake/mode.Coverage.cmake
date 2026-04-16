set(CMAKE_CXX_FLAGS_COVERAGE
  "-fprofile-instr-generate -fcoverage-mapping"
  CACHE
  INTERNAL
  "")
update_build_flags(Coverage
  WITH_DEBUG_INFO
  OPTIMIZATION_LEVEL "g")

set(scylla_build_mode_Coverage "coverage")

# Coverage mode sets cmake_build_type='Debug' for Seastar
# (configure.py:515), so Seastar's pkg-config --cflags output
# (configure.py:2252-2267, queried at configure.py:3039) includes debug
# defines, sanitizer compile flags, and -fstack-clash-protection.
# Seastar's CMake generator expressions only activate these for
# Debug/Sanitize configs, so we add them explicitly for Coverage.
set(Seastar_DEFINITIONS_COVERAGE
  SCYLLA_BUILD_MODE=${scylla_build_mode_Coverage}
  SEASTAR_DEBUG
  SEASTAR_DEFAULT_ALLOCATOR
  SEASTAR_SHUFFLE_TASK_QUEUE
  SEASTAR_DEBUG_SHARED_PTR
  SEASTAR_DEBUG_PROMISE
  SEASTAR_TYPE_ERASE_MORE)
foreach(definition ${Seastar_DEFINITIONS_COVERAGE})
  add_compile_definitions(
    $<$<CONFIG:Coverage>:${definition}>)
endforeach()

add_compile_options(
  $<$<CONFIG:Coverage>:-fsanitize=address>
  $<$<CONFIG:Coverage>:-fsanitize=undefined>
  $<$<CONFIG:Coverage>:-fsanitize=vptr>
  $<$<CONFIG:Coverage>:-fstack-clash-protection>)

set(CMAKE_EXE_LINKER_FLAGS_COVERAGE
  "-fprofile-instr-generate -fcoverage-mapping")

maybe_limit_stack_usage_in_KB(40 Coverage)
