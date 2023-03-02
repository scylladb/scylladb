function(add_perf_test name)
  cmake_parse_arguments(parsed_args
    ""
    ""
    "LIBRARIES"
    ${ARGN})
  set(src "${name}.cc")
  add_executable(${name} ${src})
  target_include_directories(${name}
    PRIVATE
      ${CMAKE_SOURCE_DIR})
  target_link_libraries(${name}
    PRIVATE
      perf-lib
      test-lib
      seastar_perf_testing
      Seastar::seastar
      xxHash::xxhash)
  if(parsed_args_LIBRARIES)
    target_link_libraries(${name}
      PRIVATE
        ${parsed_args_LIBRARIES})
  endif()
endfunction()

add_library(perf-lib OBJECT perf.cc)
target_include_directories(perf-lib
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(perf-lib
  PUBLIC
    Seastar::seastar
  PRIVATE
    xxHash::xxhash)

add_perf_test(perf_mutation_readers
  LIBRARIES
    auth
    cql3
    dht
    mutation
    readers
    replica
    schema
    service
    types
    utils)
add_perf_test(perf_checksum)
add_perf_test(perf_mutation_fragment)
add_perf_test(perf_idl
  LIBRARIES
    idl)
add_perf_test(perf_vint)
add_perf_test(perf_big_decimal
  LIBRARIES
    mutation
    schema)
