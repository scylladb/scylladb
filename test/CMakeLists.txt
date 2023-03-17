find_package(jsoncpp REQUIRED)

add_subdirectory(lib)

add_library(test-perf STATIC)
target_sources(test-perf
  PRIVATE
    perf/perf_fast_forward.cc
    perf/perf_row_cache_update.cc
    perf/perf_simple_query.cc
    perf/perf_sstable.cc
    perf/perf.cc)
target_include_directories(test-perf
  PUBLIC
    ${CMAKE_SOURCE_DIR})
if(NOT TARGET Seastar::seastar_perf_testing)
    add_library(Seastar::seastar_perf_testing)
    target_sources(
        tests/perf/linux_perf_event.cc)
endif()
target_link_libraries(test-perf
  PRIVATE
    idl
    test-lib
    Seastar::seastar_perf_testing
    JsonCpp::JsonCpp
    xxHash::xxhash
    wasmtime_bindings)

add_subdirectory(perf)

#
# Add a scylla unit test
#
# add_scylla_test(<test_name>
#   [KIND <kind>]
#   [SOUCES <src>...])
#
# kind can be:
# * SEASTAR - a unit test that depends on the scylla sources and the
#   seastar test framework.
# * BOOST - a unit test that only depends on the listed sources and the
#   Boost test framework
# * UNIT - a test driven its own main(). and it depends on the
#   seastar test framework.
# test_name should map to a source file, such that ${test_name}.cc
# is a valid source file. If this isn't the case, please use the SOURCE
# param.
#
function(add_scylla_test name)
  cmake_parse_arguments(parsed_args
    ""
    "KIND"
    "LIBRARIES;SOURCES"
    ${ARGN})
  if(parsed_args_KIND)
    set(kind ${parsed_args_KIND})
  else()
    set(kind "SEASTAR")
  endif()
  if(parsed_args_SOURCES)
    set(src "${parsed_args_SOURCES}")
  else()
    set(src "${name}.cc")
  endif()
  add_executable(${name} ${src})
  target_include_directories(${name}
    PRIVATE
      ${CMAKE_SOURCE_DIR})
  target_link_libraries(${name}
    PRIVATE
      test-lib
      Seastar::seastar
      xxHash::xxhash)
  if(kind STREQUAL "SEASTAR")
    target_link_libraries(${name}
      PRIVATE
        Seastar::seastar_testing)
    target_compile_definitions(${name}
      PRIVATE
        SEASTAR_TESTING_MAIN)
  elseif(kind STREQUAL "BOOST")
    target_link_libraries(${name}
      PRIVATE
        Boost::unit_test_framework
        Boost::dynamic_linking)
  elseif(kind STREQUAL "UNIT")
    target_link_libraries(${name}
      PRIVATE
        Seastar::seastar_testing)
  else()
    message(FATAL_ERROR "unknown test KIND: ${kind}")
  endif()
  if(parsed_args_LIBRARIES)
    target_link_libraries(${name}
      PRIVATE
        ${parsed_args_LIBRARIES})
  endif()
endfunction()

if(BUILD_TESTING)
    add_subdirectory(boost)
    add_subdirectory(manual)
    add_subdirectory(unit)
endif()
