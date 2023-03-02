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

if(BUILD_TESTING)
    add_subdirectory(boost)
endif()
