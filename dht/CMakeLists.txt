include(add_whole_archive)

add_library(scylla_dht STATIC)
target_sources(scylla_dht
  PRIVATE
    boot_strapper.cc
    i_partitioner.cc
    murmur3_partitioner.cc
    range_streamer.cc
    token.cc)
target_include_directories(scylla_dht
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(scylla_dht
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    replica)

if (Scylla_USE_PRECOMPILED_HEADER_USE)
  target_precompile_headers(scylla_dht REUSE_FROM scylla-precompiled-header)
endif()
add_whole_archive(dht scylla_dht)

check_headers(check-headers scylla_dht
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
