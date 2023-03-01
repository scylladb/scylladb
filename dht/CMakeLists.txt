add_library(dht STATIC)
target_sources(dht
  PRIVATE
    boot_strapper.cc
    i_partitioner.cc
    murmur3_partitioner.cc
    range_streamer.cc
    token.cc)
target_include_directories(dht
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(dht
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    replica)
