add_library(mutation STATIC)
target_sources(mutation
  PRIVATE
    async_utils.cc
    atomic_cell.cc
    canonical_mutation.cc
    frozen_mutation.cc
    mutation.cc
    mutation_fragment.cc
    mutation_fragment_stream_validator.cc
    mutation_partition.cc
    mutation_partition_serializer.cc
    mutation_partition_v2.cc
    mutation_partition_view.cc
    partition_version.cc
    range_tombstone.cc
    range_tombstone_list.cc)
target_include_directories(mutation
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(mutation
  PUBLIC
    idl
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    absl::headers)

check_headers(check-headers mutation
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
