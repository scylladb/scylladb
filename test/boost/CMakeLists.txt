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
  endif()
  if(parsed_args_LIBRARIES)
    target_link_libraries(${name}
      PRIVATE
        ${parsed_args_LIBRARIES})
  endif()
endfunction()

add_scylla_test(anchorless_list_test
  KIND BOOST)
add_scylla_test(auth_passwords_test
  KIND BOOST
  LIBRARIES auth)
add_scylla_test(auth_resource_test
  KIND BOOST)
add_scylla_test(big_decimal_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(caching_options_test
  KIND BOOST)
add_scylla_test(cartesian_product_test
  KIND BOOST)
add_scylla_test(checksum_utils_test
  KIND BOOST)
add_scylla_test(chunked_vector_test
  KIND BOOST)
add_scylla_test(compress_test
  KIND BOOST)
add_scylla_test(cql_auth_syntax_test
  KIND BOOST
  LIBRARIES cql3)
add_scylla_test(crc_test
  KIND BOOST)
add_scylla_test(duration_test
  KIND BOOST)
add_scylla_test(dynamic_bitset_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(enum_option_test
  KIND BOOST)
add_scylla_test(enum_set_test
  KIND BOOST)
add_scylla_test(idl_test
  KIND BOOST
  LIBRARIES idl)
add_scylla_test(json_test
  KIND BOOST
  LIBRARIES cql3)
add_scylla_test(keys_test
  KIND BOOST
  LIBRARIES idl schema)
add_scylla_test(large_paging_state_test
  KIND SEASTAR)
add_scylla_test(like_matcher_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(limiting_data_source_test
  KIND SEASTAR)
add_scylla_test(linearizing_input_stream_test
  KIND BOOST)
add_scylla_test(lister_test
  KIND SEASTAR)
add_scylla_test(loading_cache_test
  KIND SEASTAR)
add_scylla_test(log_heap_test
  KIND BOOST)
add_scylla_test(logalloc_test
  KIND SEASTAR)
add_scylla_test(logalloc_standard_allocator_segment_pool_backend_test
  KIND SEASTAR)
add_scylla_test(managed_bytes_test
  KIND SEASTAR)
add_scylla_test(managed_vector_test
  KIND SEASTAR)
add_scylla_test(memtable_test
  KIND SEASTAR)
add_scylla_test(multishard_mutation_query_test
  SOURCES
    multishard_mutation_query_test.cc
    test_table.cc
  KIND SEASTAR)
add_scylla_test(murmur_hash_test
  KIND BOOST)
add_scylla_test(mutation_fragment_test
  KIND SEASTAR)
add_scylla_test(mutation_query_test
  KIND SEASTAR)
add_scylla_test(mutation_reader_test
  KIND SEASTAR)
add_scylla_test(multishard_combining_reader_as_mutation_source_test
  KIND SEASTAR)
add_scylla_test(mutation_test
  KIND SEASTAR)
add_scylla_test(mutation_writer_test
  KIND SEASTAR)
add_scylla_test(mvcc_test
  KIND SEASTAR)
add_scylla_test(map_difference_test
  KIND BOOST)
add_scylla_test(network_topology_strategy_test
  KIND SEASTAR)
add_scylla_test(nonwrapping_range_test
  KIND BOOST)
add_scylla_test(observable_test
  KIND BOOST)
add_scylla_test(partitioner_test
  KIND SEASTAR)
add_scylla_test(per_partition_rate_limit_test
  KIND SEASTAR)
add_scylla_test(querier_cache_test
  KIND SEASTAR)
add_scylla_test(query_processor_test
  KIND SEASTAR)
add_scylla_test(radix_tree_test
  KIND SEASTAR)
add_scylla_test(range_test
  KIND BOOST)
add_scylla_test(range_tombstone_list_test
  KIND BOOST)
add_scylla_test(rate_limiter_test
  KIND SEASTAR)
add_scylla_test(reader_concurrency_semaphore_test
  KIND SEASTAR)
add_scylla_test(repair_test
  KIND SEASTAR)
add_scylla_test(restrictions_test
  KIND SEASTAR)
add_scylla_test(result_utils_test
  KIND SEASTAR)
add_scylla_test(reusable_buffer_test
  KIND SEASTAR)
add_scylla_test(role_manager_test
  KIND SEASTAR)
add_scylla_test(row_cache_test
  KIND SEASTAR)
add_scylla_test(rust_test
  KIND BOOST
  LIBRARIES inc)
add_scylla_test(serialization_test
  KIND BOOST)
add_scylla_test(small_vector_test
  KIND SEASTAR)
add_scylla_test(summary_test
  KIND BOOST)
add_scylla_test(top_k_test
  KIND BOOST)
add_scylla_test(tracing_test
  KIND SEASTAR)
add_scylla_test(transport_test
  KIND SEASTAR)
add_scylla_test(type_json_test
  KIND BOOST)
add_scylla_test(types_test
  KIND SEASTAR)
add_scylla_test(vint_serialization_test
  KIND BOOST
  LIBRARIES
    scylla-main
    utils)
add_scylla_test(bptree_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(utf8_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(user_function_test
  KIND SEASTAR
  LIBRARIES idl)
add_scylla_test(user_types_test
  KIND SEASTAR)
add_scylla_test(expr_test
  KIND BOOST
  LIBRARIES cql3)
