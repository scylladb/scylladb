add_scylla_test(UUID_test
  KIND BOOST)
add_scylla_test(aggregate_fcts_test
  KIND SEASTAR)
add_scylla_test(allocation_strategy_test
  KIND SEASTAR)
add_scylla_test(alternator_unit_test
  KIND BOOST
  LIBRARIES alternator)
add_scylla_test(anchorless_list_test
  KIND BOOST)
add_scylla_test(auth_passwords_test
  KIND BOOST
  LIBRARIES auth)
add_scylla_test(auth_resource_test
  KIND BOOST)
add_scylla_test(auth_test
  KIND SEASTAR)
add_scylla_test(batchlog_manager_test
  KIND SEASTAR)
add_scylla_test(big_decimal_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(bloom_filter_test
  KIND SEASTAR)
add_scylla_test(bptree_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(broken_sstable_test
  KIND SEASTAR)
add_scylla_test(btree_test
  KIND SEASTAR)
add_scylla_test(bytes_ostream_test
  KIND SEASTAR)
add_scylla_test(cache_algorithm_test
  KIND SEASTAR)
add_scylla_test(cache_mutation_reader_test
  KIND SEASTAR)
add_scylla_test(cached_file_test
  KIND SEASTAR)
add_scylla_test(caching_options_test
  KIND BOOST)
add_scylla_test(canonical_mutation_test
  KIND SEASTAR)
add_scylla_test(cartesian_product_test
  KIND BOOST)
add_scylla_test(castas_fcts_test
  KIND SEASTAR)
add_scylla_test(cdc_generation_test
  KIND SEASTAR)
add_scylla_test(cdc_test
  KIND SEASTAR)
add_scylla_test(cell_locker_test
  KIND SEASTAR)
add_scylla_test(checksum_utils_test
  KIND BOOST)
add_scylla_test(chunked_managed_vector_test
  KIND SEASTAR)
add_scylla_test(chunked_vector_test
  KIND BOOST)
add_scylla_test(clustering_ranges_walker_test
  KIND SEASTAR)
add_scylla_test(column_mapping_test
  KIND SEASTAR)
add_scylla_test(commitlog_cleanup_test
  KIND SEASTAR)
add_scylla_test(commitlog_test
  KIND SEASTAR)
add_scylla_test(compaction_group_test
  KIND SEASTAR)
add_scylla_test(compound_test
  KIND SEASTAR)
add_scylla_test(compress_test
  KIND BOOST)
add_scylla_test(config_test
  KIND SEASTAR)
add_scylla_test(continuous_data_consumer_test
  KIND SEASTAR)
add_scylla_test(counter_test
  KIND SEASTAR)
add_scylla_test(cql_auth_query_test
  KIND SEASTAR)
add_scylla_test(cql_auth_syntax_test
  KIND BOOST
  LIBRARIES cql3)
add_scylla_test(cql_functions_test
  KIND SEASTAR)
add_scylla_test(cql_query_group_test
  KIND SEASTAR)
add_scylla_test(cql_query_large_test
  KIND SEASTAR)
add_scylla_test(cql_query_like_test
  KIND SEASTAR)
add_scylla_test(cql_query_test
  KIND SEASTAR
  LIBRARIES cql3)
add_scylla_test(crc_test
  KIND BOOST)
add_scylla_test(data_listeners_test
  KIND SEASTAR)
add_scylla_test(database_test
  KIND SEASTAR)
add_scylla_test(dirty_memory_manager_test
  KIND SEASTAR)
add_scylla_test(double_decker_test
  KIND SEASTAR)
add_scylla_test(duration_test
  KIND BOOST)
add_scylla_test(dynamic_bitset_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(enum_option_test
  KIND BOOST)
add_scylla_test(enum_set_test
  KIND BOOST)
add_scylla_test(error_injection_test
  KIND SEASTAR)
add_scylla_test(estimated_histogram_test
  KIND BOOST)
add_scylla_test(exception_container_test
  KIND SEASTAR)
add_scylla_test(exceptions_fallback_test
  KIND SEASTAR)
add_scylla_test(exceptions_optimized_test
  KIND SEASTAR)
add_scylla_test(expr_test
  KIND BOOST
  LIBRARIES cql3)
add_scylla_test(extensions_test
  KIND SEASTAR)
add_scylla_test(filtering_test
  KIND SEASTAR)
add_scylla_test(flush_queue_test
  KIND SEASTAR)
add_scylla_test(fragmented_temporary_buffer_test
  KIND SEASTAR)
add_scylla_test(frozen_mutation_test
  KIND SEASTAR)
add_scylla_test(generic_server_test
  KIND SEASTAR)
add_scylla_test(gossiping_property_file_snitch_test
  KIND SEASTAR)
add_scylla_test(group0_cmd_merge_test
  KIND SEASTAR)
add_scylla_test(group0_test
  KIND SEASTAR)
add_scylla_test(hash_test
  KIND SEASTAR)
add_scylla_test(hashers_test
  KIND SEASTAR)
add_scylla_test(hint_test
  KIND SEASTAR)
add_scylla_test(idl_test
  KIND BOOST
  LIBRARIES idl)
add_scylla_test(index_reader_test
  KIND SEASTAR)
add_scylla_test(index_with_paging_test
  KIND SEASTAR)
add_scylla_test(input_stream_test
  KIND SEASTAR)
add_scylla_test(intrusive_array_test
  KIND SEASTAR)
add_scylla_test(json_cql_query_test
  KIND SEASTAR)
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
add_scylla_test(locator_topology_test
  KIND SEASTAR)
add_scylla_test(log_heap_test
  KIND BOOST)
add_scylla_test(logalloc_standard_allocator_segment_pool_backend_test
  KIND SEASTAR)
add_scylla_test(logalloc_test
  KIND SEASTAR)
add_scylla_test(managed_bytes_test
  KIND SEASTAR)
add_scylla_test(managed_vector_test
  KIND SEASTAR)
add_scylla_test(map_difference_test
  KIND BOOST)
add_scylla_test(memtable_test
  KIND SEASTAR)
add_scylla_test(multishard_combining_reader_as_mutation_source_test
  KIND SEASTAR)
add_scylla_test(multishard_mutation_query_test
  KIND SEASTAR)
add_scylla_test(murmur_hash_test
  KIND BOOST)
add_scylla_test(mutation_fragment_test
  KIND SEASTAR)
add_scylla_test(mutation_query_test
  KIND SEASTAR)
add_scylla_test(mutation_reader_another_test
  KIND SEASTAR)
add_scylla_test(mutation_reader_test
  KIND SEASTAR)
add_scylla_test(mutation_test
  KIND SEASTAR)
add_scylla_test(mutation_writer_test
  KIND SEASTAR)
add_scylla_test(mvcc_test
  KIND SEASTAR)
add_scylla_test(network_topology_strategy_test
  KIND SEASTAR)
add_scylla_test(nonwrapping_interval_test
  KIND BOOST)
add_scylla_test(observable_test
  KIND BOOST)
add_scylla_test(partitioner_test
  KIND SEASTAR)
add_scylla_test(per_partition_rate_limit_test
  KIND SEASTAR)
add_scylla_test(pretty_printers_test
  KIND BOOST)
add_scylla_test(querier_cache_test
  KIND SEASTAR)
add_scylla_test(query_processor_test
  KIND SEASTAR)
add_scylla_test(radix_tree_test
  KIND SEASTAR)
add_scylla_test(range_tombstone_list_test
  KIND BOOST)
add_scylla_test(rate_limiter_test
  KIND SEASTAR)
add_scylla_test(reader_concurrency_semaphore_test
  KIND SEASTAR)
add_scylla_test(recent_entries_map_test
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
add_scylla_test(s3_test
  KIND SEASTAR)
add_scylla_test(schema_change_test
  KIND SEASTAR)
add_scylla_test(schema_changes_test
  KIND SEASTAR)
add_scylla_test(schema_loader_test
  KIND SEASTAR
  LIBRARIES tools)
add_scylla_test(schema_registry_test
  KIND SEASTAR)
add_scylla_test(secondary_index_test
  KIND SEASTAR)
add_scylla_test(serialization_test
  KIND BOOST)
add_scylla_test(serialized_action_test
  KIND SEASTAR)
add_scylla_test(service_level_controller_test
  KIND SEASTAR)
add_scylla_test(sessions_test
  KIND SEASTAR)
add_scylla_test(small_vector_test
  KIND SEASTAR)
add_scylla_test(snitch_reset_test
  KIND SEASTAR)
add_scylla_test(sorting_test
  KIND SEASTAR)
add_scylla_test(sstable_3_x_test
  KIND SEASTAR)
add_scylla_test(sstable_compaction_test
  KIND SEASTAR)
add_scylla_test(sstable_conforms_to_mutation_source_test
  KIND SEASTAR)
add_scylla_test(sstable_datafile_test
  KIND SEASTAR)
add_scylla_test(sstable_directory_test
  KIND SEASTAR)
add_scylla_test(sstable_generation_test
  KIND BOOST)
add_scylla_test(sstable_move_test
  KIND SEASTAR)
add_scylla_test(sstable_mutation_test
  KIND SEASTAR)
add_scylla_test(sstable_partition_index_cache_test
  KIND SEASTAR)
add_scylla_test(sstable_resharding_test
  KIND SEASTAR)
add_scylla_test(sstable_set_test
  KIND SEASTAR)
add_scylla_test(sstable_test
  KIND SEASTAR)
add_scylla_test(stall_free_test
  KIND SEASTAR)
add_scylla_test(statement_restrictions_test
  KIND SEASTAR
  LIBRARIES cql3)
add_scylla_test(storage_proxy_test
  KIND SEASTAR)
add_scylla_test(string_format_test
  KIND BOOST)
add_scylla_test(summary_test
  KIND BOOST)
add_scylla_test(tablets_test
  KIND SEASTAR)
add_scylla_test(tagged_integer_test
  KIND SEASTAR)
add_scylla_test(token_metadata_test
  KIND SEASTAR)
add_scylla_test(top_k_test
  KIND BOOST)
add_scylla_test(tracing_test
  KIND SEASTAR)
add_scylla_test(transport_test
  KIND SEASTAR)
add_scylla_test(types_test
  KIND SEASTAR)
add_scylla_test(user_function_test
  KIND SEASTAR
  LIBRARIES idl)
add_scylla_test(user_types_test
  KIND SEASTAR)
add_scylla_test(utf8_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(view_build_test
  KIND SEASTAR)
add_scylla_test(view_complex_test
  KIND SEASTAR)
add_scylla_test(view_schema_ckey_test
  KIND SEASTAR)
add_scylla_test(view_schema_pkey_test
  KIND SEASTAR)
add_scylla_test(view_schema_test
  KIND SEASTAR)
add_scylla_test(vint_serialization_test
  KIND BOOST
  LIBRARIES
    scylla-main
    utils)
add_scylla_test(virtual_reader_test
  KIND SEASTAR)
add_scylla_test(virtual_table_mutation_source_test
  KIND SEASTAR)
add_scylla_test(virtual_table_test
  KIND SEASTAR)
add_scylla_test(wasm_alloc_test
  KIND SEASTAR)
add_scylla_test(wasm_test
  KIND SEASTAR)
add_scylla_test(wrapping_interval_test
  KIND BOOST)

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/suite.yaml")
  set(scylla_tests "${scylla_tests}" PARENT_SCOPE)
endif()
