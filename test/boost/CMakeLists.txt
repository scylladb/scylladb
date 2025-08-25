add_scylla_test(UUID_test
  KIND BOOST)
add_scylla_test(advanced_rpc_compressor_test
  KIND SEASTAR)
add_scylla_test(allocation_strategy_test
  KIND BOOST)
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
add_scylla_test(big_decimal_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(bloom_filter_test
  KIND SEASTAR)
add_scylla_test(bptree_test
  KIND SEASTAR
  LIBRARIES utils)
add_scylla_test(broken_sstable_test
  KIND SEASTAR)
add_scylla_test(btree_test
  KIND SEASTAR)
add_scylla_test(bytes_ostream_test
  KIND BOOST
  LIBRARIES Seastar::seastar_testing)
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
add_scylla_test(cdc_generation_test
  KIND BOOST
  LIBRARIES Seastar::seastar_testing)
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
add_scylla_test(compaction_group_test
  KIND SEASTAR)
add_scylla_test(comparable_bytes_test
  KIND SEASTAR
  LIBRARIES types)
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
add_scylla_test(cql_auth_syntax_test
  KIND BOOST
  LIBRARIES cql3)
add_scylla_test(crc_test
  KIND BOOST)
add_scylla_test(dict_trainer_test
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
add_scylla_test(encrypted_file_test
  KIND SEASTAR
  LIBRARIES
    encryption)
add_scylla_test(encryption_at_rest_test
  KIND SEASTAR
  LIBRARIES
    Boost::filesystem
    encryption)
add_scylla_test(enum_option_test
  KIND BOOST)
add_scylla_test(enum_set_test
  KIND BOOST)
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
add_scylla_test(hash_test
  KIND SEASTAR)
add_scylla_test(hashers_test
  KIND SEASTAR)
add_scylla_test(hint_test
  KIND SEASTAR)
add_scylla_test(idl_test
  KIND BOOST
  LIBRARIES idl)
add_scylla_test(incremental_compaction_test
  KIND SEASTAR)
add_scylla_test(index_reader_test
  KIND SEASTAR)
add_scylla_test(input_stream_test
  KIND BOOST
  LIBRARIES Seastar::seastar_testing)
add_scylla_test(intrusive_array_test
  KIND SEASTAR)
add_scylla_test(json_test
  KIND BOOST
  LIBRARIES cql3)
add_scylla_test(keys_test
  KIND BOOST
  LIBRARIES idl schema)
add_scylla_test(like_matcher_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(limiting_data_source_test
  KIND SEASTAR)
add_scylla_test(linearizing_input_stream_test
  KIND BOOST)
add_scylla_test(lister_test
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
  KIND BOOST
  LIBRARIES Seastar::seastar_testing)
add_scylla_test(managed_vector_test
  KIND SEASTAR)
add_scylla_test(map_difference_test
  KIND BOOST)
add_scylla_test(murmur_hash_test
  KIND BOOST)
add_scylla_test(mutation_fragment_test
  KIND SEASTAR)
add_scylla_test(mutation_query_test
  KIND SEASTAR)
add_scylla_test(mutation_reader_another_test
  KIND SEASTAR)
add_scylla_test(mutation_test
  KIND SEASTAR)
add_scylla_test(mvcc_test
  KIND SEASTAR)
add_scylla_test(nonwrapping_interval_test
  KIND BOOST)
add_scylla_test(observable_test
  KIND BOOST)
add_scylla_test(partitioner_test
  KIND SEASTAR)
add_scylla_test(pretty_printers_test
  KIND BOOST)
add_scylla_test(radix_tree_test
  KIND SEASTAR)
add_scylla_test(range_tombstone_list_test
  KIND BOOST)
add_scylla_test(rate_limiter_test
  KIND SEASTAR)
add_scylla_test(recent_entries_map_test
  KIND SEASTAR)
add_scylla_test(result_utils_test
  KIND SEASTAR)
add_scylla_test(reusable_buffer_test
  KIND SEASTAR)
add_scylla_test(reservoir_sampling_test
  KIND BOOST)
add_scylla_test(rust_test
  KIND BOOST
  LIBRARIES inc)
add_scylla_test(s3_test
  KIND SEASTAR)
add_scylla_test(aws_errors_test
  KIND BOOST)
add_scylla_test(aws_error_injection_test
  KIND SEASTAR)
add_scylla_test(schema_changes_test
  KIND SEASTAR)
add_scylla_test(schema_loader_test
  KIND SEASTAR
  LIBRARIES tools)
add_scylla_test(serialization_test
  KIND BOOST)
add_scylla_test(serialized_action_test
  KIND SEASTAR)
add_scylla_test(service_level_controller_test
  KIND SEASTAR)
add_scylla_test(small_vector_test
  KIND SEASTAR)
add_scylla_test(snitch_reset_test
  KIND SEASTAR)
add_scylla_test(sorting_test
  KIND SEASTAR)
add_scylla_test(sstable_3_x_test
  KIND SEASTAR)
add_scylla_test(sstable_conforms_to_mutation_source_test
  KIND SEASTAR)
add_scylla_test(sstable_datafile_test
  KIND SEASTAR)
add_scylla_test(sstable_generation_test
  KIND BOOST)
add_scylla_test(sstable_inexact_index_test
  KIND SEASTAR)
add_scylla_test(sstable_move_test
  KIND SEASTAR)
add_scylla_test(sstable_mutation_test
  KIND SEASTAR)
add_scylla_test(sstable_partition_index_cache_test
  KIND SEASTAR)
add_scylla_test(sstable_resharding_test
  KIND SEASTAR)
add_scylla_test(sstable_test
  KIND SEASTAR)
add_scylla_test(stall_free_test
  KIND SEASTAR)
add_scylla_test(stream_compressor_test
  KIND BOOST
  LIBRARIES
    Seastar::seastar_testing
    utils)
add_scylla_test(string_format_test
  KIND BOOST)
add_scylla_test(summary_test
  KIND BOOST)
add_scylla_test(file_stream_test
  KIND SEASTAR)
add_scylla_test(tagged_integer_test
  KIND SEASTAR)
add_scylla_test(token_metadata_test
  KIND SEASTAR)
add_scylla_test(top_k_test
  KIND BOOST)
add_scylla_test(transport_test
  KIND SEASTAR)
add_scylla_test(bti_index_test
  KIND SEASTAR)
add_scylla_test(bti_node_sink_test
  KIND BOOST)
add_scylla_test(bti_key_translation_test
  KIND BOOST)
add_scylla_test(trie_traversal_test
  KIND SEASTAR)
add_scylla_test(trie_writer_test
  KIND BOOST)
add_scylla_test(types_test
  KIND SEASTAR)
add_scylla_test(unique_view_test
  KIND BOOST)
add_scylla_test(scoped_item_list_test
  KIND SEASTAR)
add_scylla_test(utf8_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(vint_serialization_test
  KIND BOOST
   LIBRARIES
     scylla-main
     utils)
add_scylla_test(virtual_table_mutation_source_test
  KIND SEASTAR)
add_scylla_test(wasm_alloc_test
  KIND SEASTAR)
add_scylla_test(wasm_test
  KIND SEASTAR)
add_scylla_test(wrapping_interval_test
  KIND BOOST)
add_scylla_test(address_map_test
  KIND SEASTAR)
add_scylla_test(vector_store_client_test
  KIND SEASTAR)

add_scylla_test(combined_tests
  KIND SEASTAR
  SOURCES
    combined_tests.cc
    aggregate_fcts_test.cc
    auth_test.cc
    batchlog_manager_test.cc
    cache_algorithm_test.cc
    castas_fcts_test.cc
    cdc_test.cc
    column_mapping_test.cc
    commitlog_cleanup_test.cc
    commitlog_test.cc
    cql_auth_query_test.cc
    cql_functions_test.cc
    cql_query_group_test.cc
    cql_query_large_test.cc
    cql_query_like_test.cc
    cql_query_test.cc
    database_test.cc
    data_listeners_test.cc
    error_injection_test.cc
    extensions_test.cc
    filtering_test.cc
    group0_cmd_merge_test.cc
    group0_test.cc
    group0_voter_calculator_test.cc
    index_with_paging_test.cc
    json_cql_query_test.cc
    large_paging_state_test.cc
    loading_cache_test.cc
    memtable_test.cc
    multishard_combining_reader_as_mutation_source_test.cc
    multishard_mutation_query_test.cc
    mutation_reader_test.cc
    mutation_writer_test.cc
    network_topology_strategy_test.cc
    per_partition_rate_limit_test.cc
    pluggable_test.cc
    querier_cache_test.cc
    query_processor_test.cc
    reader_concurrency_semaphore_test.cc
    repair_test.cc
    role_manager_test.cc
    restrictions_test.cc
    row_cache_test.cc
    schema_change_test.cc
    schema_registry_test.cc
    secondary_index_test.cc
    sessions_test.cc
    sstable_compaction_test.cc
    sstable_compressor_factory_test.cc
    sstable_directory_test.cc
    sstable_set_test.cc
    statement_restrictions_test.cc
    storage_proxy_test.cc
    tablets_test.cc
    tracing_test.cc
    user_function_test.cc
    user_types_test.cc
    view_build_test.cc
    view_complex_test.cc
    view_schema_ckey_test.cc
    view_schema_pkey_test.cc
    view_schema_test.cc
    virtual_reader_test.cc
    virtual_table_test.cc
  LIBRARIES
    cql3
    idl
    tools
    utils)
