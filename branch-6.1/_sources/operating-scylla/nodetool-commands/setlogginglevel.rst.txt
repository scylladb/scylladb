Nodetool setlogginglevel
========================

**setlogginglevel** sets the level log threshold for a given component or class during runtime. If this command is called with no parameters, the log level is reset to the initial configuration. 

.. note:: Using trace or debug logging levels will create very large log files where the readers may not find what they are looking for. It is best to use these levels for a very short period of time or with the help of ScyllaDB Support. 

.. code-block:: shell

   nodetool setlogginglevel <class> <level>

Where class is the specific class you want a log for, and level is one of the below levels.

**Example**

This example sets the logging level of the ``paging`` class to ``trace``:

.. code-block:: shell

   nodetool setlogginglevel paging trace

Log Levels
---------- 
You can set the log to any of the following log threshold levels presented here in 
order of severity and verbosity:

* trace
* debug
* info - default setting
* warn
* error

Log Classes
-----------
 
To display the log classes (output changes with each version so your display may be different). 

.. note::

   Please note that this list could be outdated, so you might need to run the following
   command to see the updated list of logger classes.

.. code-block:: console

   $ scylla --help-loggers

   BatchStatement
   LeveledManifest
   alter_keyspace
   alter_table
   alternator-auth
   alternator-conditions
   alternator-executor
   alternator-serialization
   alternator-server
   alternator_controller
   alternator_ttl
   api
   auth
   auth_service
   authorized_prepared_statements_cache
   batchlog_manager
   bloom_filter
   boot_strapper
   cache
   cdc
   certificate_authenticator
   collection_type_impl
   command_factory
   commitlog
   commitlog_replayer
   compaction
   compaction_manager
   compound
   consistency
   controller
   cql3_fuctions
   cql3_selection
   cql_expression
   cql_logger
   cql_server
   cql_server_controller
   create_keyspace
   create_table
   database
   debug_error_injection
   default_authorizer
   describe
   direct_failure_detector
   dns_resolver
   endpoint_state
   event_notifier
   exception
   features
   format_selector
   forward_service
   gossip
   group0_client
   group0_raft_sm
   group0_raft_sm_merger
   heat_load_balance
   hints_manager
   hints_resource_manager
   http
   httpd
   i_partitioner
   incremental_reader_selector
   init
   io
   keys
   keyspace_utils
   large_data
   legacy_schema_migrator
   lister
   load_balancer
   load_broadcaster
   lsa
   lsa-api
   lsa-timing
   lua
   mc_writer
   messaging_service
   metadata_collector
   migration_manager
   multishard_mutation_query
   mutation
   mutation_data
   mutation_fragment_stream_validator
   mutation_partition
   mutation_reader
   node_ops
   on_internal_error
   paging
   password_authenticator
   paxos
   prepared_statements_cache
   qos
   querier
   querier_cache
   query
   query_processor
   query_result
   query_result_log
   raft
   raft_group0
   raft_group0_upgrade
   raft_group_registry
   raft_rpc
   raft_service_distributed_level_data_accessor
   raft_topology
   range_streamer
   reader_concurrency_semaphore
   redis_server
   repair
   replication_strategy
   restrictions
   row_locking
   rpc
   s3
   scheduler
   schema_diff
   schema_loader
   schema_registry
   schema_tables
   scollectd
   scylla-nodetool
   scylla-sstable
   seastar
   seastar_memory
   serializer
   service_level_controller
   session
   sstable
   sstable_directory
   sstables_loader
   sstables_manager
   standard_role_manager
   storage_proxy
   storage_service
   stream_session
   strongly_consistent_modification_statement
   strongly_consistent_select_statement
   system_distributed_keyspace
   system_keyspace
   table
   table_helper
   tablets
   tags
   task_manager
   testlog
   token_group_based_splitting_mutation_writer
   token_metadata
   topology
   topology_state_machine
   trace_keyspace_helper
   trace_state
   tracing
   types
   unimplemented
   version_generator
   view
   view_update_generator
   virtual_tables
   wasm

Additional Information
----------------------
:doc:`Change the Log Level</troubleshooting/log-level/>`

.. include:: nodetool-index.rst
