Nodetool setlogginglevel
========================

**setlogginglevel** sets the level log threshold for a given component or class during runtime. If this command is called with no parameters, the log level is reset to the initial configuration. 

.. note:: Using trace or debug logging levels will create very large log files where the readers may not find what they are looking for. It is best to use these levels for a very short period of time or with the help of Scylla Support. 

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

.. code-block:: shell

   scylla --help-loggers

   lsa-api
   trace_state
   tracing
   password_authenticator
   init
   stream_session
   lister
   load_broadcaster
   paging
   storage_service
   org.apache.cassandra.db.marshal.TimestampType
   scollectd
   event_notifier
   thrift
   org.apache.cassandra.db.marshal.DateType
   collection_type_impl
   repair
   compaction
   lsa
   failure_injector
   BatchStatement
   seastar_memory
   migration_task
   legacy_schema_migrator
   sstable
   dns_resolver
   lsa-timing
   database
   cql_server
   storage_proxy
   cache
   schema_tables
   rpc
   compaction_manager
   seastar
   migration_manager
   trace_keyspace_helper
   query_processor
   gossip
   failure_detector
   schema_registry
   replication_strategy
   LeveledManifest
   mutation_data
   consistency
   commitlog
   default_authorizer
   auth
   messaging_service
   config
   commitlog_replayer
   heat_load_balance
   batchlog_manager
   token_metadata
   view
   bloom_filter
   unimplemented
   boot_strapper
   query_result
   system_keyspace
   ange_streamer

Additional Information
----------------------
:doc:`Change the Log Level</troubleshooting/log-level/>`

.. include:: nodetool-index.rst
