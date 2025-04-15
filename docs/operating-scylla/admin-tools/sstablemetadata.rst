SSTableMetadata
===============

.. warning:: SSTableMetadata is deprecated since ScyllaDB 5.4, and will be removed in the next release.
             Please consider switching to :ref:`scylla sstable dump-statistics` and :ref:`scylla sstable dump-summary`.

SSTableMetadata prints metadata in ``Statistics.db`` and ``Summary.db`` about the specified SSTables to the console.

Use the full path to the data file when executing the command.

For example:

.. code-block:: console

   $ sstablemetadata /var/lib/scylla/data/keyspace1/standard1-e6a565803a5e11ee83de7d84e184e393/me-9-big-Data.db
   SSTable: /var/lib/scylla/data/keyspace1/standard1-e6a565803a5e11ee83de7d84e184e393/me-9-big-Data.db
   Partitioner: org.apache.cassandra.dht.Murmur3Partitioner
   Bloom Filter FP chance: 0.010000
   Minimum timestamp: 1691989005063001
   Maximum timestamp: 1691989010554004
   SSTable min local deletion time: 2147483647
   SSTable max local deletion time: 2147483647
   Compressor: -
   TTL min: 0
   TTL max: 0
   First token: -9220288174854979040 (key=4e374b384d4b39313930)
   Last token: -5881915680038312597 (key=324b4e31343339373231)
   minClustringValues: []
   maxClustringValues: []
   Estimated droppable tombstones: 0.0
   SSTable Level: 0
   Repaired at: 0
   Replay positions covered: {}
   totalColumnsSet: 22895
   totalRows: 4579
   originatingHostId: 6a684ae1-7b76-44cc-9e07-32975af0e60e
   Estimated tombstone drop times:Count               Row Size        Cell Count
   1                          0                 0
   2                          0                 0
   3                          0                 0
   4                          0                 0
   5                          0              4579
   6                          0                 0
   7                          0                 0
   8                          0                 0
   ...
   1414838745986                 0
   Estimated cardinality: 42
   EncodingStats minTTL: 0
   EncodingStats minLocalDeletionTime: 1442880000
   EncodingStats minTimestamp: 1691988990337000
   KeyType: org.apache.cassandra.db.marshal.BytesType
   ClusteringTypes: []
   StaticColumns: {}
   RegularColumns: {C3:org.apache.cassandra.db.marshal.BytesType, C4:org.apache.cassandra.db.marshal.BytesType, C0:org.apache.cassandra.db.marshal.BytesType, C1:org.apache.cassandra.db.marshal.BytesType,  C2:org.apache.cassandra.db.marshal.BytesType}
