Nodetool refresh
================

**refresh** - Load newly placed SSTables to the system without a restart. This is the only supported way to upload SSTables directly to a node, placing them directly in the node's data directory is not supported and will lead to failures.

Copy the files to the table's upload directory, by default it is located under ``/var/lib/scylla/data/keyspace_name/table_name-UUID/upload``.

If the table has any :doc:`Materialized Views (MV)</cql/mv/>` or :doc:`Secondary Indexes (SI)</cql/secondary-indexes/>`, content view updates will be generated from the uploaded sstables. Uploading MV or SI SSTables is not required and will fail.

.. _nodetool-refresh-local:

Local refresh
-------------

SSTables are copied from the upload directory to the table's data directory and added to the table's SSTable registry.

After the upload, ScyllaDB will run cleanup on the uploaded SSTables, to remove any partition which is not owned by the target node.
Use ``--skip-cleanup`` to skip this step. Note that this can lead to increased disk utilization, use only if you are certain the SSTables contain only data owned by the target node.

The uploaded SSTables may disrupt the SSTable layout maintained by the table's compaction strategy. To remedy this, ScyllaDB runs an off-strategy compaction after the upload.
Use ``--skip-reshape`` to skip this step. Note that this can lead to increased read amplification and increased latencies due to reads having to consult more SSTables than optimal.

Although this is the default mode for ``nodetool refresh``, using this mode to restore backed up SSTables is not efficient, for restoring backup use :ref:`--load-and-stream <nodetool-refresh-load-and-stream>` instead.
Use local upload if you are certain that uploaded SSTables contain data only for the target node.

This mode is not supported for tablets.

Syntax:

.. code::

    nodetool refresh <my_keyspace> <my_table> [--skip-cleanup] [--skip-reshape]

Example:

.. code::

    cp /path/to/my/sstables/* /var/lib/scylla/data/nba/player_stats-91cd2060f99d11e6a47/upload``

    nodetool refresh nba player_stats

.. _nodetool-refresh-load-and-stream:

Load and Stream
---------------

SSTables are read and each partition is streamed to its respective replica(s). Allows efficient upload of SSTables to a cluster.
Each SSTable has to be uploaded only once, as ``--load-and-stream`` will ensure that all partitions reach their respective replicas.
The ``--scope`` and ``--primary-replica-only`` options can be used to filter the set of target replicas for each partition.

Syntax:

.. code::

   nodetool refresh <my_keyspace> <my_table> [(--load-and-stream | -las) [[(--primary-replica-only | -pro)] | [--scope <scope>]]]

Filter target replicas
^^^^^^^^^^^^^^^^^^^^^^

By default, each partition is streamed to all nodes which are replicas for the partition.
This can be inefficient in a large cluster, especially if there are multiple Datacenters.
Constraining the subset of replicas where data will be streamed to allows orchestrating concurrent upload to multiple nodes without duplicate work -- the same partition being streamed to a replica from multiple source nodes.

There are two options available to manipulate the replica(s) to stream data to: ``--primary-replica-only`` and ``--scope``.
The two are mutually exclusive.

The ``--primary-replica-only`` (or ``-pro``) option makes ScyllaDB only stream each partition to its primary replica.
After all SSTables are uploaded to the cluster, a repair is required to replicate data to all replicas.

The ``--scope`` parameter allows for more advanced constraining of the subset of nodes where data will be streamed:

* ``node`` - the local node (roughly equivalent to :ref:`local refresh <nodetool-refresh-local>`)
* ``rack`` - replicas in the local rack
* ``dc`` - replicas in the local datacenter (DC)
* ``all`` (default) - all replicas in the cluster

Compatibility with Apache Cassandra SSTables
--------------------------------------------

Uploading SSTable from Apache Cassandra is supported. For the supported SSTable versions, see `ScyllaDB SSTable Format </architecture/sstable/>`_. Note that SSTables using Trie-Based Indexes are `not` supported.

If your SSTables are using a version unsupported by ScyllaDB, use `nodetool upgradesstables <https://cassandra.apache.org/doc/latest/cassandra/managing/tools/sstable/sstableupgrade.html>`_ to downgrade them to a supported one.

Known Problems
^^^^^^^^^^^^^^

Digest mismatch
~~~~~~~~~~~~~~~

Apache Cassandra and ScyllaDB slightly diverged on how digests are calculated on SSTable Data components.
For compressed SSTables, Apache Cassandra allows for a trailing zero-sized (pre-compression) chunk in the Data component.
This chunk has no data but has non-zero size after compression.
This trailing chunk is ignored by ScyllaDB and thus excluded from the Digest calculation, therefore the digest calculated by ScyllaDB will not match that in the Digest component.
Consequently ScyllaDB will reject the SSTable file.

Example:

.. code::

    sstables::malformed_sstable_exception Failed to read partition from SSTable .../me-71-big-Data.db due to Digest mismatch: expected=1580246239, actual=3782253270

Workaround: delete the Digest component (``me-*-big-Digest.crc32``) and remove it from the TOC file (``me-*-big-TOC.txt``) too.

.. include:: nodetool-index.rst
