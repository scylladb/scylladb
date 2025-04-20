================
Nodetool restore
================

**restore** - Load SSTables from a designated bucket in object store into a specified keyspace or table

Note that status of restore can be checked for ``user_task_ttl`` seconds after the operation is done.
You can set the ttl using :doc:`nodetool tasks user-ttl </operating-scylla/nodetool-commands/tasks/user-ttl>`.
If ``--nowait`` flag is not set, the command relies on ``user_task_ttl`` internally.

Syntax
------

.. code-block:: console

   nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
               --endpoint <endpoint> --bucket <bucket>
               --prefix <prefix>
               --keyspace <keyspace>
               --table <table>
               [--nowait]
               [--scope <scope>]
               <sstables>...

Example
-------

.. code-block:: console

   nodetool restore --endpoint s3.us-east-2.amazonaws.com  --bucket bucket-foo --prefix ks/cf/24601 --keyspace ks --table cf \
     scylla/ks/cf/34/me-3gdq_0bki_2dy4w2gqj6hoso4mw1-big-TOC.txt \
     scylla/ks/cf/34/me-3gdq_0bki_2dipc1ysb2x2a3btgh-big-TOC.txt \
     scylla/ks/cf/42/me-3gdq_0bki_2s3e829t3gyq994yjl-big-TOC.txt


Options
-------

* ``-h <host>`` or ``--host <host>`` - Node hostname or IP address.
* ``--endpoint`` - Name of the configured object storage endpoint to load SSTables from.
  This should be configured as per :ref:`the object storage configuration instructions <object-storage-configuration>`.
* ``--bucket`` - Name of the bucket to load SSTables from
* ``--prefix`` - The share prefix for object keys of backed up SSTables
* ``--keyspace`` - Name of the keyspace to load SSTables into
* ``--table`` - Name of the table to load SSTables into
* ``--nowait`` - Don't wait on the restore process
* ``--scope <scope>`` - Use specified load-and-stream scope
* ``<sstables>`` - Remainder of keys of the TOC (Table of Contents) components of SSTables to restore, relative to the specified prefix

The `scope` parameter describes the subset of cluster nodes where you want to load data:

* `node` - On the local node.
* `rack` - On the local rack.
* `dc` - In the datacenter (DC) where the local node lives.
* `all` (default) - Everywhere across the cluster.

To fully restore a cluster, you should combine the ``scope`` parameter with the correct list of
SStables to restore to each node.
On one extreme, one node is given all SStables with the scope ``all``; on the other extreme, all
nodes are restoring only their own SStables with the scope ``node``. In between, you can choose
a subset of nodes to restore only SStables that belong to the rack or DC.

See also

:doc:`Nodetool backup </operating-scylla/nodetool-commands/backup/>`

.. include:: nodetool-index.rst
