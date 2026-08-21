ScyllaDB Fails to Start - SSTable Corruption Problem
=====================================================

This troubleshooting guide describes what to do when Scylla fails to start due to a corrupted SSTables.
Corruption can be a result of a bug, disk issue or human error, for example deleting one of the SSTable files


Problem
^^^^^^^
Scylla node fails to start, node status shows that the node is down (DN)

How to Verify
^^^^^^^^^^^^^
When there is an unknown problem, checking the :doc:`logs</getting-started/logging/>` is mandatory, using ``journalctl -xe`` 

For example:

.. code-block:: shell

   scylla[28659]:  [shard 0] database - Exception while populating keyspace '<mykeyspace>' with 'test' table from file '/var/lib/scylla/data/mykeyspace/test-fa9994e02fd811e7a4ee000000000000': sstables::malformed_sstable_exception (At directory:/var/lib/scylla/data/mykeyspace/test-fa9994e02fd811e7a4ee000000000000: no TOC found for SSTable with generation 2!. Refusing to boot)
 
In this scenario, a missing ``TOC`` file will prevent the Scylla node from starting.

The SSTable corporation problem can be different, for example, other missing or unreadable files. The following solution apply for all of the scenarios.

Solution
^^^^^^^^

1. | Locate all the SSTable files with the reported corrupted generation number as reported in the log, in our example the generation number is **2**.
   | By default the SSTable can be located under ``/var/lib/scylla/data/keyspace_name/table_name-UUID/``
 
2. Delete all the SSTables files that belong to the generation number

For example: 

.. code-block:: shell

   sudo rm test-ka-2*

   -rw-r--r-- 1 scylla scylla   66 May  8 14:17 test-ka-2-CompressionInfo.db
   -rw-r--r-- 1 scylla scylla  357 May  8 14:17 test-ka-2-Data.db
   -rw-r--r-- 1 scylla scylla   10 May  8 14:17 test-ka-2-Digest.sha1
   -rw-r--r-- 1 scylla scylla   24 May  8 14:17 test-ka-2-Filter.db
   -rw-r--r-- 1 scylla scylla  140 May  8 14:17 test-ka-2-Index.db
   -rw-r--r-- 1 scylla scylla   38 May  8 14:17 test-ka-2-Scylla.db
   -rw-r--r-- 1 scylla scylla 4446 May  8 14:17 test-ka-2-Statistics.db
   -rw-r--r-- 1 scylla scylla   92 May  8 14:17 test-ka-2-Summary.db

3. Start Scylla node

``sudo systemctl start scylla-server``

4. Verify that the node is up again using ``nodetool status``. The node status should be Up Normal (UN)

5. Run the :doc:`nodetool repair </operating-scylla/nodetool-commands/repair/>` command on the node with the corrupted SSTable

.. include:: /troubleshooting/_common/ts-return.rst
