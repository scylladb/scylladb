=============================================
How to flush old tombstones from a table
=============================================

**Audience: Devops professionals, architects**

Description
-----------
If you have large partitions with lots of tombstones, you can use this workaround to flush the old tombstones.
To avoid data resurrection, make sure that tables are repaired (either with nodetool repair or ScyllaDB Manager) before the ``gc_grace_seconds`` threshold is reached. 
After the repair finishes, any tombstone older than the previous repair can be flushed.

.. note:: Use :doc:`this article </troubleshooting/large-partition-table/>` to help you find large partitions.

Steps:
^^^^^^
1. Run nodetool repair to synchronize the data between nodes. Alternatively, you can use ScyllaDB Manager to run a repair.

.. code-block:: sh
   
   nodetool repair <options>;
   
2. Set  the ``gc_grace_seconds`` to the time since last repair was started -  For instance, if the last repair was executed one day ago, then set ``gc_grace_seconds`` to one day (86400sec). For more information, please refer to :doc:`this KB article </kb/gc-grace-seconds/>`.

.. note:: To prevent the compaction of unsynched tombstones, it is important to get the timing correctly. If you are not sure what time should set, please contact `ScyllaDB support <https://www.scylladb.com/product/support/>`_.

.. code-block:: sh

   ALTER TABLE <keyspace>.<mytable> WITH gc_grace_seconds = <newTimeValue>;

.. note:: Steps 3 & 4 should be repeated on ALL nodes affected with large tombstone partitions.

3. Run nodetool flush

.. code-block:: sh

   nodetool flush <keyspace> <mytable>;

4. Run compaction (this will remove big partitions with tombstones from specified table)

.. note:: By default, major compaction runs on all the keyspaces and tables, so if we want to specyfy e.g. only one table, we should point at it using arguments: ``<keyspace>.<mytable>``. For more information, please refer to `this article <https://docs.scylladb.com/operating-scylla/nodetool-commands/compact/>`_.

.. code-block:: sh
   
   nodetool compact <keyspace> <mytable>;

5. Alter the table and change the grace period back to the original ``gc_grace_seconds`` value.

.. code-block:: sh
   
   ALTER TABLE <keyspace>.<mytable> WITH gc_grace_seconds = <oldValue>;
