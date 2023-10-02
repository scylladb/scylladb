=====================================
Tombstone Garbage Collection (GC) 
=====================================

Overview
----------
When you delete some data from the database, the data record is not immediately 
removed. Instead, it is marked with a tombstone - a data marker that prevents 
the data from being returned as a query result. The data covered by the tombstone 
and the tombstone itself are eventually removed after the compaction process 
is performed.

ScyllaDB allows you to manage how and when tombstones are removed, considering 
that tombstones must be garbage-collected:

* Within a reasonable time because a large number of tombstones impacts query 
  response times and leads to performance degradation.
* After a cluster-wide repair to ensure that all replicas are in sync. Otherwise, 
  tombstones might be missing on some replica nodes, which may lead to 
  the resurrection of deleted data.

You can choose one of the following ways to garbage-collect tombstones:

* Repair-based Tombstone GC (recommended) - tombstones are removed when a repair 
  process has been performed.
* Timeout-based Tombstone GC - tombstones are removed when they exceed the time 
  specified with the ``gc_grace_seconds`` option.


Repair-based Tombstone GC (recommended)
-----------------------------------------

When repair-based tombstone GC is enabled, tombstones are only removed after repair 
is performed. Repair synchronizes data between nodes so that all replicas hold 
the same data. In this way, repair-based tombstone GC prevents data resurrection.

You can enable repair-based tombstone GC when you CREATE TABLE or ALTER TABLE. 
Set the ``tombstone_gc`` option to ``repair``. For example:

.. code:: cql

    CREATE TABLE ks.cf (key blob PRIMARY KEY,  val blob) WITH tombstone_gc = {'mode':'repair'};

.. code:: cql

    ALTER TABLE ks.cf WITH tombstone_gc = {'mode':'repair'} ;

See :ref:`Tombstones GC options <ddl-tombstones-gc>`.

Timeout-based Tombstone GC
----------------------------

When timeout-based tombstone GC is enabled, tombstones are only removed after 
they exceed the time specified with the ``gc_grace_seconds`` option. 
However, you must run the repair process to ensure that replicas hold the same 
data, and the process must be completed within the ``gc_grace_seconds`` limit. 
Failing to do so may result in the resurrection of deleted data.

Compared to repair-based tombstone GC, timeout-based tombstone GC is a less 
robust solution because:

* It relies on the user starting repair within ``gc_grace_seconds``.
* It relies on the system completing repair within ``gc_grace_seconds``. If there 
  are other tasks in progress, repair may be slowed down and may not be completed 
  within ``gc_grace_seconds``.

To apply timeout-based tombstone GC:

#. Set the ``tombstone_gc`` option to ``timeout`` when you CREATE TABLE or ALTER TABLE.
   (You can skip this step if the timeout mode is the default; see :ref:`Tombstones GC options <ddl-tombstones-gc>`.)

#. Specify the value of ``gc_grace_seconds`` when you CREATE TABLE or ALTER TABLE. The default is 864000 seconds (10 days). Example:

  .. code:: cql

    ALTER TABLE ks.cf WITH gc_grace_seconds = 80000; 

See :doc:`How to Change gc_grace_seconds for a Table </kb/gc-grace-seconds>` for additional information.

Other Tombstone GC Modes
---------------------------
In rare cases, you may want to choose one of the following tombstone GC modes:

* ``disable`` - Tombstone GC is never performed. This mode may be useful when 
  loading data to the database to avoid tombstone GC when some data is not 
  yet available.
* ``immediate`` - Tombstone GC is immediately performed. There is no wait time 
  or repair requirement. This mode is useful for a table that uses the TWCS 
  compaction strategy with no user deletes. After data expires after TTL, 
  ScyllaDB can perform compaction to drop the expired data immediately.

Both modes can be enabled when you CREATE TABLE or ALTER TABLE. Examples:

.. code:: cql

    CREATE TABLE ks.cf (key blob PRIMARY KEY,  val blob) WITH tombstone_gc = {'mode':'disable'};


.. code:: cql

    ALTER TABLE ks.cf WITH tombstone_gc = {'mode':'immediate'} ;

References
-------------

* `Preventing Data Resurrection with Repair Based Tombstone Garbage Collection <https://www.scylladb.com/2022/06/30/preventing-data-resurrection-with-repair-based-tombstone-garbage-collection/>`_
  at the ScyllaDB blog.
* :doc:`ScyllaDB Repair </operating-scylla/procedures/maintenance/repair>`
* :doc:`Data Definition </cql/ddl>`