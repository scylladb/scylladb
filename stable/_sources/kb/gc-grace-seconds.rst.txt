==========================================
How to Change gc_grace_seconds for a Table
==========================================

.. your title should be something customers will search for.

**Topic: subject**

How to change (reduce) gc_grace_seconds parameter of the table

**Audience: ScyllaDB administrators**


Issue
-----

When you want to change the ``gc_grace_seconds`` parameter value for a particular table you should readjust your repairs frequency to have at
least one successful full repair for the table in question every (new) ``gc_grace_seconds`` window as discussed `here <https://university.scylladb.com/courses/scylla-operations/lessons/repair-tombstones-and-scylla-manager/topic/repair-tombstones-and-scylla-manager/>`_
in order to avoid data resurrection.

In addition, you should follow the procedure below in order to avoid data resurrection after the changing the ``gc_grace_seconds`` and before the next repair.


Resolution
----------
#. Run a `full repair <https://manager.docs.scylladb.com/stable/repair>`_ for the table in question.
#. Change the ``gc_grace_seconds`` value for the table using the :ref:`ALTER table <alter-table-statement>` command.
#. Verify that the schema is in sync after the change by issuing :doc:`nodetool describecluster </operating-scylla/nodetool-commands/describecluster>` command from all nodes.
   Verify that only a single schema version is reported. Read the :doc:`Schema Mismatch Troubleshooting Guide </troubleshooting/error-messages/schema-mismatch>` if it's not the case.
#. Make sure that you run at least one `full repair <https://manager.docs.scylladb.com/stable/repair>`_ for the table in question during the ``gc_grace_seconds`` time window.
   For example, if the ``gc_grace_seconds`` is set to 10 days, you should run a full repair on your tables every 8-9 days to make sure your tables are repaired before the ``gc_grace_seconds`` threshold is reached.


Additional Notes
----------------
You can also avoid data resurrection (and hence the requirement of running the repair every gc_grace_seconds)
if you make sure that tombstones are generated with operations that use Consistency Level: *ALL*

The operations that generate tombstones are:

* DELETE operations
* TTLs
* INSERT/UPDATE operations that overwrite the whole composite type value(s) like *map*, *list* or *UDT*.
