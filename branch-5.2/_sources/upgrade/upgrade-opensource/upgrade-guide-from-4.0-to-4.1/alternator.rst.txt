Alternator Upgrade - Scylla 4.0 to Scylla 4.1
=============================================

Alternator allows users to choose the isolation level per table. If they do not use one, a default isolation level is set. In Scylla 4.0 the default level was  *always*.
In Scylla 4.1, the user needs to explicitly set the default value in *scylla.yaml*:

* **forbid_rmw** - Forbids write requests which require a read before a write. Will return an error on read-modify-write operations, e.g., conditional updates (UpdateItem with a ConditionExpression).
* **only_rmw_uses_lwt** -  This mode isolates only updates that require read-modify-write. Use this setting only if you do not mix read-modify-write and write-only updates to the same item, concurrently.
* **always** - Isolate every write operation, even those that do not need a read before the write. This is the slowest choice, guaranteed to work correctly for every workload. This was the default in 4.0,

For example, the following configuration will set the default isolation mode to always, which was the default in Scylla 4.0:

.. code:: 

   alternator_write_isolation: always

.. note::

   Alternator 4.1 will not run without setting a default value in scylla.yaml

