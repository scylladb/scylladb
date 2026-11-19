=============
toppartitions
=============

``EXECUTE COMMAND toppartitions`` samples the reads and writes of the node the statement is
sent to, like :doc:`nodetool toppartitions </operating-scylla/nodetool-commands/toppartitions>`.
It blocks for ``duration`` milliseconds and returns one row per sampled partition, ranked per
table and kind.
A client-side timeout does not cancel the run; it ends when ``duration`` elapses, the
connection closes, or the node shuts down.

Parameters:

* ``local``: mandatory, must be ``true``
* ``keyspace_name``, ``table_name``: sample one keyspace or one table (default: all tables)
* ``duration``: sampling window in milliseconds (``-d``, default 5000, at most one hour)
* ``capacity``: sampler capacity (``-s``, default 256)
* ``list_size``: partitions reported per sampler (``-k``, default 10 or ``capacity`` if smaller, at most ``capacity``)
* ``kind``: ``'read'`` or ``'write'`` to report a single sampler (``-a``, default both)

Superusers can sample anything. Any other role can sample a single user table it has
``SELECT`` on, given by both ``keyspace_name`` and ``table_name``, for at most 60000 ms.
Sampling a whole keyspace, all tables, or an internal table is superuser-only.

.. code-block:: cql

   EXECUTE COMMAND toppartitions WITH local = true AND keyspace_name = 'ks' AND table_name = 't'
       AND duration = 3000 AND capacity = 64 AND list_size = 5;

Example output (``t`` has an ``int`` partition key, so ``partition_key`` is the stringified integer):

.. code-block:: text

    keyspace_name | table_name | kind  | rank | partition_key | count | error
   ---------------+------------+-------+------+---------------+-------+-------
               ks |          t |  read |    0 |             0 |  4102 |     0
               ks |          t | write |    0 |             0 |  4102 |     0
