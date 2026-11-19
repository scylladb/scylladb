=============
toppartitions
=============

``EXECUTE COMMAND toppartitions`` samples the reads and writes of the node the statement is
sent to, like :doc:`nodetool toppartitions </operating-scylla/nodetool-commands/toppartitions>`.
It blocks for ``duration`` milliseconds and returns one row per sampled partition, ranked per
table and kind.

Parameters:

* ``local``: mandatory, must be ``true``
* ``keyspace_name``, ``table_name``: sample one keyspace or one table (default: all tables)
* ``duration``: sampling window in milliseconds (``-d``, default 5000, at most one hour)
* ``capacity``: keys each shard's sampler tracks (``-s``, default 256, at most 10000)
* ``list_size``: partitions reported per sampler (``-k``, default 10 or ``capacity`` if smaller, at most ``capacity``)
* ``kind``: ``'read'`` or ``'write'`` to report a single sampler (``-a``, default both)

Superusers can sample anything. Any other role can sample a single user table it has
``SELECT`` on, given by both ``keyspace_name`` and ``table_name``, for at most 60000 ms.
Sampling a whole keyspace, all tables, or an internal table is superuser-only.

Every query in progress adds work to each read and write on the node, so a node runs at most
4 toppartitions queries at once, counting those started through the REST API (``nodetool
toppartitions``). Roles that are not superusers can hold at most 2 of them, so the rest stay
available to superusers and nodetool. Another query fails right away with
``Too many concurrent toppartitions queries``.

.. code-block:: cql

   EXECUTE COMMAND toppartitions WITH local = true AND keyspace_name = 'ks' AND table_name = 't'
       AND duration = 3000 AND capacity = 64 AND list_size = 5;

Example output (``t`` has an ``int`` partition key, so ``partition_key`` is the stringified integer):

.. code-block:: text

    keyspace_name | table_name | kind  | rank | partition_key | count | error
   ---------------+------------+-------+------+---------------+-------+-------
               ks |          t |  read |    0 |             0 |  4102 |     0
               ks |          t | write |    0 |             0 |  4102 |     0
