Nodetool clearsnapshot
======================
**clearsnapshot** ``[-t tag] [<keyspace>]``- This command removes snapshots. By default all the snapshots will be removed unless a ``snapshot_name`` is provided.

For example:

All snapshots.

.. code-block:: shell

   nodetool clearsnapshot

A specific snapshot name.

.. code-block:: shell

   nodetool clearsnapshot -t <snapshot_name>

.. include:: nodetool-index.rst
