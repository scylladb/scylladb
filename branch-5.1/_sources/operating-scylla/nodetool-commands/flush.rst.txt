Nodetool flush
==============
**flush** ``[<keyspace> <cfnames>...]``- Specify a keyspace and one or more tables that you want to flush from the memtable to on disk SSTables.

For example:

.. code-block:: shell

   nodetool flush keyspaces1 standard1

See also

:doc:`Nodetool drain </operating-scylla/nodetool-commands/drain/>`

.. include:: nodetool-index.rst




