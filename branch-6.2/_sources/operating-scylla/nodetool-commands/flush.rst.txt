Nodetool flush
==============
**flush** - Flush memtables to on-disk SSTables in the specified keyspace and table(s).

For example:

.. code-block:: shell

   nodetool flush
   nodetool flush keyspace1
   nodetool flush keyspace1 standard1

Syntax
------

.. code-block:: none

   nodetool flush [<keyspace> [<table> ...]]

nodetool flush takes the following parameters:

.. list-table::
   :widths: 50 50
   :header-rows: 1

   * - Parameter Name
     - Description
   * - ``<keyspace>``
     - The keyspace to operate on.  If omitted, all keyspaces are flushed.
   * - ``<table> ...``
     - One or more tables to operate on.  Tables may be specified only if a keyspace is given.  If omitted, all tables in the specified keyspace are flushed.

See also

:doc:`Nodetool drain </operating-scylla/nodetool-commands/drain/>`

.. include:: nodetool-index.rst




