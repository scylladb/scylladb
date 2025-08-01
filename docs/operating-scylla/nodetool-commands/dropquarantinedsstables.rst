Nodetool dropquarantinedsstables
==================================
**dropquarantinedsstables** - Drop quarantined SSTables from the specified keyspace and table(s), or from all
keyspaces if no keyspace is specified.

OPTIONS
.......

====================================================================  ==================================================================================================================
Parameter                                                             Description
====================================================================  ==================================================================================================================
``[<keyspace>]``                                                      Optional. The keyspace to drop quarantined SSTables from. If not specified, all keyspaces will be affected.
--------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------------------
``[<table...>]``                                                      Optional. One or more tables to drop quarantined SSTables from. If not specified, all tables in the keyspace will be affected.
====================================================================  ==================================================================================================================

Examples
........

Drop quarantined SSTables from all keyspaces and tables

.. code-block:: shell

   > nodetool dropquarantinedsstables

Drop quarantined SSTables from all tables in a keyspace (mykeyspace)

.. code-block:: shell

   > nodetool dropquarantinedsstables mykeyspace

Drop quarantined SSTables from a specific table (mytable) in a keyspace (mykeyspace)

.. code-block:: shell

   > nodetool dropquarantinedsstables mykeyspace mytable

Drop quarantined SSTables from multiple specific tables (mytable1, mytable2) in a keyspace (mykeyspace)

.. code-block:: shell

   > nodetool dropquarantinedsstables mykeyspace mytable1 mytable2

.. include:: nodetool-index.rst