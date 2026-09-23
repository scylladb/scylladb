Nodetool dropquarantinedsstables
==================================
**dropquarantinedsstables** - Drop quarantined SSTables from the specified keyspace and table(s), or from all
keyspaces if no keyspace is specified.

.. warning::

    Dropping quarantined SSTables can resurrect deleted data.
    This can happen regardless of the consistency level and of the ``repair`` tombstone GC mode.
    Its usage is highly discouraged. The only supported way to rid a node of corrupt data is to replace it. Anything else risks data resurrection or data loss.
    This operation should only be used with full understanding of the risks when no viable alternatives remain.
    To accept the risk and carry out the operation, provide the ``--i-accept-data-resurrection-risk`` flag.

OPTIONS
.......

====================================================================  ==================================================================================================================
Parameter                                                             Description
====================================================================  ==================================================================================================================
``--i-accept-data-resurrection-risk``                                 Optional. Accept data resurrection risk. The command is rejected if not specified.
--------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------------------
``[<keyspace>]``                                                      Optional. The keyspace to drop quarantined SSTables from. If not specified, all keyspaces will be affected.
--------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------------------
``[<table...>]``                                                      Optional. One or more tables to drop quarantined SSTables from. If not specified, all tables in the keyspace will be affected.
====================================================================  ==================================================================================================================

Examples
........

Drop quarantined SSTables from all keyspaces and tables

.. code-block:: shell

   > nodetool dropquarantinedsstables --i-accept-data-resurrection-risk

Drop quarantined SSTables from all tables in a keyspace (mykeyspace)

.. code-block:: shell

   > nodetool dropquarantinedsstables --i-accept-data-resurrection-risk mykeyspace

Drop quarantined SSTables from a specific table (mytable) in a keyspace (mykeyspace)

.. code-block:: shell

   > nodetool dropquarantinedsstables --i-accept-data-resurrection-risk mykeyspace mytable

Drop quarantined SSTables from multiple specific tables (mytable1, mytable2) in a keyspace (mykeyspace)

.. code-block:: shell

   > nodetool dropquarantinedsstables --i-accept-data-resurrection-risk mykeyspace mytable1 mytable2

.. include:: nodetool-index.rst