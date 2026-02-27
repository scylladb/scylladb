Nodetool upgradesstables
========================

**upgradesstables** - Upgrades each table that is not running the latest Scylla version by rewriting the SSTables. 

Note that this is *not* required when enabling mc format or upgrading to a newer Scylla version. In these cases, Scylla writes a new SSTable, either in MemTable flush or compaction, while keeping the old tables in the old format. 

You can specify to run this action on a specific table or keyspace or on all SSTables. Use this command when changing compression options, or encrypting/decrypting a table for encryption at rest and you want to rewrite SSTable to the new format, instead of waiting for compaction to do it for you at a later time.

Syntax:
``nodetool <options> upgradesstables [upgrade target]``

Where: 

``<options>`` are the :ref:`Nodetool Generic Options <nodetool-generic-options>`

``[upgrade target]`` indicates what you are upgrading and can be any of the following:

* ``--include-all-sstables | -a`` - Upgrades all SSTables by force, including those already in the new SSTable format.
* ``--jobs <jobs> | -j <jobs>`` - Indicates the number of SSTables you want to upgrade simultaneously. Setting this parameter to ``0`` uses all available compaction threads.
* ``keyspace-name`` - Updates all SSTables for all tables in the indicated keyspace
* ``table-name`` - Updates all SSTables for the indicated table or tables, with each table separated by a space. To specify a table, you must include the ``keyspace-name`` and the ``table-name`` separated by a space. 
* ``--`` Separator which can be used to separate an option from an argument. For example, if you are using the ``-a`` flag, use the separator between the flag and the ``keyspace-name``

Example
--------

There are two keyspaces - KeyspaceA and KeyspaceB. KeyspaceA has three tables, tableA (old format), tableB (old format), and tableE (new format).  KeyspaceB has two tables, tableC (old format) and tableD (new format). 

To upgrade tableA and tableB you can either run:

``nodetool upgradesstables KeyspaceA`` or ``nodetool upgradesstables KeyspaceA tableA tableB`` 

Running the nodetool upgradesstabes against the keyspace does not upgrade tableE because it is already in the new format. 

To upgrade tableA or tableB only:

``nodetool upgradesstables KeyspaceA tableA``   

``nodetool upgradesstables KeyspaceA tableB`` 

To upgrade all three tables (A, B, and E) you add the `-a` flag which forces an upgrade to tableE:

``nodetool upgradesstables -a -- KeyspaceA``

To upgrade all old format tables in both keyspaces:

``nodetool upgradesstables KeyspaceA KeyspaceB``

To upgrade all tables (old and new format) in both keyspaces:

``nodetool upgradesstables -a -- KeyspaceA KeyspaceB``

Additional References
---------------------

.. include:: nodetool-index.rst





