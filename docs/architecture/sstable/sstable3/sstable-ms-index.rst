.. _sstable-ms-index:

SSTable ms Index: Trie-Based Format
=====================================

The ``ms`` format introduces a trie-based partition index, replacing the previous
Cassandra 3.0 index format used by ``me`` and ``md``.

Benefits
---------

Compared to the previous format, the trie-based index provides:

* **Reduced memory footprint** - less in-memory index data per SSTable.
* **Faster partition lookups** - more efficient index traversal.
* **Smaller on-disk index files** - shared key prefixes reduce redundant storage.

Configuring ms Format
----------------------

New Clusters (2026.2 and Later)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Starting with ScyllaDB 2026.2, ``ms`` is the default value for ``sstable_format`` in the
default ``scylla.yaml``. Clusters created with ScyllaDB 2026.2 use the ``ms`` format unless
the configuration is changed.

Upgraded Clusters
~~~~~~~~~~~~~~~~~~

Clusters upgraded from an earlier version continue to use the format specified in their
``scylla.yaml``. To switch to the ``ms`` format, add the following to ``scylla.yaml``:

.. code-block:: yaml

   sstable_format: ms

After the node configuration is updated, newly created SSTables will use the ``ms`` format.

Existing SSTables are not automatically converted. They will be rewritten in ``ms`` format
during the next compaction. Conversion can be triggered using ``nodetool upgradesstables -a``
after updating the node configuration.

Related Topics
--------------

* :doc:`SSTable 3.0 Format in ScyllaDB <sstable-format>` - overview of 3.x format variants