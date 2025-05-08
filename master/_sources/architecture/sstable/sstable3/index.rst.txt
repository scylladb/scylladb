ScyllaDB SSTable - 3.x
=======================

.. toctree::
   :hidden:

   sstables-3-data-file-format
   sstables-3-statistics
   sstables-3-summary
   sstables-3-index
   sstable-format

.. include:: ../_common/sstable_what_is.rst

In ScyllaDB 6.0 and above, the ``me`` format is mandatory, and ``md`` format is used only when upgrading from an existing cluster using ``md``. The ``sstable_format`` parameter is ignored if it is set to ``md``.

Additional Information
-------------------------

For more information on ScyllaDB 3.x SSTable formats, see below:

* :doc:`SSTable 3.0 Data File Format <sstables-3-data-file-format>`
* :doc:`SSTable 3.0 Statistics <sstables-3-statistics>` 
* :doc:`SSTable 3.0 Summary <sstables-3-summary>`
* :doc:`SSTable 3.0 Index <sstables-3-index>`
* :doc:`SSTable 3.0 Format in ScyllaDB <sstable-format>`
