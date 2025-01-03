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

* In ScyllaDB 6.0 and above, the ``me`` format is mandatory, and ``md`` format is used only when upgrading from an existing cluster using ``md``. The ``sstable_format`` parameter is ignored if it is set to ``md``.
* In ScyllaDB 5.1 and above, the ``me`` format is enabled by default.
* In ScyllaDB 4.3 to 5.0, the ``md`` format is enabled by default.
* In ScyllaDB 3.1 to 4.2, the ``mc`` format is enabled by default. 
* In ScyllaDB 3.0, the ``mc`` format is disabled by default. You can enable it by adding the ``enable_sstables_mc_format`` parameter set to ``true`` in the ``scylla.yaml`` file. For example: 
    
    .. code-block:: shell
    
       enable_sstables_mc_format: true

.. REMOVE IN FUTURE VERSIONS - Remove the note above in version 5.2.

Additional Information
-------------------------

For more information on ScyllaDB 3.x SSTable formats, see below:

* :doc:`SSTable 3.0 Data File Format <sstables-3-data-file-format>`
* :doc:`SSTable 3.0 Statistics <sstables-3-statistics>` 
* :doc:`SSTable 3.0 Summary <sstables-3-summary>`
* :doc:`SSTable 3.0 Index <sstables-3-index>`
* :doc:`SSTable 3.0 Format in ScyllaDB <sstable-format>`
