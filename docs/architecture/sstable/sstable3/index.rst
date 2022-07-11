Scylla SSTable - 3.x
====================

.. toctree::
   :hidden:

   sstables-3-data-file-format
   sstables-3-statistics
   sstables-3-summary
   sstables-3-index
   sstable-format

.. include:: ../_common/sstable_what_is.rst

* In Scylla 3.1 and above, mc format is enabled by default. 

* In Scylla 3.0, mc format is disabled by default and can be enabled by adding the ``enable_sstables_mc_format`` parameter as 'true' in ``scylla.yaml`` file.

For example: 

.. code-block:: shell

   enable_sstables_mc_format: true


For more information on Scylla 3.x SSTable formats, see below:

* :doc:`SSTable 3.0 Data File Format <sstables-3-data-file-format>`
* :doc:`SSTable 3.0 Statistics <sstables-3-statistics>` 
* :doc:`SSTable 3.0 Summary <sstables-3-summary>`
* :doc:`SSTable 3.0 Index <sstables-3-index>`
* :doc:`SSTable 3.0 Format in Scylla <sstable-format>`
