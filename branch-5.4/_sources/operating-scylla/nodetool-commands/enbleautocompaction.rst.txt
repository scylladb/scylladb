Nodetool enableautocompaction
=============================

**enableautocompaction** enables automatic compaction for the given keyspace and table according to its compaction strategy.

For example:

::

    nodetool enableautocompaction keyspace.table

Syntax
------

.. code-block:: none
  
     nodetool enableautocompaction [<keyspace> <tables>...]
                
nodetool enableautocompaction takes the following parameters:

.. list-table::
   :widths: 50 50
   :header-rows: 1

   * - Parameter Name
     - Description
   * - ``[<keyspace> <tables>...]``
     - The keyspace followed by one or many tables      
            
.. include:: nodetool-index.rst

.. include:: /rst_include/apache-copyrights.rst
            
