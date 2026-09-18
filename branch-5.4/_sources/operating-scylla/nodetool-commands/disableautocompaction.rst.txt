Nodetool disableautocompaction
==============================

**disableautocompaction** disables automatic compaction for the given keyspace and table according to its compaction strategy.

For example:

::

    nodetool disableautocompaction keyspace.table

Syntax
------

.. code-block:: none
  
     nodetool disableautocompaction [<keyspace> <tables>...]
                
nodetool disableautocompaction takes the following parameters:

.. list-table::
   :widths: 50 50
   :header-rows: 1

   * - Parameter Name
     - Description
   * - ``[<keyspace> <tables>...]``
     - The keyspace followed by one or many tables      
            
.. include:: nodetool-index.rst  

.. include:: /rst_include/apache-copyrights.rst
          
