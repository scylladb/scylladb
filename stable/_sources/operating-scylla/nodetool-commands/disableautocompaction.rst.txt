Nodetool disableautocompaction
==============================

**disableautocompaction** disables automatic compaction for the given keyspace and table(s).

For example:

::

    nodetool disableautocompaction keyspace1 standard1

Syntax
------

.. code-block:: none
  
     nodetool disableautocompaction [<keyspace> [<tables>...]]

nodetool disableautocompaction takes the following parameters:

.. list-table::
   :widths: 50 50
   :header-rows: 1

   * - Parameter Name
     - Description
   * - ``<keyspace>``
     - The keyspace to operate on.  If omitted, auto-compaction will be disabled in all keyspaces.
   * - ``<tables>...``
     - A comma-separated list of one or more tables to operate on.  Tables may be specified only if a keyspace is given.  If omitted, auto-compaction will be disabled in all tables in the specified keyspace.

.. include:: nodetool-index.rst  

.. include:: /rst_include/apache-copyrights.rst
