Nodetool stop compaction
========================

Stops a compaction operation. This command is usually used to stop compaction that has a negative impact on the performance of a node.

Usage

.. code:: sh

          nodetool <options> stop -- <compaction_type>

Supported compaction types:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Type
     - Stops
   * - ``COMPACTION``
     - Regular (automatic) compactions and major compactions
   * - ``REGULAR``
     - Regular (automatic) compactions only
   * - ``MAJOR``
     - Major compactions only
   * - ``CLEANUP``
     - Cleanup compactions (see :doc:`nodetool cleanup </operating-scylla/nodetool-commands/cleanup>`)
   * - ``SCRUB``
     - Scrub compactions (see :doc:`nodetool scrub </operating-scylla/nodetool-commands/scrub>`)
   * - ``UPGRADE``
     - SSTable upgrades (see :doc:`nodetool upgradesstables </operating-scylla/nodetool-commands/upgradesstables>`)
   * - ``RESHAPE``
     - Reshape compactions
   * - ``SPLIT``
     - Tablet split compactions

Stopping a compaction by id (``--id <id>``) is not implemented.

For example:

.. code:: sh

    nodetool stop COMPACTION

    nodetool stop REGULAR

    nodetool stop MAJOR

    nodetool stop RESHAPE

.. include:: nodetool-index.rst
