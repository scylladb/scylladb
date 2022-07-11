scylla-sstable
==============

.. versionadded:: 5.0

Introduction
-------------

This tool allows you to examine the content of SStables by performing operations such as dumping the content of SStables,
generating a histogram, validating the content of SStables, and more. See `Supported Operations`_ for the list of available operations.

Run ``scylla-sstable --help`` for additional information about the tool and the operations.

Usage
------

Syntax
^^^^^^

The command syntax is as follows:

.. code-block:: console

   scylla-sstable <operation> <path to SStable>


You can specify more than one SStable.

Supported Operations
^^^^^^^^^^^^^^^^^^^^^^^
The ``dump-*`` operations output JSON. For ``dump-data``, you can specify another output format.

* ``dump-data`` - Dumps the content of the SStable. You can use it with additional parameters:

   * ``--merge`` - Allows you to process multiple SStables as a unified stream (if not specified, multiple SStables are processed one by one). 
   * ``--partition={{<partition key>}}`` or ``partitions-file={{<partition key>}}`` - Allows you to narrow down the scope of the operation to specified partitions. To specify the partition(s) you want to be processed, provide partition keys in the hexdump format used by ScyllaDB (the hex representation of the raw buffer).
   * ``--output-format=<format>`` - Allows you to specify the output format: ``json`` or ``text``.

* ``dump-index`` - Dumps the content of the SStable index.
* ``dump-compression-info`` - Dumps the SStable compression information, including compression parameters and mappings between 
  compressed and uncompressed data.
* ``dump-summary`` - Dumps the summary of the SStable index.
* ``dump-statistics`` - Dumps the statistics of the SStable, including metadata about the data component.
* ``dump-scylla-metadata`` - Dumps the SStable's scylla-specific metadata.
* ``writetime-histogram`` - Generates a histogram of all the timestamps in the SStable. You can use it with a parameter:

   * ``--bucket=<unit>`` - Allows you to specify the unit of time to be used as bucket (years, months, weeks, days, or hours).

* ``validate`` - Validates the content of the SStable with the mutation fragment stream validator.
* ``validate-checksums`` - Validates SStable checksums (full checksum and per-chunk checksum) against the SStable data.
* ``decompress`` - Decompresses the data component of the SStable (the ``*-Data.db`` file) if compressed. The decompressed data is written to a ``*-Data.decompressed`` file.

Examples
^^^^^^^^
Dumping the content of the SStable:

.. code-block:: console

   scylla-sstable dump-data /path/to/md-123456-big-Data.db

Dumping the content of two SStables as a unified stream:

.. code-block:: console

   scylla-sstable dump-data --merge /path/to/md-123456-big-Data.db /path/to/md-123457-big-Data.db


Validating the specified SStables:

.. code-block:: console

   scylla-sstable validate /path/to/md-123456-big-Data.db /path/to/md-123457-big-Data.db
