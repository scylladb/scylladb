Nodetool compact
================

Forces a (major) compaction on one or more tables.
Compaction is an optimization that reduces the cost of IO and CPU over time by merging rows in the background.

By default, major compaction runs on all the ``keyspaces`` and tables.
Major compactions will take all the SSTables for a column family and merge them into a **single SSTable per shard**.
If a keyspace is provided, the compaction will run on all of the tables within that keyspace. If one or more tables are provided as command-line arguments, the compaction will run only on those tables.

.. caution:: It is always best to allow Scylla to automatically run minor compactions using a :doc:`compaction strategy </kb/compaction>`. Using Nodetool to run compaction can quickly exhaust all resources, increase operational costs, and take up valuable disk space. For this reason, major compactions should be avoided and are not recommended for any production system.  


Syntax
-------
.. code-block:: console

   nodetool [options] compact [--partition <partition_key>] [<keyspace> [<cfnames>]...]

Options
--------

* ``-h <host>`` or  ``--host <host>`` - Node hostname or IP address.

* ``-p <port>`` or ``--port <port>`` - Remote JMX agent port number.

* ``--partition <partition_key>`` - String representation of the partition key.

* ``-pp`` or ``--print-port`` - Operate in 4.0 mode with hosts disambiguated by port number.

* ``-pw <password>`` or ``--password <password>`` - Remote JMX agent password.

* ``-pwf <passwordFilePath>`` or ``--password-file <passwordFilePath>`` - Path to the JMX password file.

* ``-u <username>`` or ``--username <username>`` - Remote JMX agent username.

* ``--`` - Separates command-line options from the list of argument(useful when an argument might be mistaken for a command-line option).

The following options are NOT supported:

* ``-st`` or ``--start-token``
* ``-et`` or ``--end-token``
* ``--user-defined``
* ``--split-output``

Examples
---------

.. code-block:: shell

   nodetool compact
   nodetool compact keyspace1
   nodetool compact keyspace1 standard1

See Also
--------

.. include:: nodetool-index.rst

:doc:`Compaction Overview </kb/compaction>`

:doc:`CQL compaction Reference </cql/compaction>`

:doc:`How to choose a Compaction Strategy </architecture/compaction/compaction-strategies>`
