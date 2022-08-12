SSTableLoader
=============

Bulk loads SSTables from a directory to a ScyllaDB cluster via the **CQL API**.

.. note::

   This tool is **different than Apache Cassandra's sstableloader**, which uses an internal RPC protocol to load data.

.. note::

   sstableloader does **not** support loading from encrypted files. For restoring data from encrypted files, see :ref:`Migration to ScyllaDB  <cassandra-to-scylla-procedure>`.

.. note:: 

    ScyllaDB does not support local counters used by Apache Cassandra 2.0. If you are migrating from Cassandra 2.0 or a later Cassandra version upgraded from 2.0, using SSTableLoader to migrate local counters to ScyllaDB will not work.
    
    ScyllaDB supports global counters, which are used by Cassandra 2.1 and later.

SSTableLoader can be used to restore data from ScyllaDB or Apache Cassandra backups or to clone data from cluster to cluster.
It is especially useful when the number of nodes or token range distribution between the source and target cluster are **not** the same. ScyllaDB takes care of distributing the data via the CQL API.

See :doc:`Apache Cassandra to ScyllaDB Migration Process </operating-scylla/procedures/cassandra-to-scylla-migration-process>` for an example of usage.


Usage
------

.. code-block:: console

    sstableloader [options] <dir_path>


Bulk loads the SSTables found in the specified directory (``<dir_path>``) to the configured cluster. The parent directories of 
``<dir_path>`` are used as the target ``keyspace/table`` name. For instance, to load an SSTable named ``Standard1-g-1-Data.db`` into 
``Keyspace1/Standard1``, you will need to have the files ``Standard1-g-1-Data.db`` and ``Standard1-g-1-Index.db`` in 
the ``/path/to/Keyspace1/Standard1/`` directory.



Parameters
------------

* ``-alg,--ssl-alg <ALGORITHM>`` - Client SSL: algorithm (default: SunX509).
* ``-bs,--batch-size <Number of bytes above which batch is being sent out>`` - Does not work with ``-nb``.
* ``-ciphers,--ssl-ciphers <CIPHER-SUITES>`` - Client SSL: comma-separated list of encryption suites to use.
* ``-cl,--consistency-level <consistency level (default: ONE)>`` - Sets the consistency level for statements.
* ``-cph,--connections-per-host <connectionsPerHost>`` - Number of concurrent connections-per-host.
* ``-d,--nodes <initial hosts>`` - Required. Try to connect to these hosts (comma separated) initially for ring information.
* ``-f,--conf-path <path to config file>`` - Path to `cassandra.yaml` file for streaming throughput and client/server SSL.
* ``-g,--ignore-missing-columns <COLUMN NAMES...>`` - Ignores the specified missing columns in tables.
* ``-h,--help`` - Displays this help message.
* ``-i,--ignore <NODES>`` - Comma-separated list of nodes to ignore (not to stream to).
* ``-ic,--ignore-dropped-counter-data`` - Ignores dropping local and remote counter shard data.
* ``-ir,--no-infinite-retry`` - Disables infinite retry policy.
* ``-j,--threads-count <Number of threads to execute tasks>`` - Runs tasks in parallel.
* ``-ks,--keystore <KEYSTORE>`` - Client SSL: full path to keystore.
* ``-kspw,--keystore-password <KEYSTORE-PASSWORD>`` - Client SSL: password of the keystore
* ``-nb,--no-batch`` - Prevents using batch statement updates for the same partition key.
* ``--no-progress`` - Prevents displaying progress.
* ``-nx,--no-prepared`` - Prevents using prepared statements.
* ``-p,--port <port>`` - Port used for connections (default: 9042).
* ``-prtcl,--ssl-protocol <PROTOCOL>`` - Client SSL: connections protocol to use (default: TLS).
* ``-pt,--partitioner <class>`` - Partitioner type to use, defaults to the cluster value.
* ``-pw,--password <password>`` - Password for ScyllaDB authentication.
* ``-s,--ssl <SSL>`` - Enables using the SSL connection(s).
* ``-sim,--simulate`` - simulate. Only print CQL generated
* ``-st,--store-type <STORE-TYPE>`` - Client SSL: type of store.
* ``-t,--throttle <throttle>`` - Throttles the speed in Mbits (unlimited by default).
* ``-tr,--token-ranges <<lo>:<hi>,...>`` - Imports only partitions that satisfy lo < token(partition) <= hi.
* ``-translate,--translate <mapping list>`` - Comma-separated list of column name mappings.
* ``-ts,--truststore <TRUSTSTORE>`` - Client SSL: full path to truststore.
* ``-tspw,--truststore-password <TRUSTSTORE-PASSWORD>`` - Client SSL: password to the truststore.
* ``--use-unset`` - Enables using `unset`` values in prepared statements.
* ``--username <username>`` - Username for ScyllaDB authentication.
* ``-v,--verbose <LEVEL>`` - Enables verbose output.
