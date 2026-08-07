SSTableLoader
=============

Bulk load the sstables from a directory to a Scylla cluster via the **CQL API**.

.. note::

   This is **different** than Apache Cassandra tool with the same name, which uses internal RPC protocol to load the data.

.. note::

   sstableloader does **not** support loading from encrypted files. For restoring data from encrypted files see :ref:`Migration to Scylla  <cassandra-to-scylla-procedure>`.


SSTableLoader can be used to restore data from Scylla or Apache Cassandra backups or to clone data from cluster to cluster.
It is especially useful, when the number of nodes, or token range distrbution between the source and target cluster are **not** the same. Since CQL API is used, Scylla will take care of distrbution the data.

For example of such usage see :doc:`Apache Cassandra to Scylla Migration Process </operating-scylla/procedures/cassandra-to-scylla-migration-process>`

usage: sstableloader [options] <dir_path>

Bulk load the sstables found in the directory <dir_path> to the configured cluster. The parent directories of <dir_path> are used as the target ``keyspace/table`` name. So, for instance, to load an sstable named ``Standard1-g-1-Data.db`` into ``Keyspace1/Standard1``, you will need to have the files ``Standard1-g-1-Data.db`` and ``Standard1-g-1-Index.db`` into a directory ``/path/to/Keyspace1/Standard1/``.

Parameters:

* ``-alg,--ssl-alg <ALGORITHM>`` - Client SSL: algorithm (default: SunX509)
* ``-bs,--batch-size <Number of bytes above which batch is being sent out>`` - Does not work with ``-nb``
* ``-ciphers,--ssl-ciphers <CIPHER-SUITES>`` - Client SSL: comma-separated list of encryption suites to use
* ``-cl,--consistency-level <consistency level (default: ONE)>`` - sets the consistency level for statements
* ``-cph,--connections-per-host <connectionsPerHost>`` - number of concurrent connections-per-host.
* ``-d,--nodes <initial hosts>`` - Required. try to connect to these hosts (comma separated) initially for ring information
* ``-f,--conf-path <path to config file>`` - cassandra.yaml file path for streaming throughput and client/server SSL.
* ``-g,--ignore-missing-columns <COLUMN NAMES...>`` - ignore named missing columns in tables
* ``-h,--help`` - display this help message
* ``-i,--ignore <NODES>`` - don't stream to this (comma separated) list of nodes
* ``-ic,--ignore-dropped-counter-data`` - ignore dropping local and remote counter shard data
* ``-ir,--no-infinite-retry`` - Disable infinite retry policy
* ``-j,--threads-count <Number of threads to execute tasks>`` - Run tasks in parallel
* ``-ks,--keystore <KEYSTORE>`` - Client SSL: full path to keystore
* ``-kspw,--keystore-password <KEYSTORE-PASSWORD>`` - Client SSL: password of the keystore
* ``-nb,--no-batch`` - Do not use batch statements updates for same partition key.
* ``--no-progress`` - don't display progress
* ``-nx,--no-prepared`` - Do not use prepared statements
* ``-p,--port <port>`` - port used for connections (default 9042)
* ``-prtcl,--ssl-protocol <PROTOCOL>`` - Client SSL: connections protocol to use (default: TLS)
* ``-pt,--partitioner <class>`` - Partitioner type to use, defaults to cluster value
* ``-pw,--password <password>`` - password for cassandra authentication
* ``-s,--ssl <SSL>`` - Use SSL connection(s)
* ``-sim,--simulate`` - simulate. Only print CQL generated
* ``-st,--store-type <STORE-TYPE>`` - Client SSL: type of store
* ``-t,--throttle <throttle>`` - throttle speed in Mbits (default unlimited)
* ``-tr,--token-ranges <<lo>:<hi>,...>`` - import only partitions that satisfy lo < token(partition) <= hi
* ``-translate,--translate <mapping list>`` - comma-separated list of column name mappings
* ``-ts,--truststore <TRUSTSTORE>`` - Client SSL: full path to truststore
* ``-tspw,--truststore-password <TRUSTSTORE-PASSWORD>`` - Client SSL: password of the truststore
* ``--use-unset`` - Use 'unset' values in prepared statements
* ``--username <username>`` - username for cassandra authentication
* ``-v,--verbose <LEVEL>`` - verbose output
