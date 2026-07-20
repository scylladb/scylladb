Nodetool snapshot
=================

NAME
....

**snapshot** - Take a snapshot of specified keyspaces or a snapshot of the specified table

SYNOPSIS
........

.. code-block:: shell

              nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
                   [(-pp | --print-port)] [(-pw <password> | --password <password>)]
                   [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]
                   [(-u <username> | --username <username>)] snapshot
                   [(-cf <table> | --column-family <table> | --table <table>)]
                   [(-kc <kclist> | --kc.list <kclist>)]
                   [(-sf | --skip-flush)] [(-t <tag> | --tag <tag>)] [--] [<keyspaces...>]

OPTIONS
.......

====================================================================  =====================================================================================
Parameter                                                             Description
====================================================================  =====================================================================================
-cf <table> / --column-family <table>, --table <table>                The table name (you must specify one and only one keyspace for using this option)
--------------------------------------------------------------------  -------------------------------------------------------------------------------------
-h <host> / --host <host>                                             Node hostname or ip address
--------------------------------------------------------------------  -------------------------------------------------------------------------------------
-kc <ktlist>, --kc.list <ktlist>                                      The list of Keyspaces to take snapshot
--------------------------------------------------------------------  -------------------------------------------------------------------------------------
-p <port> / --port <port>                                             Remote jmx agent port number
--------------------------------------------------------------------  -------------------------------------------------------------------------------------
-sf / --skip-flush                                                    Do not flush memtables before snapshotting (snapshot will not contain unflushed data)
--------------------------------------------------------------------  -------------------------------------------------------------------------------------
-t <tag> / --tag <tag>                                                The name of the snapshot
====================================================================  =====================================================================================

``--`` This option can be used to separate command-line options from the list of argument, (useful when arguments might be mistaken for command-line options.

``[<keyspaces...>]`` List of keyspaces. By default, all keyspaces.

Examples
........

Snapshot **all** the keyspaces

.. code-block:: shell

   > nodetool snapshot
   Requested creating snapshot(s) for [all keyspaces] with snapshot name [1564577645291] and options {skipFlush=false}
   Snapshot directory: 1564577645291
    

Snapshot **a** keyspace (mykeyspace)

.. code-block:: shell

   > nodetool snapshot mykeyspace;
   Requested creating snapshot(s) for [mykeyspace] with snapshot name [1564577611409] and options {skipFlush=false}
   Snapshot directory: 1564577611409

Snapshot a **list** of keyspaces
   
.. code-block:: shell

   >nodetool snapshot -kc mykeyspace,yourkeyspace
   Requested creating snapshot(s) for [mykeyspace,yourkeyspace] with snapshot name [1564639559884] and options {skipFlush=false}
   Snapshot directory: 1564639559884

Snapshot **a** table (heartrate)
  
.. code-block:: shell

   > nodetool snapshot --table heartrate mykeyspace;
   Requested creating snapshot(s) for [mykeyspace] with snapshot name [1564577586524] and options {skipFlush=false}
   Snapshot directory: 1564577586524


Snapshot location
.................

The snapshot is created in a ``snapshot`` directory in the table SSTable data directory. For example, repeating the heartrate command above three times will create three snapshots for table heartrate in keyspace mykeyspace

.. code-block:: shell

   > ls /var/lib/scylla/data/mykeyspace/heartrate-07669030b39211e9a057000000000000/snapshots/
   1564577586524  1564577611409  1564577645291

Each of the snapshots is a **hardlink** to to the SSTable directory.

.. code-block:: shell

   > ls -1a /var/lib/scylla/data/mykeyspace/heartrate-07669030b39211e9a057000000000000/snapshots/1564577586524
   la-1-big-CompressionInfo.db
   la-1-big-Data.db
   la-1-big-Digest.sha1
   la-1-big-Filter.db
   la-1-big-Index.db
   la-1-big-Scylla.db
   la-1-big-Statistics.db
   la-1-big-Summary.db
   la-1-big-TOC.txt
   manifest.json

Additional Resources
^^^^^^^^^^^^^^^^^^^^

* :doc:`Backup your data </operating-scylla/procedures/backup-restore/backup>`
* :doc:`Scylla Snapshots </kb/snapshots>`

.. include:: /rst_include/apache-copyrights.rst
