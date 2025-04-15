   
.. highlight:: none

.. _cqlsh:

CQLSh: the CQL shell
--------------------

cqlsh is a command line shell for interacting with ScyllaDB through CQL 
(the Cassandra Query Language). It is shipped with every ScyllaDB package 
and can be found in the ``bin/`` directory. In addition, it is available on 
`Docker Hub <https://hub.docker.com/r/scylladb/scylla-cqlsh>`_ and in 
the `Python Package Index (PyPI) <https://pypi.org/project/scylla-cqlsh/>`_.

cqlsh utilizes the Python native protocol driver and connects to the single 
node specified on the command line.

See the `scylla-cqlsh <https://github.com/scylladb/scylla-cqlsh>`_ repository 
on GitHub for usage examples.


Compatibility
^^^^^^^^^^^^^

cqlsh is compatible with Python 3.8 - Python 3.11.

A given version of cqlsh is only guaranteed to work with the version of ScyllaDB that it was released with.
cqlsh may work with older or newer versions of ScyllaDB without any guarantees.


Optional Dependencies
^^^^^^^^^^^^^^^^^^^^^

cqlsh ships with all essential dependencies.  However, there are some optional dependencies that can be installed to
improve the capabilities of cqlsh.

pytz
~~~~

By default, cqlsh displays all timestamps with a UTC timezone.  To support display of timestamps with another timezone,
the `pytz <http://pytz.sourceforge.net/>`__ library must be installed.  See the ``timezone`` option in cqlshrc_ for
specifying a timezone to use.

cython
~~~~~~

The performance of cqlsh's ``COPY`` operations can be improved by installing `cython <http://cython.org/>`__.  This will
compile the python modules that are central to the performance of ``COPY``.

cqlshrc
^^^^^^^


The ``cqlshrc`` file holds configuration options for cqlsh.  By default, this is in the user's home directory:

``~/.cassandra/cqlsh``, but a custom location can be specified with the ``--cqlshrc`` option.

Example config values and documentation can be found in the ``conf/cqlshrc.sample`` file of a tarball installation.  You
can also view the latest version of `cqlshrc online <https://github.com/scylladb/scylla-tools-java/blob/master/conf/cqlshrc.sample>`_.


Command Line Options
^^^^^^^^^^^^^^^^^^^^

Usage:

``cqlsh [options] [host [port]]``

Options:

``-C`` ``--color``
  Force color output

``--no-color``
  Disable color output

``--browser``
  Specify the browser to use for displaying cqlsh help.  This can be one of the `supported browser names
  <https://docs.python.org/2/library/webbrowser.html>`__ (e.g. ``firefox``) or a browser path followed by ``%s`` (e.g.
  ``/usr/bin/google-chrome-stable %s``).

``--ssl``
  Use SSL when connecting to ScyllaDB.

``-u`` ``--user``
  Username to authenticate against ScyllaDB.

``-p`` ``--password``
  The password to authenticate against ScyllaDB, which should
  be used in conjunction with ``--user``

``-k`` ``--keyspace``
  Keyspace to authenticate to should be used in conjunction
  with ``--user``

``-f`` ``--file``
  Execute commands from the given file, then exit

``--debug``
  Print additional debugging information

``--encoding``
  Specify a non-default encoding for output (defaults to UTF-8)

``--cqlshrc``
  Specify a non-default location for the ``cqlshrc`` file

``-e`` ``--execute``
  Execute the given statement, then exit

``--connect-timeout``
  Specify the connection timeout in seconds (defaults to 2s)

``--request-timeout``
  Specify the request timeout in seconds (defaults to 10s)

``-t`` ``--tty``
  Force tty mode (command prompt)


Special Commands
^^^^^^^^^^^^^^^^

In addition to supporting regular CQL statements, cqlsh also supports a number of special commands that are not part of
CQL.  These are detailed below.

.. _cqlsh-consistency:

CONSISTENCY
~~~~~~~~~~~

`Usage`: ``CONSISTENCY <consistency level>``

Sets the consistency level for operations to follow.  Valid arguments include:

.. code-block:: cql

   - ANY
   - ONE
   - TWO
   - THREE
   - QUORUM
   - ALL
   - LOCAL_QUORUM
   - LOCAL_ONE
   - SERIAL
   - LOCAL_SERIAL

.. _cqlsh-serial-consistency:

SERIAL CONSISTENCY
~~~~~~~~~~~~~~~~~~

`Usage`: ``SERIAL CONSISTENCY <consistency level>``

Sets the serial consistency level for operations to follow.  Valid arguments include:

- ``SERIAL``
- ``LOCAL_SERIAL``

The serial consistency level is only used by conditional updates (``INSERT``, ``UPDATE``, and ``DELETE`` with an ``IF``
condition). For those, the serial consistency level defines the consistency level of the serial phase (or “paxos” phase)
while the normal consistency level defines the consistency for the “learn” phase, i.e. what type of reads will be
guaranteed to see the update right away. For example, if a conditional write has a consistency level of ``QUORUM`` (and
is successful), then a ``QUORUM`` read is guaranteed to see that write. However, if the regular consistency level of that
write is ``ANY``, then only a read with a consistency level of ``SERIAL`` is guaranteed to see it (even a read with
consistency ``ALL`` is not guaranteed to be enough).

.. _cqlsh-show-version:

SHOW VERSION
~~~~~~~~~~~~
This command is useful if you want to check which Cassandra version is compatible with your ScyllaDB version.
Note that the two standards are not 100% identical and this command is simply a comparison tool.

If you want to display your current ScyllaDB version, refer to :ref:`Check your current version of ScyllaDB <check-your-current-version-of-scylla>`.

The display shows:

* The cqlsh tool version that you're using
* The Apache Cassandra version that your version of ScyllaDB is most compatible with
* The CQL protocol standard that your version of ScyllaDB is most compatible with
* The native protocol standard that your version of ScyllaDB is most compatible with

Example:

.. code-block:: none

   cqlsh> SHOW VERSION

Returns:

.. code-block:: none

   [cqlsh 5.0.1 | Cassandra 3.0.8 | CQL spec 3.3.1 | Native protocol v4]

.. _cqlsh-show-host:

SHOW HOST
~~~~~~~~~

Prints the IP address and port of the ScyllaDB node that cqlsh is connected to in addition to the cluster name.

Example:

.. code-block:: none

   cqlsh> SHOW HOST

Returns:

.. code-block:: none

   Connected to Prod_Cluster at 192.0.0.1:9042.

.. _cqlsh-show-session:

SHOW SESSION
~~~~~~~~~~~~

Pretty prints a specific tracing session.

`Usage`: ``SHOW SESSION <session id>``

Example usage:


.. code-block:: none

    cqlsh> SHOW SESSION 95ac6470-327e-11e6-beca-dfb660d92ad8

Returns:

.. code-block:: none

    Tracing session: 95ac6470-327e-11e6-beca-dfb660d92ad8

     activity                                                  | timestamp                  | source    | source_elapsed | client
    -----------------------------------------------------------+----------------------------+-----------+----------------+-----------
                                            Execute CQL3 query | 2016-06-14 17:23:13.979000 | 127.0.0.1 |              0 | 127.0.0.1
     Parsing SELECT * FROM system.local; [SharedPool-Worker-1] | 2016-06-14 17:23:13.982000 | 127.0.0.1 |           3843 | 127.0.0.1
    ...

.. _cqlsh-source:

SOURCE
~~~~~~

Reads the contents of a file and executes each line as a CQL statement or special cqlsh command.

`Usage`: ``SOURCE <string filename>``

Example usage::

    cqlsh> SOURCE '/home/thobbs/commands.cql'

.. _cqlsh-capture:

CAPTURE
~~~~~~~

Begins capturing command output and appending it to a specified file.  Output will not be shown at the console while it
is captured.

`Usage`::

    CAPTURE '<file>';
    CAPTURE OFF;
    CAPTURE;

That is, the path to the file to be appended to must be given inside a string literal. The path is interpreted relative
to the current working directory. The tilde shorthand notation (``'~/mydir'``) is supported for referring to ``$HOME``.

Only query result output is captured. Errors and output from cqlsh-only commands will still be shown in the cqlsh
session.

To stop capturing output and show it in the cqlsh session again, use ``CAPTURE OFF``.

To inspect the current capture configuration, use ``CAPTURE`` with no arguments.

.. _cqlsh-help:

HELP
~~~~

Gives information about cqlsh commands. To see available topics, enter ``HELP`` without any arguments. To see help on a
topic, use ``HELP <topic>``.  Also, see the ``--browser`` argument for controlling what browser is used to display help.

.. _cqlsh-tracing:

TRACING
~~~~~~~

Enables or disables tracing for queries.  When tracing is enabled, once a query completes, a trace of the events during
the query will be printed.

`Usage`::

    TRACING ON
    TRACING OFF

.. _cqlsh-paging:

PAGING
~~~~~~

Enables paging, disables paging, or sets the page size for read queries.  When paging is enabled, only one page of data
will be fetched at a time, and a prompt will appear to fetch the next page.  Generally, it's a good idea to leave paging
enabled in an interactive session to avoid fetching and printing large amounts of data at once.

`Usage`::

    PAGING ON
    PAGING OFF
    PAGING <page size in rows>

.. _cqlsh-expand:

EXPAND
~~~~~~

Enables or disables vertical printing of rows.  Enabling ``EXPAND`` is useful when many columns are fetched, or the
contents of a single column are large.

`Usage`::

    EXPAND ON
    EXPAND OFF

.. _cqlsh-login:

LOGIN
~~~~~

Authenticate as a specified ScyllaDB user for the current session.

`Usage`::

    LOGIN <username> [<password>]

.. _cqlsh-exit:

EXIT
~~~~

Ends the current session and terminates the cqlsh process.

`Usage`::

    EXIT
    QUIT

.. _cqlsh-clear:

CLEAR
~~~~~~

Clears the console.

`Usage`::

    CLEAR
    CLS

.. _cqlsh-describe:

DESCRIBE
~~~~~~~~

Prints a description (typically a series of DDL statements) of a schema element or the cluster.  This is useful for
dumping all or portions of the schema.

.. code-block:: cql

    DESCRIBE CLUSTER
    DESCRIBE SCHEMA
    DESCRIBE KEYSPACES
    DESCRIBE KEYSPACE <keyspace name>
    DESCRIBE TABLES
    DESCRIBE TABLE <table name>
    DESCRIBE MATERIALIZED VIEW <view name>
    DESCRIBE TYPES
    DESCRIBE TYPE <type name>
    DESCRIBE FUNCTIONS
    DESCRIBE FUNCTION <function name>
    DESCRIBE AGGREGATES
    DESCRIBE AGGREGATE <aggregate function name>

In any of the commands, ``DESC`` may be used in place of ``DESCRIBE``.

You may also omit what you want to describe and just simply use ``DESCRIBE <name>``. This will look for the object 
 in a specific order: keyspace, table, view, index, user-defined type, user-defined function, user-defined aggregate.

The ``DESCRIBE CLUSTER`` command prints the cluster name and partitioner::


    cqlsh> DESCRIBE CLUSTER

    Cluster: Test Cluster
    Partitioner: Murmur3Partitioner

The ``DESCRIBE SCHEMA`` command prints the DDL statements needed to recreate the entire schema. The higher tiers of the statement
can also include information that can be used to restore authentication/authorization and service levels. This is especially
useful for cloning a cluster or restoring its state from a backup.

See :doc:`a dedicated article </cql/describe-schema>` to learn more.

.. _cqlsh-copy-to:

COPY TO
~~~~~~~

Copies data from a table to a CSV file.

.. code-block:: cql

   COPY <table name> [(<column>, ...)] TO <file name> WITH <copy option> [AND <copy option> ...]

If no columns are specified, all columns from the table will be copied to the CSV file.  A subset of columns to copy may
be specified by adding a comma-separated list of column names surrounded by parenthesis after the table name.


The ``<file name>`` should be a string literal (with single quotes) representing a path to the destination file.  The
file name can also contain the special value ``STDOUT`` (without single quotes) print the CSV to STDOUT.

See :ref:`shared-copy-options` for options that apply to both ``COPY TO`` and ``COPY FROM``.

Options for COPY TO
```````````````````

``MAXREQUESTS``
  The maximum number token ranges to fetch simultaneously. Defaults to 6.

``PAGESIZE``
  The number of rows to fetch in a single page. Defaults to 1000.

``PAGETIMEOUT``
  By default, the page timeout is 10 seconds per 1000 entries
  in the page size or 10 seconds if pagesize is smaller.

``BEGINTOKEN``, ``ENDTOKEN``
  Token range to export.  Defaults to exporting the full ring.

``MAXOUTPUTSIZE``
  The maximum size of the output file measured in number of lines;
  beyond this maximum the output file will be split into segments.
  -1 means unlimited, and is the default.

``ENCODING``
  The encoding used for characters. Defaults to ``utf8``.

.. _cqlsh-copy-from:

COPY FROM
~~~~~~~~~
Copies data from a CSV file to table.

.. code-block:: cql

   COPY <table name> [(<column>, ...)] FROM <file name> WITH <copy option> [AND <copy option> ...]

If no columns are specified, all columns from the CSV file will be copied to the table.  A subset
of columns to copy may be specified by adding a comma-separated list of column names surrounded
by parenthesis after the table name.

The ``<file name>`` should be a string literal (with single quotes) representing a path to the
source file.  The file name can also contain the special value``STDIN`` (without single quotes) 
to read the CSV data from STDIN.

See :ref:`shared-copy-options` for options that apply to both ``COPY TO`` and ``COPY FROM``.

Options for COPY FROM
`````````````````````

``INGESTRATE``
  The maximum number of rows to process per second. Defaults to 100000.

``MAXROWS``
  The maximum number of rows to import. -1 means unlimited, and is the default.

``SKIPROWS``
  A number of initial rows to skip.  Defaults to 0.

``SKIPCOLS``
  A comma-separated list of column names to ignore.  By default, no columns are skipped.

``MAXPARSEERRORS``
  The maximum global number of parsing errors to ignore. -1 means unlimited, and is the default.

``MAXINSERTERRORS``
  The maximum global number of insert errors to ignore. -1 means unlimited.  The default is 1000.

``ERRFILE`` =
  A file to store all rows that could not be imported. By default, this is ``import_<ks>_<table>.err`` where ``<ks>`` is
  your keyspace and ``<table>`` is your table name.

``MAXBATCHSIZE``
  The max number of rows inserted in a single batch. Defaults to 20.

``MINBATCHSIZE``
  The min number of rows inserted in a single batch. Defaults to 2.

``CHUNKSIZE``
  The number of rows that are passed to child worker processes from the main process at a time. Defaults to 1000.

.. _shared-copy-options:

Shared COPY Options
```````````````````

Options that are common to both ``COPY TO`` and ``COPY FROM``.

``NULLVAL``
  The string placeholder for null values.  Defaults to ``null``.

``HEADER``
  For ``COPY TO``, controls whether the first line in the CSV output file will contain the column names.  For COPY FROM,
  specifies whether the first line in the CSV input file contains column names.  Defaults to ``false``.

``DECIMALSEP``
  The character that is used as the decimal point separator.  Defaults to ``.``.

``THOUSANDSSEP``
  The character that is used to separate thousands. Defaults to the empty string.

``BOOLSTYlE``
  The string literal format for boolean values.  By default this is ``True, False``.

``NUMPROCESSES``
  The number of child worker processes to create for ``COPY`` tasks.  Defaults to a max of 4 for ``COPY FROM`` and 16
  for ``COPY TO``.  However, at most (num_cores - 1), processes will be created.

``MAXATTEMPTS``
  The maximum number of failed attempts to fetch a range of data (when using ``COPY TO``) or insert a chunk of data
  (when using ``COPY FROM``) before giving up. Defaults to 5.

``REPORTFREQUENCY``
  How often status updates are refreshed in seconds.  By default, this is 0.25 seconds.

``RATEFILE``
  An optional file to export rate statistics to.  By default, this is disabled and statistics are not exported to a file.

See also:

CQLSH `lesson <https://university.scylladb.com/courses/data-modeling/lessons/basic-data-modeling-2/topic/cql-cqlsh-and-basic-cql-syntax/>`_ on ScyllaDB University

* :doc:`Apache Cassandra Query Language (CQL) Reference </cql/index>`

:doc:`Back </getting-started/index/>`

.. include:: /rst_include/apache-copyrights.rst
