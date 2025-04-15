Change the Log Level
====================

You have the option to change the log level either while the cluster is offline or during runtime. Each log level is assigned to a specific ScyllaDB class. To display the log classes (output changes with each version), run the following: 

.. code-block:: shell

   scylla --help-loggers


How to Change the Log Level without Downtime
--------------------------------------------

ScyllaDB presents the user with a variety of loggers that control the amount and detail of information printed to the system logs. This article contains information about how to query and change the log level of each individual logging system.


To obtain the status of a particular logger:

``curl -X GET http://127.0.0.1:10000/system/logger/<subsystem_name>``

For example: 

``curl -X GET http://127.0.0.1:10000/system/logger/sstable``

Example output 

``"info"``

To change the status of a particular logger:

``curl -X POST http://127.0.0.1:10000/system/logger/sstable?level=trace``

Valid log levels are: ``trace``, ``debug``, ``info``, ``warn``, ``error``.

Alternatively, you can use Nodetool commands. Refer to :doc:`setlogginglevel</operating-scylla/nodetool-commands/setlogginglevel>` to set the logging level threshold for ScyllaDB classes.

How to Change Log Level Offline
-------------------------------

In order to debug issues that occur during the ScyllaDB start, the procedure above will not be helpful. This procedure can be used to assess ScyllaDB Start issues, or it can be used when the cluster is down.  Note that any changes made here will only take effect after ScyllaDB starts. 

ScyllaDB has command line options you can invoke to set the log level. Once set, the change is implemented during the ScyllaDB start. Thus users can append new log level options by editing the SCYLLA_ARGS parameter in ``/etc/sysconfig/scylla-server``. 



**ScyllaDB Options**

.. list-table::
   :widths: 50 50
   :header-rows: 1

   * - ScyllaDB Options
     - Description
   * - ``--default-log-level arg (=info)``
     - Default log level for log messages. Valid values are trace, debug, info, warn, error.
   * - ``--logger-log-level arg``
     - Map of logger name to log level. The format is ``NAME0=LEVEL0[:NAME1=LEVEL1:...]``. Valid logger names can be queried with ``--help-loggers``. Valid values for log levels are ``trace``, ``debug``, ``info``, ``warn``, ``error``. This option can be specified multiple times.

**Syntax**

Log levels are assigned to a specific class for a specific log level in the following format:

single

.. code-block:: none
   
   class=level

multiple

.. code-block:: none
   
   class=level:class=level:class=level

**Examples**

To enable the debug log level for the sstable class and enable trace debug level for the cql_server class.

``--logger-log-level sstable=debug:cql_server=trace``

.. code-block:: shell

    sudo cat /etc/sysconfig/scylla-server|grep ^SCYLLA_ARGS=

    SCYLLA_ARGS="--log-to-syslog 1 --log-to-stdout 0 --default-log-level info --network-stack posix  --logger-log-level sstable=debug:cql_server=trace"

    ps aux|grep '/usr/bin/scylla' |grep -v grep

    scylla   32645  5.1  1.7 17180101468 67484 ?   Ssl  09:03   0:03 /usr/bin/scylla --log-to-syslog 1 --log-to-stdout 0 --default-log-level info --network-stack posix
             --logger-log-level sstable=debug:cql_server=trace --num-io-queues=2 --max-io-requests=8

    curl -X GET http://127.0.0.1:10000/system/logger/cql_server

    "trace"

    curl -X GET http://127.0.0.1:10000/system/logger/sstable

    "debug"

To enable the debug log level for all classes

``--default-log-level debug``


Related Topics
--------------

:doc:`Nodetool setlogginglevel </operating-scylla/nodetool-commands/setlogginglevel>`

