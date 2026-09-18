=================================
ScyllaDB Configuration Reference
=================================

This guide describes the commands that you can use to configure your Scylla clusters.
The commands are all sent via the command line in a terminal and sudo or root access is not required as long as you have permission to execute in the directory.

.. caution:: You should **only** use configuration settings which are officially supported.

A list of all Scylla commands can be obtained by running

.. code-block:: none

   scylla --help

.. note:: This command displays all Scylla commands as well as Seastar commands. The Seastar commands are listed as Core Options.

For example:

.. code-block:: none

   Scylla version 4.2.3-0.20210104.24346215c2 with build-id 0c8faf8bb8a3a0eda9337aad98ed3a6d814a4fa9 starting ...
   command used: "scylla --help"
   parsed command line options: [help]
   Scylla options:
     -h [ --help ]                         show help message
     --version                             print version number and exit
     --options-file arg                    configuration file (i.e.
                                           <SCYLLA_HOME>/conf/scylla.yaml)
     --memtable-flush-static-shares arg    If set to higher than 0, ignore the
                                           controller's output and set the
                                           memtable shares statically. Do not set
                                           this unless you know what you are doing
                                           and suspect a problem in the
                                           controller. This option will be retired
                                           when the controller reaches more
                                           maturity
     --compaction-static-shares arg        If set to higher than 0, ignore the
                                           controller's output and set the
                                           compaction shares statically. Do not
                                           set this unless you know what you are
                                           doing and suspect a problem in the
                                           controller. This option will be retired
                                           when the controller reaches more
                                           maturity

.. note:: This is an incomplete screenshot. For the complete file, run the command in a terminal.

Scylla Configuration Files and Scylla Commands
----------------------------------------------

Some Scylla Command Line commands are derived from the `scylla.yaml <https://github.com/scylladb/scylla/blob/master/conf/scylla.yaml>`_ configuration parameters.

For example, in the case of ``cluster_name: 'Test Cluster'`` as seen in the `scylla.yaml <https://github.com/scylladb/scylla/blob/master/conf/scylla.yaml>`_ configuration parameters.

To send this configuration setting with the command line, run:

.. code-block:: none

   scylla --cluster-name 'Test Cluster'


As you can see from the example above, the general rule of thumb is:

#. Take a configuration parameter from the scylla.yaml file.
#. Prepend it with ``scylla --``.
#. In any place where there is an underscore, replace with a dash.
#. Run the command in a terminal.

