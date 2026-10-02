How to Report a ScyllaDB Problem
==================================


In the event there is an issue you would like to report to ScyllaDB support, you need to submit logs and other files which help the support team diagnose the issue. Only the ScyllaDB support team members can read the data you share. 

In general, there are two types of issues:

* **ScyllaDB failure** - There is some kind of failure, possibly due to a connectivity issue, a timeout, or otherwise, where the ScyllaDB server or the ScyllaDB nodes are not working. These cases require you to send :ref:`ScyllaDB Doctor vitals and ScyllaDB logs <report-scylla-problem-scylla-doctor>`, as well as `Core Dump`_ files (if available), to ScyllaDB support.
* **ScyllaDB performance** - you have noticed some type of degradation of service with ScyllaDB reads or writes. If it is clearly a performance case and not a failure, refer to `Report a performance problem`_.

Once you have used our diagnostic tools to report the current status, you need to `Send files to ScyllaDB support`_ for further analysis.

Make sure the ScyllaDB system logs are configured properly to report info level messages: `install debug info <https://github.com/scylladb/scylla/wiki/How-to-install-scylla-debug-info/>`_.

.. note:: 
   If you are unsure which reports need to be included, `Open a support ticket or GitHub issue`_ and consult with the ScyllaDB team. 


.. _report-scylla-problem-scylla-doctor:

ScyllaDB Doctor
^^^^^^^^^^^^^^^

ScyllaDB Doctor is a troubleshooting tool that checks the node status regarding 
system requirements, configuration, and tuning. The collected information is 
output as a ``.vitals.json`` file and an archive file with ScyllaDB logs. 
You need to run the tool **on every node in the cluster**. 

#. Download ScyllaDB Doctor as a Linux package or a generic tarball:

   * Ubuntu/Debian (DEB): https://downloads.scylladb.com/downloads/scylla-doctor/deb/
   * RHEL/Rocky (RPM): https://downloads.scylladb.com/downloads/scylla-doctor/rpm/
   * Tarball: https://downloads.scylladb.com/downloads/scylla-doctor/tar/

#. Run ScyllaDB Doctor on every node in the cluster.

   * If you installed ScyllaDB Doctor with DEB or RPM, you can run it with 
     the ``scylla-doctor`` command.

   * If you downloaded the tarball, extract the ``scylla_doctor.pyz`` file and
     copy the file to all nodes in the cluster. Next, execute the following
     command from the directory where you copied ``scylla_doctor.pyz`` on each node:

     .. code:: shell

        sudo ./scylla_doctor.pyz --save-vitals <unique-host-id>.vitals.json
 
     Make sure you provide a unique host identifier in the filename, such as 
     the host IP.
     
   Running ScyllaDB Doctor will generate:
   
   * ``<unique-host-id>.vitals.json`` - ScyllaDB Doctor vitals
   * ``scylla_logs_<timestamp>.tar.gz`` - ScyllaDB logs

   **Authenticated Clusters**

   If CQL authentication is enabled on the cluster, you need to additionally
   provide CQL credentials with permissions to perform the ``DESCRIBE SCHEMA`` 
   command using the following parameters:

   .. code:: shell

      -sov CQL,user,<CQL user name> -sov CQL,password,<CQL password>

   ScyllaDB Doctor employs cqlsh installed on a given node using the provided 
   credentials. Make sure to set up any additional configuration required to 
   use cqlsh, such as TLS-related information, in the .cqlshrc file.

#. Collect the ``.vitals.json`` and log files from each node into a local
   directory with a name identifying your cluster and compress them into an archive.
   In the following example, the Linux ``tar`` command is used to compress 
   the files in the ``my_cluster_123`` directory:

   .. code:: shell

      tar czvf my_cluster_123_vitals.tgz my_cluster_123

#. Upload the archive using the instructions in the 
   :ref:`Send files to ScyllaDB support <report-problem-send-files-to-support>` 
   section.

.. _report-scylla-problem-core-dump:

Core Dump
^^^^^^^^^

When ScyllaDB fails, it creates a core dump which can later be used to debug the issue. The file is written to ``/var/lib/scylla/coredump``. If there is no file in the directory, see `Troubleshooting Core Dump`_.


Compress the core dump file
...........................

**Procedure**

1. The core dump file can be very large. Make sure to zip it using ``xz`` or similar. 

.. code-block:: shell

   xz -z core.21692

2. Upload the compressed file to upload.scylladb.com. See `Send files to ScyllaDB support`_.


Troubleshooting Core Dump
^^^^^^^^^^^^^^^^^^^^^^^^^

In the event the ``/var/lib/scylla/coredump`` directory is empty, the following solutions may help. Note that this section only handles some of the reasons why a core dump file is not created. It should be noted that in some cases where a core dump file fails to create not because it is in the wrong location or because the system is not configured to generate core dump files, but because the failure itself created an issue where the core dump file wasn't created or is not accessible. 

Operating System not set to generate core dump files
....................................................

If ScyllaDB restarts for some reason and there is no core dump file, the OS system daemon needs to be modified.

**Procedure**

1. Open the custom configuration file. ``/etc/systemd/coredump.conf.d/custom.conf``.

2. Refer to :ref:`generate core dumps <admin-core-dumps>` for details. 


.. note:: You will need spare disk space larger than that of ScyllaDB's RAM.


Core dump file exists, but not where you expect it to be
........................................................

If the ``scylla/coredump`` directory is empty even after you changed the custom configuration file, it might be that Automatic Bug Reporting Tool (ABRT) is running and all core dumps are pipelined directly to it.

**Procedure**

1. Check the ``/proc/sys/kernel/core_pattern`` file.
   If it contains something similar to ``|/usr/libexec/abrt-hook-ccpp %s %c %p %u %g %t %h %e 636f726500`` replace the contents with ``core``.

.. _report-performance-problem:

Report a performance problem
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
 
If you are experiencing a performance issue when using ScyllaDB, let us know and we can help. To save time and increase the likelihood of a speedy solution, it is important to supply us with as much information as possible.

Include the following information in your report:

* Complete :ref:`ScyllaDB Doctor Vitals <report-scylla-problem-scylla-doctor>`
* A `Server Metrics`_ Report 
* A `Client Metrics`_ Report
* The contents of your tracing data. See :ref:`Collecting Tracing Data <tracing-collecting-tracing-data>`.

Metrics Reports
...............

There are two types of metrics you need to collect: ScyllaDB Server and ScyllaDB Client (node). The ScyllaDB Server metrics can be displayed using an external monitoring service like `ScyllaDB Monitoring Stack <https://monitoring.docs.scylladb.com/>`_ or they can be collected using `scyllatop <http://www.scylladb.com/2016/03/22/scyllatop/>`_ and other commands.

.. note:: 
   It is highly recommended to use the ScyllaDB monitoring stack so that the Prometheus metrics collected can be shared. 

Server Metrics
~~~~~~~~~~~~~~

There are several commands you can use to see if there is a performance issue on the ScyllaDB Server. Note that checking the CPU load using ``top`` is not a good metric for checking ScyllaDB. 
Use ``scyllatop`` instead. 

.. note:: 
   To help the ScyllaDB support team assess your problem, it is best to pipe the results to a file which you can attach with ScyllaDB Doctor vitals and ScyllaDB logs.

1. Check the ``Send files to ScyllaDB supportgauge-load``. If the load is close to 100%, the bottleneck is ScyllaDB CPU. 

.. code-block:: shell

   scyllatop *gauge-load

2. Check if one of ScyllaDB core is busier than the others:

.. code-block:: shell

   sar -P ALL

3. Check the load on one CPU (0 in this example)

.. code-block:: shell

   perf top -C0 

4. Check if the disk utilization percentage is close to 100%. If yes, the disk might be the bottleneck.

.. code-block:: shell
   
   ostat -x 1`` to observe the disk utilization. 

5. Collect run time statistics.

.. code-block:: shell
   
   sudo perf record --call-graph dwarf -C 0 -F 99 -p $(ps -C scylla -o pid --no-headers) -g sleep 10  
   

Alternatively, you can run the ``sudo ./collect-runtime-info.sh`` ` which does all of the above, except scyllatop and uploads the compressed result to s3.

The script contents is  as follows:

.. code-block:: shell

   #!/bin/bash -e

   mkdir report
   rpm -qa > ./report/rpm.txt
   journalctl -b > ./report/journalctl.txt
   df -k > ./report/df.txt
   netstat > ./report/netstat.txt

   sar -P ALL > ./report/sar.txt
   iostat -d 1 10 > ./report/iostat.txt
   sudo perf record --call-graph dwarf -C 0 -F 99 -p $(ps -C scylla -o pid --no-headers) -g --output ./report/perf.data sleep 10

   export report_uuid=$(uuidgen)
   echo $report_uuid
   tar c report | xz > report.tar.xz
   curl --request PUT --upload-file report.tar.xz "scylladb-users-upload.s3.amazonaws.com/$report_uuid/report.tar.xz"
   echo $report_uuid

You can also see the results in `./report` dir

Server Metrics with Prometheus
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When using `Grafana and Prometheus to monitor ScyllaDB <https://github.com/scylladb/scylla-monitoring>`_, sharing the metrics stored in Prometheus is very useful. This procedure shows how to gather the metrics from the monitoring server.

**Procedure**

1. Validate Prometheus instance is running 

.. code-block:: shell

   docker ps

2. Download the DB, using your CONTAINER ID instead of a64bf3ba0b7f 

.. code-block:: shell

   sudo docker cp a64bf3ba0b7f:/prometheus /tmp/prometheus_data

3. Zip the file.

.. code-block:: shell

   sudo tar -zcvf /tmp/prometheus_data.tar.gz /tmp/prometheus_data/

4. Upload the file you created in step 3 to upload.scylladb.com (see `Send files to ScyllaDB support`_).


Client Metrics
~~~~~~~~~~~~~~
 
Check the client CPU using ``top``. If the CPU is close to 100%, the bottleneck is the client CPU. In this case, you should add more loaders to stress ScyllaDB.

.. _report-problem-send-files-to-support:

Send files to ScyllaDB support
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once you have collected and compressed your reports, send them to ScyllaDB for analysis. 

**Procedure**

.. _uuid:

1. Generate a UUID:

.. code-block:: shell

   export report_uuid=$(uuidgen) 
   echo $report_uuid

2. Upload **all required** report files:

.. code-block:: shell

   curl -X PUT https://upload.scylladb.com/$report_uuid/yourfile -T yourfile


For example with the Scylla Doctor's vitals:

  
.. code-block:: shell
 
   curl -X PUT https://upload.scylladb.com/$report_uuid/my_cluster_123_vitals.tgz -T my_cluster_123_vitals.tgz


The **UUID** you generated replaces the variable ``$report_uuid`` at runtime. ``yourfile`` is any file you need to send to ScyllaDB support.


Open a support ticket or GitHub issue
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
If you have not done so already, supply ScyllaDB support with the UUID. Keep in mind that although the ID you supply is public, only ScyllaDB support team members can read the data you share. In the ticket/issue you open, list the documents you have uploaded.

**Procedure**

1. Do *one* of the following:

* If you are a ScyllaDB customer, open a `Support Ticket`_ and **include the UUID** within the ticket.

.. _Support Ticket: http://scylladb.com/support


* If you are a ScyllaDB user, open an issue on `GitHub`_ and **include the UUID** within the issue.

.. _GitHub: https://github.com/scylladb/scylla/issues/new


See Also
........

`ScyllaDB benchmark results <http://www.scylladb.com/technology/cassandra-vs-scylla-benchmark-cluster-1/>`_ for an example of the level of details required in your reports.
