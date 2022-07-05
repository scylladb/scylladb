Troubleshoot Monitoring
========================


This document describes steps that needed to be done to troubleshoot monitoring problems when using `Grafana/Prometheus`_ monitoring tool.

..  _`Grafana/Prometheus`: /monitoring_apis/

Problem
~~~~~~~


A Container Failed To Start
^^^^^^^^^^^^^^^^^^^^^^^^^^^

When running ``./start-all.sh`` a container can fail to start. for example you can see something like:

.. code-block:: shell

   Wait for Prometheus container to start........Error: Prometheus container failed to start


Check the docker logs for more information.

.. code-block:: shell

   docker logs aprom

Usually the reason for the failure will be in the logs.

Files And Directory Permissions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


.. note::

   Avoid running container as root.

The preferred way of running the container is using a non root user.
see the :doc:`monitoring <monitoring-stack>` docker post installation section.

If a container failed to start because of a permission problem make sure
the external directory you are using is owned by the current user and have enough permissions.

.. note::

   If you started the container in the past as root you may need to change the directory and files ownership and permissions.

For example if your Prometheus data directory is ``/prom-data`` and you are using ``centos`` user

.. code-block:: shell

   ls -la /|grep prom-data

   drwxr-xr-x    2 root root  4096 Jun 25 17:51 prom-data

   sudo chown -R centos:centos /prom-data

   ls -la /|grep prom-data

   drwxr-xr-x    2 centos centos  4096 Jun 25 17:51 prom-data



No Data Points
^^^^^^^^^^^^^^

``No data points`` on all data charts.

Solution
........
If there are no data points, or if a node appears to be unreachable when you know it is, the immediate suspect is the Prometheus connectivity.

Start by login to the Prometheus console:

point your browser to ``http://{ip}:9090`` ({ip} is the Prometheus ip address).

Go to the target tabs: ``http://{ip}:9090/targets`` and see if any of the targets is down and if there is an error message.

* Not using the local network for local ip range

When using Docker containers, by default, the local ip range (127.0.0.X) is inside the Docker container and not the host local address,
if you are trying to connect to a target via the local ip range from inside a Docker container, you need to use the ``-l`` flag to enable local network stack.


* Prometheus may be pointing to the wrong target. Check your ``prometheus/scylla_servers.yml``. Make sure Prometheus is pulling data from the Scylla server.

Or

* Your dashboard and Scylla version may not be aligned. If you are running Scylla 3.1.x, you can specify a specific version with ``-v`` flag.

For example:

.. code-block:: shell

   ./start-all.sh -v 3.1

More on start-all.sh `options`_.

..  _`options`: /monitoring_stack/


Grafana Chart Shows Error (!) Sign
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Run this procedure on the Monitoring server.

All of Grafana chart shows error (!) sign.
There is a problem with the connection between Grafana and Prometheus. On the monitoring server:

Solution
.........

1. Check Prometheus is running using ``docker ps``.
If it is not running check the ``prometheus.yml`` for errors.

For example:

.. code-block:: shell

   CONTAINER ID  IMAGE    COMMAND                  CREATED         STATUS         PORTS                                                    NAMES
   41bd3db26240  monitor  "/docker-entrypoin..."   25 seconds ago  Up 23 seconds  7000-7001/tcp, 9042/tcp, 9160/tcp, 9180/tcp, 10000/tcp   monitor

2. If it is running, go to "Data Source" in the Grafana GUI, choose Prometheus and click Test Connection.

Grafana Shows Server Level Metrics, but not Scylla Metrics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Grafana shows server level metrics like disk usage, but not Scylla metrics.
Prometheus fails to fetch metrics from Scylla servers.

Solution
.........

* use ``curl <scylla_node>:9180/metrics`` to fetch binary metric data from Scylla.  If curl does not return data, the problem is the connectivity between the monitoring and Scylla server. Please check your IPs and firewalls.

For example

.. code-block:: shell

   curl 172.17.0.2:9180/metrics

Grafana Shows Scylla Metrics, but not Server Level Metrics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Grafana dashboard shows Scylla metrics, such as load, but not server metrics like disk usage.
Prometheus fail to fetch metrics from ``node_exporter``.

Solution
.........

1. Make sure ``node_exporter`` is running on each Scylla server. ``node_exporter`` is installed by ``scylla_setup``.
If it does not, make sure to install and run it.

2. If it is running, use ``curl <scylla_node>:9100/metrics`` (where 172.17.0.2 is a Scylla server IP) to fetch binary metric data from Scylla.  If curl does not return data, the problem is the connectivity between the monitoring and Scylla server. Please check your IPs and firewalls.

Notice to users upgrading to Scylla Open Source 3.0 or Scylla Enterprise 2019.1
................................................................................

While upgrading you need to upgrade the ``node_exporter`` from 0.14 to 0.17 version.

If the node_exporter service is not starting it may be that it needs to be updated manually.

Check the node_exporter version ``node_exporter --version`` if it shows 0.14 check the node_exporter section
in the `upgrade guide`_.

.. _`upgrade guide`: /upgrade/upgrade-opensource/upgrade-guide-from-2.3-to-3.0/



Working with wire-shark
^^^^^^^^^^^^^^^^^^^^^^^

No metrics shown in Scylla monitor.

1. Install `wire-shark`_

..  _`wire-shark`: https://www.wireshark.org/#download

2. Capture the traffic between Scylla monitor and Scylla node using the ``tshark`` command.
``tshark -i <network interface name> -f "dst port 9180"``

For example:

.. code-block:: shell

   tshark -i eth0 -f "dst port 9180"

Capture from Scylla node towards Scylla monitor server.

Scylla is running.

.. code-block:: shell

   Monitor ip        Scylla node ip
   199.203.229.89 -> 172.16.12.142 TCP 66 59212 > 9180 [ACK] Seq=317 Ack=78193 Win=158080 Len=0 TSval=79869679 TSecr=3347447210

Scylla is not running

.. code-block:: shell

   Monitor ip        Scylla node ip
   199.203.229.89 -> 172.16.12.142 TCP 74 60440 > 9180 [SYN] Seq=0 Win=29200 Len=0 MSS=1460 SACK_PERM=1 TSval=79988291 TSecr=0 WS=128


