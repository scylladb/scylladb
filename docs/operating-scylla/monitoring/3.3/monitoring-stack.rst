===============================
Install Scylla Monitoring Stack
===============================


This document describes the setup of Scylla Monitoring Stack, based on :doc:`Scylla Prometheus API <monitoring-apis>`.

The Scylla Monitoring stack needs to be installed on a dedicated server, external to the Scylla cluster. Make sure the Scylla Monitoring server has access to the Scylla nodes to pull the metrics over the Prometheus API.

For evaluation, you can run the Scylla Monitoring stack on any server (or laptop) that can handle three Docker instances simultaneously. For production, see the recommendations below.

.. include:: min-prod-hw.rst

Prerequisites
-------------

* Follow the Installation Guide and install `docker`_ on the Scylla Monitoring Server. This server can be the same server that is running the Scylla Manager. Alternatively, you can :doc:`Deploy Scylla Monitoring Without Docker <monitor-without-docker>`.

.. _`docker`: https://docs.docker.com/install/

Docker Post Installation
------------------------

Docker post installation guide can be found `here`_

.. _`here`: https://docs.docker.com/install/linux/linux-postinstall/

.. note::

   Avoid running the container as root.

To avoid running docker as root, you should add the user you are going to use for monitoring purposes to the Docker group.

1. Create a Docker group.

.. code-block:: sh

   sudo groupadd docker

2. Add your user to the Docker group.

.. code-block:: sh

   sudo usermod -aG docker $USER

3. Start Docker by calling:

.. code-block:: sh

   sudo systemctl enable docker

Install Scylla Monitoring
-------------------------

**Procedure**

1. Download and extract the latest `Scylla Monitoring Stack binary`_;

.. _`Scylla Monitoring Stack binary`: https://github.com/scylladb/scylla-monitoring/releases

.. code-block:: sh

   wget https://github.com/scylladb/scylla-monitoring/archive/scylla-monitoring-3.3.tar.gz
   tar -xvf scylla-monitoring-3.3.tar.gz
   cd scylla-monitoring-scylla-monitoring-3.3

As an alternative, you can clone and use the Git repository directly.

.. code-block:: sh

   git clone https://github.com/scylladb/scylla-monitoring.git
   cd scylla-monitoring
   git checkout branch-3.3

2. Start Docker service if needed

.. code-block:: sh

   sudo systemctl restart docker

Configure Scylla Monitoring
---------------------------

To monitor the cluster, Scylla Monitor (Specifically the Prometheus Server) needs to know the IP of all the nodes and the IP of the Scylla Manager Server (if you are using Scylla Manager).

This configuration can be done from files or using the Consul_ api.

.. _Consul: https://www.consul.io/


Sylla Manager 2.0 and higher supports the Consul API.

Configure Scylla nodes from files
.................................


1. Create ``prometheus/scylla_servers.yml`` with the targets' IPs (the servers you wish to monitor).

.. note::
   It is important that the name listed in ``dc`` in the ``labels`` matches the datacenter names used by Scylla.
   Use the ``nodetool status`` command to validate the datacenter names used by Scylla.

For example:

.. code-block:: yaml

   - targets:
         - 172.17.0.2
         - 172.17.0.3
     labels:
         cluster: cluster1
         dc: dc1

.. note:: If you want to add your managed cluster to Scylla Monitoring, add the IPs of the nodes as well as the cluster name you used when you added the cluster to Scylla Manager. It is important that the label ``cluster name`` and the cluster name in the Scylla Manager match.

*Using IPV6*

To use IPv6 inside scylla_server.yml, add the IPv6 addresses with their square brackets and the port numbers.

For example:

.. code-block:: yaml

   - targets:
         - "[2600:1f18:26b1:3a00:fac8:118e:9199:67b9]:9180"
         - "[2600:1f18:26b1:3a00:fac8:118e:9199:67ba]:9180"
     labels:
         cluster: cluster1
         dc: dc1

.. note:: For IPv6 to work, both Scylla Prometheus address and node_exporter's `--web.listen-address` should be set to listen to an IPv6 address.


For general node information (disk, network, etc.) Scylla Monitoring Stack uses the ``node_exporter`` agent that runs on the same machine as Scylla does.
By default, Prometheus will assume you have a ``node_exporter`` running on each machine. If this is not the case, you can override the ``node_exporter``
targets configuration file by creating an additional file and passing it with the ``-n`` flag.

.. note::
   By default, there is no need to create ``node_exporter_server.yml``. Prometheus will use the same targets it uses for
   Scylla, and will assume you have a ``node_exporter`` running on each Scylla server.


If needed, you can set your own target file instead of the default ``prometheus/scylla_servers.yml``, using the ``-s`` for Scylla target files.

For example:

.. code-block:: yaml

   ./start-all.sh -s my_scylla_server.yml -d data_dir


Mark the different Data Centers with Labels.

As can be seen in the examples, each target has its own set of labels to mark the cluster name and the data center (dc).
You can add multiple targets in the same file for multiple clusters or multiple data centers.

You can use the ``genconfig.py`` script to generate the server file. For example:

.. code-block:: yaml

   ./genconfig.py -d myconf -dc dc1:192.168.0.1,192.168.0.2 -dc dc2:192.168.0.3,192.168.0.4

This will generate a server file for four servers in two datacenters server ``192.168.0.1`` and ``192.168.0.2`` in dc1 and ``192.168.0.3`` and ``192.168.0.4`` in dc2.

OR

The ``genconfig.py`` script can also use ``nodetool status`` to generate the server file using the ``-NS`` flag.

.. code-block:: yaml

   nodetool status | ./genconfig.py -NS


2. Connect to `Scylla Manager`_ by creating ``prometheus/scylla_manager_servers.yml``
If you are using the Scylla Manager, you should set its IP.

You must add a scylla_manager_servers.yml file even if you are not using the manager.
You can look at: ``prometheus/scylla_manager_servers.example.yml`` for an example.

..  _`Scylla Manager`: /operating-scylla/manager/

For example

.. code-block:: yaml

   # List Scylla Manager end points

   - targets:
     - 172.17.0.7:56090

Note that you do not need to add labels to the Scylla Manager targets.

Configure Scylla nodes using Scylla-Manager Consul API
......................................................

Scylla Manager 2.0 has a Consul_ like API.

.. _Consul: https://www.consul.io/


When using the manager as the configuration source, there is no need to set any of the files.
Instead, you should set the scylla-manager IP from the command line using the `-L` flag.

For example:

.. code-block:: yaml

   ./start-all.sh -L 10.10.0.1


.. note::
   If you are running Scylla-Manager on the same host as Scylla-Monitoring you should use -l flag so that the localhost address
   will be available from within the container.

Start and Stop Scylla Monitoring Stack
--------------------------------------

Start
.....

.. code-block:: yaml

   ./start-all.sh -d data_dir


Stop
....

.. code-block:: yaml

   ./kill-all.sh


Start a Specific Scylla Monitoring Stack  Version
.................................................

By default, start-all.sh will start with dashboards for the latest two Scylla versions and the latest Scylla Manager version.

You can specify specific Scylla version with the ``-v`` flag and Scylla Manager version with ``-M`` flag

For example:

.. code-block:: sh

   ./start-all.sh -v 3.1,master -M 2.0 -d /promethes-data

will load the dashboards for Scylla versions ``3.1`` and ``master`` and the dashboard for Scylla Manager ``2.0``


Accessing the `localhost`
.........................

The Prometheus server runs inside a Docker container if it needs to reach a target on the local- host: either Scylla or Scylla-Manager. It needs to use the host network and not the Docker network.
To do that run ./start-all.sh with the -l flag. For example:

.. code-block:: sh

   ./start-all.sh -l -d /promethes-data



View Grafana Dashboards
-----------------------

Point your browser to ``your-server-ip:3000``
By default, Grafana authentication is disabled. To enable it and set a password for user admin, use the ``-a`` option.



