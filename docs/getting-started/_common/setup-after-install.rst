Configure and Run ScyllaDB
-------------------------------

#. Configure the following parameters in the ``/etc/scylla/scylla.yaml`` configuration file.

   * ``cluster_name`` - The name of the cluster. All the nodes in the cluster must have the same 
     cluster name configured.
   * ``seeds`` - The IP address of the first node. Other nodes will use it as the first contact 
     point to discover the cluster topology when joining the cluster.
   * ``listen_address`` - The IP address that ScyllaDB uses to connect to other nodes in the cluster.
   * ``rpc_address`` - The IP address of the interface for CQL client connections.

#. Run the ``scylla_setup`` script to tune the system settings and determine the optimal configuration.

   .. code-block:: console
    
      sudo scylla_setup

   * The script invokes a set of :ref:`scripts <system-configuration-scripts>` to configure several operating system settings; for example, it sets 
     RAID0 and XFS filesystem. 
   * The script runs a short (up to a few minutes) benchmark on your storage and generates the ``/etc/scylla.d/io.conf`` 
     configuration file. When the file is ready, you can start ScyllaDB. ScyllaDB will not run without XFS 
     or ``io.conf`` file.
   * You can bypass this check by running ScyllaDB in :doc:`developer mode </getting-started/installation-common/dev-mod>`. 
     We recommend against enabling developer mode in production environments to ensure ScyllaDB's maximum performance.

#. Run ScyllaDB as a service (if not already running).

   .. code-block:: console
    
      sudo systemctl start scylla-server


Now you can start using ScyllaDB. Here are some tools you may find useful.

Run nodetool:
   
.. code-block:: console
     
     nodetool status

Run cqlsh:

.. code-block:: console
     
     cqlsh

Run cassandra-stress:

.. code-block:: console
     
     cassandra-stress write -mode cql3 native 


