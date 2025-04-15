scylla.yaml
===========

scylla.yaml is equivalent to the Apache Cassandra cassandra.yaml configuration file and it is compatible with relevant parameters. Below is a **subset** of scylla.yaml with parameters you are likely to update. For a full list of parameters, look at the file itself.

.. code-block:: yaml

   # The name of the cluster. This is mainly used to prevent machines in
   # one logical cluster from joining another.
   cluster_name: 'Test Cluster'

   # This defines the number of tokens randomly assigned to this node on the ring
   # The more tokens, relative to other nodes, the larger the proportion of data
   # that this node will store. You probably want all nodes to have the same number
   # of tokens assuming they have equal hardware capability.
   num_tokens: 256

   # Directory where Scylla should store data on disk.
   data_file_directories:
       - /var/lib/scylla/data

   # commit log.  when running on magnetic HDD, this should be a
   # separate spindle than the data directories.
   commitlog_directory: /var/lib/scylla/commitlog

   # schema commit log. A special commitlog instance
   # used for schema and system tables.
   # When running on magnetic HDD, this should be a
   # separate spindle than the data directories.
   # schema_commitlog_directory: /var/lib/scylla/commitlog/schema

   # seed_provider class_name is saved for future use.
   # A seed address is mandatory.
   seed_provider:
       # The addresses of hosts that will serve as contact points for the joining node.
       # It allows the node to discover the cluster ring topology on startup (when 
       # joining the cluster).
       # Once the node has joined the cluster, the seed list has no function.
       - class_name: org.apache.cassandra.locator.SimpleSeedProvider
         parameters:
             # In a new cluster, provide the address of the first node.
             # In an existing cluster, specify the address of at least one existing node.
             # If you specify addresses of more than one node, use a comma to separate them.
             # For example: "<IP1>,<IP2>,<IP3>"
             - seeds: "127.0.0.1"

   # Address or interface to bind to and tell other Scylla nodes to connect to.
   # You _must_ change this if you want multiple nodes to be able to communicate!
   #
   # Setting listen_address to 0.0.0.0 is always wrong.
   listen_address: localhost

   # Address to broadcast to other Scylla nodes
   # Leaving this blank will set it to the same value as listen_address
   # broadcast_address: 1.2.3.4

   # port for the CQL native transport to listen for clients on
   # For security reasons, you should not expose this port to the internet.  Firewall it if needed.
   native_transport_port: 9042

   # Uncomment to enable experimental features
   # experimental: true

By default scylla.yaml is located at :code:`/etc/scylla/scylla.yaml`. Note that the file will open as read-only unless you edit it as the root user or by using sudo. 
 

scylla.yaml Required Settings
-----------------------------
The following configuration items must be set

==============  ==================================================  
Item	        Content                                             
==============  ==================================================  
cluster_name	Name of the cluster, all the nodes in the cluster must have the same name  
--------------  --------------------------------------------------  
seeds           IP address of an existing node in the cluster. It allows a new node to discover the cluster ring topology when joining the cluster.	
--------------  --------------------------------------------------  
listen_address  IP address that the Scylla use to connect to other Scylla nodes in the cluster
--------------  --------------------------------------------------  
rpc_address     IP address of the interface for CQL client connections
==============  ==================================================  

.. _yaml_enabling_experimental_features:

Enabling Experimental Features
------------------------------

There are two ways to enable experimental features:

* Enable all experimental features (which may be risky)
* Enable only the features you want to try (available in Scylla Open Source from version 3.2)

Enable All Experimental Features
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To enable all experimental features to add to the scylla.yaml:

.. code-block:: yaml

   experimental: true

Enable Specific Experimental Features
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To enable specific experimental features, add to the scylla.yaml the list of experimental features you want to enable, by setting the experimental_features array.
The list of valid elements for this array can be obtained from ``scylla --help``.
Note that this list is version specific. Your results may be different.
For example:

.. code-block:: yaml

   experimental_features:
    - cdc
    - lwt

.. _ipv6_addresses:

IPv6 Addresses
--------------

You can use IPv6 addresses wherever an IPv4 address is used, including client-to-node and node-to-node communication, Scylla Manager to Scylla nodes (Scylla Manager Agent), and Monitoring to nodes.

For example:

.. code-block:: yaml

   - seeds: "2a05:d018:223:f00:971d:14af:6418:fe2d"
   - listen_address: 2a05:d018:223:f00:971d:14af:6418:fe2d
   - broadcast_rpc_address: 2a05:d018:223:f00:971d:14af:6418:fe2d

To enable IPv6 addressing, set the following parameter in scylla.yaml:

.. code-block:: yaml

   enable_ipv6_dns_lookup: true

To read the rest of the :doc:`Administration Guide </operating-scylla/admin>` (from the top).  

To go to the System Configuration documentation, click :doc:`here </operating-scylla/system-configuration/index>`.




