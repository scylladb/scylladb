* Make sure that all the :ref:`ports <cqlsh-networking>` are open.

* Obtain the IP addresses of all nodes that have been created for the cluster.

* Choose one of the nodes to be a seed node. You'll need to provide the IP 
  of that node using the seeds parameter in the scylla.yaml configuration file on each node.

* Select a unique name as ``cluster_name`` for the cluster (identical for all the nodes in the cluster).

* Choose which :ref:`snitch <faq-snitch-strategy>` to use (identical for all the nodes in the cluster). For a production system, it is recommended to use a DC-aware snitch, which can support the ``NetworkTopologyStrategy`` :ref:`replication strategy <create-keyspace-statement>` for your keyspaces.

