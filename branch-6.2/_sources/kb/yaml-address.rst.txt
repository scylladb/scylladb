
Configure ScyllaDB Networking with Multiple NIC/IP Combinations
===============================================================

There are many ways to configure IP addresses in scylla.yaml. Setting the IP addresses incorrectly, can yield less than optimal results. This article focuses on configuring the addresses which are vital to network communication. 

This article contains examples of the different ways to configure networking in scylla.yaml. The entire scope for address configuration is in the :ref:`Admin guide <admin-address-configuration-in-scylla>`. 

As these values depend on a particular network configuration in your setup there are a few ways to configure the address parameters. In the examples below, we will provide instructions for the most common use cases (all in the resolution of a single ScyllaDB node).

1 NIC, 1 IP
-----------

This is the case where a ScyllaDB cluster is meant to operate in a single subnet with a single address space (no "public/internal IP"s).

In this case:

* IP = node's IP address 

.. code-block:: none

   listen_address: IP
   rpc_address: IP
   broadcast_address: not set
   broadcast_rpc_address: not set
   endpoint_snitch: no restrictions

1 NIC, 2 IPs
------------

This is the case where you are using Public and Internal IP addresses. AWS instances with public IP addresses is a classic example of this configuration.

In this case:

* IPp = the node's public IP address
* IPi = the node's internal IP address

IPi is the IP configured to the same IP as the NIC (from the OS perspective) and when running the ip command, the output displays this IP address.  For ip command parameters, refer to your Linux distro documentation.

.. code-block:: none

   listen_address: IPi
   rpc_address: IPi
   broadcast_address: IPp
   broadcast_rpc_address: IPp
   endpoint_snitch: GossipintPropertyFileSnitch with prefer_local=true | any of Ec2xxxSnitch snitches.

2 NICs, 2 IPs
-------------

This is the case where a user wants CQL requests or responses to be sent via one subnet (net1) and inter-node communication to go over another subnet (net2).

In this case:

* IP1 = node's IP in net1 subnet
* IP2 = node's IP in net2 subnet

.. code-block:: none

   listen_address: IP2
   rpc_address: IP1
   broadcast_address: not set
   broadcast_rpc_address: not set
   endpoint_snitch: no restrictions


Additional References
---------------------

:doc:`Administration Guide </operating-scylla/admin>` - User guide for ScyllaDB Administration

