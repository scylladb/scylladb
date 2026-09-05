Replace a Dead Node in a Scylla Cluster 
***************************************

Replace dead node operation will cause the other nodes in the cluster to stream data to the node that was replaced. This operation can take some time (depending on the data size and network bandwidth).

This procedure is for replacing one dead node. To replace more than one dead node, run the full procedure to completion one node at a time.

-------------
Prerequisites
-------------

* Verify the status of the node using :doc:`nodetool status </operating-scylla/nodetool-commands/status>` command, the node with status DN is down and need to be replaced

.. code-block:: shell

   Datacenter: DC1
   Status=Up/Down
   State=Normal/Leaving/Joining/Moving
   --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
   UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
   UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1
   DN  192.168.1.203  124.42 KB  256     32.6%             675ed9f4-6564-6dbd-can8-43fddce952gy   B1

.. warning::

   It's essential to ensure the replaced (dead) node will **never** come back to the cluster, which might lead to a split-brain situation.
   Remove the replaced (dead) node from the cluster network or VPC.

* Log in to the dead node and manually remove the data if you can. Delete the data with the following commands:

   .. include:: /rst_include/clean-data-code.rst

* Login to one of the nodes in the cluster with (UN) status. Collect the following info from the node:

  * cluster_name - ``cat /etc/scylla/scylla.yaml | grep cluster_name``
  * seeds - ``cat /etc/scylla/scylla.yaml | grep seeds:``
  * endpoint_snitch - ``cat /etc/scylla/scylla.yaml | grep endpoint_snitch``
  * Scylla version - ``scylla --version``

---------
Procedure
---------
.. note::
   If your Scylla version is earlier than Scylla Open Source 4.3 or Scylla Enterprise 2021.1, check if 
   the dead node is a seed node by running ``cat /etc/scylla/scylla.yaml | grep seeds:``.
   
   * If the dead node’s IP address is listed, the dead node is a seed node. Replace the seed node following the instructions in :doc:`Replacing a Dead Seed Node </operating-scylla/procedures/cluster-management/replace-seed-node>`.
   * If the dead node’s IP address is not listed, the dead node is not a seed node. Replace it according to the procedure below.

   We recommend checking the seed nodes configuration of all nodes. Refer to :doc:`Seed Nodes</kb/seed-nodes>` for details


#. Install Scylla on a new node, see :doc:`Getting Started</getting-started/index>` for further instructions. Follow the Scylla install procedure up to ``scylla.yaml`` configuration phase. Ensure that the Scylla version of the new node is identical to the other nodes in the cluster. 

    .. include:: /operating-scylla/procedures/cluster-management/_common/match_version.rst

#. In the ``scylla.yaml`` file edit the parameters listed below. The file can be found under ``/etc/scylla/``.

    - **cluster_name** - Set the selected cluster_name
 
    - **listen_address** - IP address that Scylla uses to connect to other Scylla nodes in the cluster

    - **seeds** - Set the seed nodes

    - **auto_bootstrap** - By default, this parameter is set to true, it allows new nodes to migrate data to themselves automatically

    - **endpoint_snitch** - Set the selected snitch

    - **rpc_address** - Address for client connection (Thrift, CQL)

#. Add the ``replace_address_first_boot`` parameter to the ``scylla.yaml`` config file on the new node. This line can be added to any place in the config file. After a successful node replacement, there is no need to remove it from the ``scylla.yaml`` file. (Note: The obsolete parameter "replace_address" is not supported and should not be used). The value of the ``replace_address_first_boot`` parameter should be the IP address of the node to be replaced.

    For example (using the address of the failed node from above):

    ``replace_address_first_boot: 192.168.1.203``   

#. Start Scylla node.

    .. include:: /rst_include/scylla-commands-start-index.rst

#. Verify that the node has been added to the cluster using ``nodetool status`` command.

    For example:
    
    .. code-block:: shell
    
       Datacenter: DC1
       Status=Up/Down
       State=Normal/Leaving/Joining/Moving
       --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
       UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
       UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1
       DN  192.168.1.203  124.42 KB  256     32.6%             675ed9f4-6564-6dbd-can8-43fddce952gy   B1
    
    ``192.168.1.203`` is the dead node.
    
    The replacing node ``192.168.1.204`` will be bootstrapping data.
    We will not see ``192.168.1.204`` during the bootstrap.

    .. code-block:: shell
    
       Datacenter: dc1
       ===============
       Status=Up/Down
       |/ State=Normal/Leaving/Joining/Moving
       --  Address    Load       Tokens       Owns    Host ID                               Rack
           UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
       UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1
   
    Use ``nodetool gossipinfo`` to see ``192.168.1.204`` is in HIBERNATE status.

    .. code-block:: shell
                             
       /192.168.1.204
         generation:1553759984                                                                                            
         heartbeat:104                      
         HOST_ID:655ae64d-e3fb-45cc-9792-2b648b151b67
         STATUS:hibernate,true
         RELEASE_VERSION:3.0.8
         X3:3                                        
         X5:                                                                                    
         NET_VERSION:0
         DC:DC1
         X4:0
         SCHEMA:2790c24e-39ff-3c0a-bf1c-cd61895b6ea1
         RPC_ADDRESS:192.168.1.204
         X2:
         RACK:B1
         INTERNAL_IP:192.168.1.204
    
         /192.168.1.203
         generation:1553759866
         heartbeat:2147483647
        HOST_ID:655ae64d-e3fb-45cc-9792-2b648b151b67
         STATUS:shutdown,true
         RELEASE_VERSION:3.0.8
         X3:3
         X5:0:18446744073709551615:1553759941343
         NET_VERSION:0
         DC:DC1
         X4:1
         SCHEMA:2790c24e-39ff-3c0a-bf1c-cd61895b6ea1
         RPC_ADDRESS:192.168.1.203
         RACK:B1
         LOAD:1.09776e+09
         INTERNAL_IP:192.168.1.203

    After the bootstrapping is over, ``nodetool status`` will show:

    .. code-block:: shell
    
       Datacenter: DC1
       Status=Up/Down
       State=Normal/Leaving/Joining/Moving
       --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
       UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
       UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1
       UN  192.168.1.204  124.42 KB  256     32.6%             675ed9f4-6564-6dbd-can8-43fddce952gy   B1 

#. Run the ``nodetool repair`` command on the node that was replaced to make sure that the data is synced with the other nodes in the cluster. You can use `Scylla Manager <https://manager.docs.scylladb.com/>`_ to run the repair.

    .. note:: 
       When :doc:`Repair Based Node Operations (RBNO) <repair-based-node-operation>` for **replace** is enabled, there is no need to rerun repair.

------------------------------
Setup RAID Following a Restart
------------------------------


In case you need to to restart (stop + start, not reboot) an instance with ephemeral storage, like EC2 i3, or i3en nodes,  you should be aware that:

   ephemeral volumes persist only for the life of the instance. When you stop, hibernate, or terminate an instance, the applications and data in its instance store volumes are erased. (see `https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/storage-optimized-instances.html <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/storage-optimized-instances.html>`_)

In this case, the node's data will be cleaned after restart. To remedy this, you need to recreate the RAID again.

#. Stop the Scylla server on the node you restarted. The rest of the commands will run on this node as well.

   .. include:: /rst_include/scylla-commands-stop-index.rst

#. Run the following command, remembering not to mount an invalid RAID disk after reboot:

   .. code-block:: none

      sudo sed -e '/.*scylla/s/^/#/g' -i /etc/fstab

#. Run the following command to enable auto_bootstrap after restart to sync the data:

   .. code-block:: none

      sudo sed -e '/auto_bootstrap:.*/s/False/True/g' -i /etc/scylla/scylla.yaml

#. Run the following command, replacing 172.30.0.186 with the listen_address / rpc_address of the node that you are restarting:

   .. code-block:: none

      echo 'replace_address_first_boot: 172.30.0.186' | sudo tee --append /etc/scylla/scylla.yaml

#. Run the following command to re-setup RAID

   .. code-block:: none

      sudo /opt/scylladb/scylla-machine-image/scylla_create_devices

#. Start Scylla Server

   .. include:: /rst_include/scylla-commands-stop-index.rst

Sometimes the public/ private IP of instance is changed after restart. If so refer to the Replace Procedure_ above.
