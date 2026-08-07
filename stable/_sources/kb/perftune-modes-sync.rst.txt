==============================================================
Updating the Mode in perftune.yaml After a ScyllaDB Upgrade
==============================================================

In versions 5.1 (ScyllaDB Open Source) and 2022.2 (ScyllaDB Enterprise), we improved ScyllaDB's performance by `removing the rx_queues_count from the mode 
condition <https://github.com/scylladb/seastar/pull/949>`_. As a result, ScyllaDB operates in 
the ``sq_split`` mode instead of the ``mq`` mode (see :doc:`Seastar Perftune </operating-scylla/admin-tools/perftune>` for information about the modes).
If you upgrade from an earlier version of ScyllaDB, your cluster's existing nodes may use the ``mq`` mode, 
while new nodes will use the ``sq_split`` mode. As using different modes across one cluster is not recommended, 
you should change the configuration to ensure that the ``sq_split`` mode is used on all nodes.

This section describes how to update the `perftune.yaml` file to configure the ``sq_split`` mode on all nodes. 

Procedure
------------
The examples below assume that you are using the default locations for storing data and the `scylla.yaml` file, 
and that your NIC is ``eth5``. 

#. Backup your old configuration. 

   .. code-block:: console

     sudo mv /etc/scylla.d/cpuset.conf /etc/scylla.d/cpuset.conf.old
     sudo mv /etc/scylla.d/perftune.yaml /etc/scylla.d/perftune.yaml.old

#. Create a new configuration.

   .. code-block:: console

     sudo scylla_sysconfig_setup --nic eth5 --homedir /var/lib/scylla --confdir /etc/scylla

   A new ``/etc/scylla.d/cpuset.conf`` will be generated on the output.

#. Compare the contents of the newly generated ``/etc/scylla.d/cpuset.conf`` with ``/etc/scylla.d/cpuset.conf.old`` you created in step 1.
    
     - If they are exactly the same, rename ``/etc/scylla.d/perftune.yaml.old`` you created in step 1 back to ``/etc/scylla.d/perftune.yaml`` and continue to the next node.
     - If they are different, move on to the next steps.

#. Restart the ``scylla-server`` service.

   .. code-block:: console

     nodetool drain
     sudo systemctl restart scylla-server

#. Wait for the service to become up and running (similarly to how it is done during a :doc:`rolling restart </operating-scylla/procedures/config-change/rolling-restart>`). It may take a considerable amount of time before the node is in the UN state due to resharding.

#. Continue to the next node.