=========================================
Add a cluster or a node to Scylla Manager
=========================================

.. include:: /operating-scylla/manager/_common/note-versions.rst


Scylla Manager manages clusters. A cluster contains one or more nodes / datacenters. When you add a cluster to Scylla Manager, it adds all of the nodes which are:

* associated with it, 
* that are running Scylla Manager Agent, 
* and are accessible   


Port Settings
=============

Confirm all ports required for Scylla Manager and Scylla Manager Agent are open. This includes:

* 9042 CQL
* 9142 SSL CQL
* 10001 Scylla Agent REST API


Add a Cluster
=============

This procedure adds the nodes to Scylla Manager so the cluster can be a managed cluster under Scylla Manager.

Prerequisites
-------------

For each node in the cluster, the **same** :ref:`authentication token <manager-2.1-generate-auth-token>` needs to be identified in ``/etc/scylla-manager-agent/scylla-manager-agent.yaml``

Create a Managed Cluster
------------------------

.. _name:

**Procedure**

#. From the Scylla Manager Server, provide the broadcast_address of one of the nodes and the generated auth_token (if used) and a custom name if desired.

   Where:

   * ``--host`` is hostname or IP of one of the cluster nodes. You can use an IPv6 or an IPv4 address.
   * ``--name`` is an alias you can give to your cluster. Using an alias means you do not need to use the ID of the cluster in all other operations.  
   * ``--auth-token`` is the authentication :ref:`token <manager-2.1-generate-auth-token>` you identified in ``/etc/scylla-manager-agent/scylla-manager-agent.yaml``
   * ``--without-repair`` - when a cluster is added, the Manager schedules repair to repeat every 7 days. To create a cluster without a scheduled repair, use this flag.
   * ``--username`` and ``--password`` - optionally, you can provide CQL credentials to the cluster.
     For security reasons, the user should NOT have access to your data.
     This enables :ref:`CQL query-based health check <manager-2.1-cql-query-health-check>` compared to :ref:`credentials agnostic health check <manager-2.1-credentials-agnostic-health-check>` if you do not specify the credentials.
     This also enables CQL schema backup, which isn't performed if credentials aren't provided.

   Example (IPv4):

   .. code-block:: none

      sctool cluster add --host 34.203.122.52 --auth-token "6Es3dm24U72NzAu9ANWmU3C4ALyVZhwwPZZPWtK10eYGHJ24wMoh9SQxRZEluWMc0qDrsWCCshvfhk9uewOimQS2x5yNTYUEoIkO1VpSmTFu5fsFyoDgEkmNrCJpXtfM" --name prod-cluster

      c1bbabf3-cad1-4a59-ab8f-84e2a73b623f
       __  
      /  \     Cluster added! You can set it as default by exporting env variable.
      @  @     $ export SCYLLA_MANAGER_CLUSTER=c1bbabf3-cad1-4a59-ab8f-84e2a73b623f
      |  |     $ export SCYLLA_MANAGER_CLUSTER=prod-cluster
      || |/    
      || ||    Now run:
      |\_/|    $ sctool status -c prod-cluster
      \___/    $ sctool task list -c prod-cluster


   Example (IPv6):

   .. code-block:: none

         sctool cluster add --host 2a05:d018:223:f00:971d:14af:6418:fe2d --auth-token       "6Es3dm24U72NzAu9ANWmU3C4ALyVZhwwPZZPWtK10eYGHJ24wMoh9SQxRZEluWMc0qDrsWCCshvfhk9uewOimQS2x5yNTYUEoIkO1VpSmTFu5fsFyoDgEkmNrCJpXtfM" --name prod-cluster

   Each cluster has a unique ID.
   You will use this ID in all commands where the cluster ID is required.
   Each cluster is automatically registered with a repair task that runs once a week.
   This can be canceled using ``--without-repair``.
   To use a different repair schedule, see :ref:`Schedule a Repair <manager-2.1-schedule-a-repair>`.

#. Verify that the cluster you added has a registered repair task by running the ``sctool task list -c <cluster-name>`` command, adding the name_  of the cluster you created in step 1 (with the ``--name`` flag).

   .. code-block:: none

      sctool task list -c prod-cluster
      ╭───────────────────────────────────────────────────────┬───────────┬────────────────────────────────┬────────╮
      │ Task                                                  │ Arguments │ Next run                       │ Status │
      ├───────────────────────────────────────────────────────┼───────────┼────────────────────────────────┼────────┤
      │ healthcheck/8988932e-de2f-4c42-a2f8-ae3b97fd7126      │           │ 02 Apr 20 12:28:10 CEST (+15s) │ NEW    │
      │ healthcheck_rest/9b7e694d-a1e3-42f1-8ca6-d3dfd9f0d94f │           │ 02 Apr 20 12:28:40 CEST (+1h)  │ NEW    │
      │ repair/0fd8a43b-eacf-4df8-9376-2a31b0dee6cc           │           │ 03 Apr 20 00:00:00 CEST (+7d)  │ NEW    │
      ╰───────────────────────────────────────────────────────┴───────────┴────────────────────────────────┴────────╯

   You will see 3 tasks which are created by adding the cluster:

   * Healthcheck - which checks the Scylla CQL, starting immediately, repeating every 15 seconds. See :doc:`Scylla Health Check <health-check>`
   * Healthcheck REST - which checks the Scylla REST API, starting immediately, repeating every hour. See :doc:`Scylla Health Check <health-check>`
   * Repair - an automated repair task, starting at midnight tonight, repeating every seven days at midnight. See :doc:`Run a Repair <repair>`

   .. note:: If you want to change the schedule for the repair, see :ref:`Reschedule a repair <manager-2.1-reschedule-a-repair>`.

Connect Managed Cluster to Scylla Monitoring
============================================

Connecting your cluster to Scylla Monitoring allows you to see metrics about your cluster and Scylla Manager all within Scylla Monitoring. 

To connect your cluster to Scylla Monitoring, it is **required** to use the same cluster name_ as you used when you created the cluster. See `Add a Cluster`_.

**Procedure**

Follow the procedure |mon_root| as directed, remembering to update the Scylla Node IPs and  Cluster name_  as well as the Scylla Manager IP in the relevant Prometheus configuration files. 

If you have any issues connecting to Scylla Monitoring Stack, consult the :doc:`Troubleshooting Guide </troubleshooting/manager-monitoring-integration>`.

Add a Node to a Managed Cluster
===============================

Although Scylla Manager is aware of all topology changes made within every cluster it manages, it cannot properly manage nodes/datacenters without establishing connections with every node/datacenter in the cluster, including the Scylla Manager Agent, which is on each managed node. 

**Before You Begin**

* Confirm you have a managed cluster running under Scylla Manager. If you do not have a managed cluster, see `Add a cluster`_.
* Confirm the :ref:`node <add-node-to-cluster-procedure>` or :doc:`Datacenter </operating-scylla/procedures/cluster-management/add-dc-to-existing-dc>` is added to the Scylla Cluster.

**Procedure**

#. :doc:`Add Scylla Manager Agent <install-agent>` to the new node. Use the **same** authentication token as you did for the other nodes in this cluster. Do not generate a new token. 

#. Confirm the node / datacenter was added by checking its :ref:`status <sctool_status>`. From the node running the Scylla Manager server, run the ``sctool status`` command, using the name of the managed cluster.
 
   .. code-block:: none
   
      sctool status -c prod-cluster
      Datacenter: eu-west
      ╭────┬───────────────┬────────────┬──────────────┬──────────────────────────────────────╮
      │    │ CQL           │ REST       │ Host         │ Host ID                              │
      ├────┼───────────────┼────────────┼──────────────┼──────────────────────────────────────┤
      │ UN │ UP SSL (42ms) │ UP (52ms)  │ 10.0.114.68  │ 45a7390d-d162-4daa-8bff-6469c9956f8b │
      │ UN │ UP SSL (38ms) │ UP (88ms)  │ 10.0.138.46  │ 8dad7fc7-5a82-4fbb-8901-f6f60c12342a │
      │ UN │ UP SSL (38ms) │ UP (298ms) │ 10.0.196.204 │ 44eebe5b-e0cb-4e45-961f-4ad175592977 │
      │ UN │ UP SSL (43ms) │ UP (159ms) │ 10.0.66.115  │ 918a52aa-cc42-43a4-a499-f7b1ccb53b18 │
      ╰────┴───────────────┴────────────┴──────────────┴──────────────────────────────────────╯

#. If you are using the Scylla Monitoring Stack, continue to `Connect Managed Cluster to Scylla Monitoring`_ for more information. 

Remove a Node/Datacenter from Scylla Manager
--------------------------------------------

There is no need to perform any action in Scylla Manager after removing a node or datacenter from a Scylla cluster. 

.. note:: If you are removing the cluster from Scylla Manager and you are using Scylla Monitoring, refer to |mon_root| for more information. 

See Also
========

* :doc:`sctool Reference <sctool>`
* :doc:`Remove a node from a Scylla Cluster </operating-scylla/procedures/cluster-management/remove-node>` 
* :doc:`Scylla Monitoring </operating-scylla/monitoring/index>`

