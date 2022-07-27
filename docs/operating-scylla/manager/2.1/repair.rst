======
Repair
======

.. include:: /operating-scylla/manager/_common/note-versions.rst

.. note:: If, after upgrading to the latest Scylla, you experience repairs that are slower than usual please consider :doc:`upgrading Scylla Manager to the appropriate version </upgrade/upgrade-manager/upgrade-guide-from-2.x.a-to-2.y.b/upgrade-row-level-repair>`.

When you create a cluster, a repair job is automatically scheduled. 
This task is set to occur each week by default, but you can change it to another time or add additional repair tasks. 
It is important to make sure that data across the nodes is consistent when maintaining your clusters.


Why repair with Scylla Manager
-------------------------------

Scylla Manager automates the repair process and allows you to manage how and when the repair occurs. 
The advantage of repairing the cluster with Scylla Manager is:

* Clusters are repaired node by node, ensuring that each database shard performs exactly one repair task at a time.
  This gives the best repair parallelism on a node, shortens the overall repair time, and does not introduce unnecessary load.

* If there is an error, Scylla Manager’s retry mechanism will try to run the repair again up to the number of retries that you set.

* It has a restart (pause) mechanism that allows for restarting a repair from where it left off.

* Repair what you want, when you want, and how often you want. Manager gives you that flexibility.

* The most apparent advantage is that with Manager you do not have to manually SSH into every node as you do with nodetool.

What can you repair with Scylla Manager
----------------------------------------

Scylla Manager can repair any item which it manages, specifically:

* Specific tables, keyspaces, clusters, or data centers.

* A group of tables, keyspaces, clusters or data centers.

* All tables, keyspaces, clusters, or data centers.


What sort of repairs can I run with Scylla Manager
---------------------------------------------------

You can run two types of repairs:

* Ad-hoc - this is a one time repair 

* Scheduled - this repair is scheduled in advance and can repeat

.. _manager-2.1-schedule-a-repair:

Schedule a Repair
-----------------

By default, a cluster successfully added to Scylla Manager has a repair task created for it, which repairs the entire cluster. 
This is a repeating task that runs every week. 
You can change this repair, add additional repairs, or delete this repair. 
You can schedule repairs to run in the future on a regular basis, schedule repairs to run once, or schedule repairs to run immediately on an as-needed basis. 
Any repair can be rescheduled, paused, resumed, or deleted. 
For information on what is repaired and the types of repairs available, see `What can you repair with Scylla Manager`_. 

Create a scheduled repair
.........................

While the most recommended way to run a repair is across an entire cluster, repairs can be scheduled to run on a single/multiple datacenters, keyspaces, or tables.
Scheduled repairs run every X days, depending on the frequency you set. 
The procedure here shows the most frequently used repair command. 
Additional parameters are located in the :ref:`sctool Reference <sctool-repair-parameters>`.

**Procedure**

Run the following sctoool repair command, replacing the parameters with your own parameters:

* ``-c`` - cluster name - replace `prod-cluster` with the name of your cluster

* ``-s`` - start-time - replace 2018-01-02T15:04:05-07:00 with the time  you want the repair to begin

* ``-i`` - interval - replace -i 7d with your own time interval

For example:

.. code-block:: none

   sctool repair -c prod-cluster -s 2018-01-02T15:04:05-07:00 -i 7d

2. The command returns the task ID. You will need this ID for additional actions.

3. If you want to run the repair only once, remove the `-i` argument. 

4. If you want to run this command immediately, but still want the repair to repeat, keep the interval argument (``-i``), but remove the start-date (``-s``).

Schedule an ad-hoc repair
.........................

An ad-hoc repair runs immediately and does not repeat. 
This procedure shows the most frequently used repair command. 
Additional parameters can be used. Refer to the :ref:`sctool Reference <sctool-repair-parameters>`.

**Procedure**

1. Run the following command, replacing the -c argument with your cluster name: 

.. code-block:: none

   sctool repair -c prod-cluster

2. The command returns the task ID. You will need this ID for additional actions.

Repair faster or slower
.......................

When scheduling repair, you may specify ``--intensity`` flag, the intensity meaning is:

* For values > 1 intensity specifies the number of segments repaired by Scylla in a single repair command. Higher values result in higher speed and may increase cluster load.
* For values < 1 intensity specifies what percent of node's shards repaired in parallel.
* For intensity equal to 1 it will repair one segment in each repair command on all shards in parallel.
* For zero intensity it uses limits specified in Scylla Manager :ref:`configuration <repair-settings>`.

  Please note that this only works with versions that are **not** :doc:`row-level-repair enabled </upgrade/upgrade-manager/upgrade-guide-from-2.x.a-to-2.y.b/upgrade-row-level-repair>`.

**Example**

.. code-block:: none

   sctool repair -c prod-cluster --intensity 16

.. _manager-2.1-reschedule-a-repair:

Reschedule a Repair
-------------------

You can change the run time of a scheduled repair using the update repair command. 
The new time you set replaces the time which was previously set. 
This command requires the task ID, which was generated when you set the repair. 
This can be retrieved using the command sctool :ref:`task list <sctool-task-list>`.

This example updates a task to run in 3 hours instead of whatever time it was supposed to run.

.. code-block:: none

   sctool task update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd -s now+3h

To start a scheduled repair immediately, run the following command inserting the task id and cluster name:

.. code-block:: none

   sctool task start repair/143d160f-e53c-4890-a9e7-149561376cfd -c prod-cluster


Pause a Repair
--------------

Pauses a specified task, provided it is running. 
You will need the task ID for this action. 
This can be retrieved using the command ``sctool task list``. To start the task again, see `Resume a Repair`_.
 
.. code-block:: none
 
   sctool task stop repair/143d160f-e53c-4890-a9e7-149561376cfd -c prod-cluster

Resume a Repair 
---------------

Restart a repair that is currently in the paused state. 
To start running a repair which is scheduled, but is currently not running, use the task update command. 
See `Reschedule a Repair`_.
You will need the task ID for this action. This can be retrieved using the command ``sctool task list``.

.. code-block:: none

   sctool task start repair/143d160f-e53c-4890-a9e7-149561376cfd -c prod-cluster

Delete a Repair
---------------

This action removes the repair from the task list. 
Once removed, you cannot resume the repair. 
You will have to create a new one.  
You will need the task ID for this action. 
This can be retrieved using the command ``sctool task list``.

.. code-block:: none

   sctool task delete repair/143d160f-e53c-4890-a9e7-149561376cfd -c prod-cluster
