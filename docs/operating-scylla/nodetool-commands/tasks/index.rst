Nodetool tasks
==============

.. toctree::
   :hidden:

   abort <abort>
   drain <drain>
   user-ttl <user-ttl>
   list <list>
   modules <modules>
   status <status>
   tree <tree>
   ttl <ttl>
   wait <wait>

**tasks** - Nodetool supercommand for managing task manager tasks.

Task manager is an API-based tool for tracking long-running background operations, such as repair or compaction,
which makes them observable and controllable. Task manager operates per node.

Task Status Retention
---------------------

* When a task completes, its status is temporarily stored on the executing node
* Status information is retained for up to :confval:`task_ttl_in_seconds` seconds

Supported tasks suboperations
-----------------------------

* :doc:`abort </operating-scylla/nodetool-commands/tasks/abort>` - Aborts the task.
* :doc:`drain </operating-scylla/nodetool-commands/tasks/drain>` - Unregisters all finished local tasks.
* :doc:`user-ttl </operating-scylla/nodetool-commands/tasks/user-ttl>` - Gets or sets user_task_ttl value.
* :doc:`list </operating-scylla/nodetool-commands/tasks/list>` - Lists tasks in the module.
* :doc:`modules </operating-scylla/nodetool-commands/tasks/modules>` - Lists supported modules.
* :doc:`status </operating-scylla/nodetool-commands/tasks/status>` - Gets status of the task.
* :doc:`tree </operating-scylla/nodetool-commands/tasks/tree>` - Gets statuses of the task and all its descendants.
* :doc:`ttl </operating-scylla/nodetool-commands/tasks/ttl>` - Gets or sets task_ttl value.
* :doc:`wait </operating-scylla/nodetool-commands/tasks/wait>` - Waits for the task and gets its status.
