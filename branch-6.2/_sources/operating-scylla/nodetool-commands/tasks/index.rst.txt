Nodetool tasks
==============

.. toctree::
   :hidden:

   abort <abort>
   list <list>
   modules <modules>
   status <status>
   tree <tree>
   ttl <ttl>
   wait <wait>

**tasks** - Nodetool supercommand for managing task manager tasks.

Task manager is an API-based tool for tracking long-running background operations, such as repair or compaction,
which makes them observable and controllable. Task manager operates per node.

Supported tasks suboperations
-----------------------------

* :doc:`abort </operating-scylla/nodetool-commands/tasks/abort>` - Aborts the task.
* :doc:`list </operating-scylla/nodetool-commands/tasks/list>` - Lists tasks in the module.
* :doc:`modules </operating-scylla/nodetool-commands/tasks/modules>` - Lists supported modules.
* :doc:`status </operating-scylla/nodetool-commands/tasks/status>` - Gets status of the task.
* :doc:`tree </operating-scylla/nodetool-commands/tasks/tree>` - Gets statuses of the task and all its descendants.
* :doc:`ttl </operating-scylla/nodetool-commands/tasks/ttl>` - Gets or sets task_ttl value.
* :doc:`wait </operating-scylla/nodetool-commands/tasks/wait>` - Waits for the task and gets its status.
