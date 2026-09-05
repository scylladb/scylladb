Nodetool tasks wait
===================
**tasks wait** - Waits until a task manager task is finished and gets its status.
If timeout is set and it expires, the appropriate message with failure reason
will be printed.

Syntax
-------
.. code-block:: console

   nodetool tasks wait <task_id> [(--quiet|-q)] [--timeout <time_in_second>]

Options
-------

* ``--quiet`` or ``-q`` - if set, status of a task isn't printed. Instead the proper exit code is returned:

   * 0 - if task finished successfully
   * 123 - if task failed
   * 124 - if request timed out
   * 125 - if an error occurred and the task status is undetermined

* ``--timeout`` - timeout in seconds.

Syntax
-------
.. code-block:: console

   nodetool tasks wait <task_id>

For example:

.. code-block:: shell

   > nodetool tasks wait ef1b7a61-66c8-494c-bb03-6f65724e6eee

Example output
--------------

.. code-block:: shell

   id : 29dd6552-1e9a-4f17-b2c9-231d088fbee6
   type : repair
   kind : node
   scope : keyspace
   state : done
   is_abortable : true
   start_time : 2024-07-29T17:00:53Z
   end_time : 2024-07-29T17:00:53Z
   error :
   parent_id : none
   sequence_number : 2
   shard : 0
   keyspace : abc
   table :
   entity :
   progress_units : ranges
   progress_total : 4
   progress_completed : 4
   children_ids : [{task_id: 5d74a62e-aaf6-4993-b0f1-973899d89cd0, node: 127.0.0.1 }, {task_id: 055d5a59-d97c-45a8-a7e6-dcad39ed61ca, node: 127.0.0.1 }]


See also
--------

-  :doc:`tasks status </operating-scylla/nodetool-commands/tasks/status>`
-  :doc:`tasks tree </operating-scylla/nodetool-commands/tasks/tree>`
-  :doc:`tasks list </operating-scylla/nodetool-commands/tasks/list>`
