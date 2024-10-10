Nodetool tasks status
=========================
**tasks status** - Gets the status of a task manager task. If the task was finished it is unregistered.

Syntax
-------
.. code-block:: console

   nodetool tasks status <task_id>

For example:

.. code-block:: shell

   > nodetool tasks status ef1b7a61-66c8-494c-bb03-6f65724e6eee

Example output
--------------

.. code-block:: shell

   id: 52b69817-aac7-47bf-9d8d-0b487dd6866a
   type: repair
   kind: node
   scope: keyspace
   state: done
   is_abortable: true
   start_time: 2024-07-29T15:48:55Z
   end_time: 2024-07-29T15:48:55Z
   error:
   parent_id: none
   sequence_number: 5
   shard: 0
   keyspace: abc
   table:
   entity:
   progress_units: ranges
   progress_total: 4
   progress_completed: 4
   children_ids: [{task_id: 9de80fd8-296e-432b-837e-63daf7c93e17, node: 127.0.0.1 }, {task_id: fa1c1338-abd1-4f0c-8ce1-8a806486e5c3, node: 127.0.0.1 }]

See also
--------

-  :doc:`tasks tree </operating-scylla/nodetool-commands/tasks/tree>`
-  :doc:`tasks list </operating-scylla/nodetool-commands/tasks/list>`
-  :doc:`tasks wait </operating-scylla/nodetool-commands/tasks/wait>`
