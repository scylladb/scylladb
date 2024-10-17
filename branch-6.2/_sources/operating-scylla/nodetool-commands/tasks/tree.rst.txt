Nodetool tasks tree
=======================
**tasks tree** - Gets the statuses of a task manager task and all its descendants.
The statuses are listed in BFS order. If the task was finished it is unregistered.

If task_id isn't specified, trees of all non-internal tasks are printed
(internal tasks are the ones that have a parent or cover an operation that
is invoked internally).

Syntax
-------
.. code-block:: console

   nodetool tasks tree [<task_id>]

For example:

.. code-block:: shell

   > nodetool tasks tree 2ef0a5b6-f243-4c01-876f-54539b453766

Example output
--------------

For single task:

.. code-block:: shell

   id                                   type   kind scope    state is_abortable start_time           end_time             error parent_id                            sequence_number shard keyspace table entity progress_units total completed children_ids
   be5559ea-bc5a-428c-b8ce-d14eac7a1765 repair node keyspace done  true         2024-07-29T16:06:46Z 2024-07-29T16:06:46Z       none                                 1               0     abc                   ranges         4     4         [{task_id: 542e38cb-9ad4-40aa-9010-de2630004e55, node: 127.0.0.1 }, {task_id: 8974ebcc-1e87-4040-88fe-f2438261f7fb, node: 127.0.0.1 }]
   542e38cb-9ad4-40aa-9010-de2630004e55 repair node shard    done  false        2024-07-29T16:06:46Z 2024-07-29T16:06:46Z       be5559ea-bc5a-428c-b8ce-d14eac7a1765 1               0     abc                   ranges         2     2         []
   8974ebcc-1e87-4040-88fe-f2438261f7fb repair node shard    done  false        2024-07-29T16:06:46Z 2024-07-29T16:06:46Z       be5559ea-bc5a-428c-b8ce-d14eac7a1765 1               1     abc                   ranges         2     2         []

For all tasks:

.. code-block:: shell

   id                                   type                   kind    scope    state is_abortable start_time           end_time             error parent_id                            sequence_number shard keyspace table entity progress_units total completed children_ids
   16eafb1e-8b2e-48e6-bd7a-432ca3d8b9fc repair                 node    keyspace done  true         2024-07-29T16:34:46Z 2024-07-29T16:34:46Z       none                                 1               0     abc                   ranges         4     4         [{task_id: e0aa1aa4-58ca-4bfb-b3e6-74e5f3a0f6ee, node: 127.0.0.1 }, {task_id: 49eb5797-b67e-46b0-9365-4460f7cf988a, node: 127.0.0.1 }]
   e0aa1aa4-58ca-4bfb-b3e6-74e5f3a0f6ee repair                 node    shard    done  false        2024-07-29T16:34:46Z 2024-07-29T16:34:46Z       16eafb1e-8b2e-48e6-bd7a-432ca3d8b9fc 1               0     abc                   ranges         2     2         []
   49eb5797-b67e-46b0-9365-4460f7cf988a repair                 node    shard    done  false        2024-07-29T16:34:46Z 2024-07-29T16:34:46Z       16eafb1e-8b2e-48e6-bd7a-432ca3d8b9fc 1               1     abc                   ranges         2     2         []
   82d7b2a4-146e-4a72-ba93-c66d5b4e9867 offstrategy compaction node    keyspace done  true         2024-07-29T16:34:16Z 2024-07-29T16:34:16Z       none                                 954             0     abc                                  1     1         [{task_id: 9818277b-238d-4298-a56b-c0d2153bf140, node: 127.0.0.1 }, {task_id: c1eb0701-ad7a-45ff-956f-7b8d671fc5db, node: 127.0.0.1 }
   9818277b-238d-4298-a56b-c0d2153bf140 offstrategy compaction node    shard    done  false        2024-07-29T16:34:16Z 2024-07-29T16:34:16Z       82d7b2a4-146e-4a72-ba93-c66d5b4e9867 954             0     abc                                  1     1         []
   c1eb0701-ad7a-45ff-956f-7b8d671fc5db offstrategy compaction node    shard    done  false        2024-07-29T16:34:16Z 2024-07-29T16:34:16Z       82d7b2a4-146e-4a72-ba93-c66d5b4e9867 954             1     abc                                  1     1         []

See also
--------

-  :doc:`tasks status </operating-scylla/nodetool-commands/tasks/status>`
-  :doc:`tasks list </operating-scylla/nodetool-commands/tasks/list>`
-  :doc:`tasks wait </operating-scylla/nodetool-commands/tasks/wait>`
