Nodetool tasks user-ttl
=======================
**tasks user-ttl** - Gets or sets user_task_ttl value in seconds, i.e. time for which tasks started by user are kept
in task manager after they are finished.

The new value is set only in memory. To change the configuration, modify ``user_task_ttl_in_seconds`` in ``scylla.yaml``.
You can also run Scylla with ``--user-task-ttl-in-seconds`` parameter.

If user_task_ttl == 0, tasks are unregistered from task manager immediately after they are finished.

Syntax
-------
.. code-block:: console

   nodetool tasks user-ttl [--set <time_in_seconds>]

Options
-------

* ``--set`` - sets user_task_ttl to the specified value.

For example:

Gets user_task_ttl value:

.. code-block:: shell

   > nodetool tasks user-ttl

Sets user_task_ttl value to 10 seconds:

.. code-block:: shell

   > nodetool tasks user-ttl --set 10
