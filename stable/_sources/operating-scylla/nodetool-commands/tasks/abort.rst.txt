Nodetool tasks abort
====================
**tasks abort** - Aborts a task manager task with the provided id if the task
is abortable. If the task is not abortable, the appropriate message with failure
reason will be printed.

Syntax
-------
.. code-block:: console

   nodetool tasks abort <task_id>

For example:

.. code-block:: shell

   > nodetool tasks abort ef1b7a61-66c8-494c-bb03-6f65724e6eee

See also
--------

-  :doc:`tasks list </operating-scylla/nodetool-commands/tasks/list>`
