Nodetool tasks list
=========================
**tasks list** - Gets the list of task manager tasks in a provided module.
An operation may be repeated if appropriate flags are set.


Syntax
-------
.. code-block:: console

   nodetool tasks list <module> [--internal] [(--keyspace <keyspace> | -ks <keyspace>)]
   [(--table <table> | -t <table>)] [--interval <time_in_seconds>] [(--iterations <number> | -i <number>)]

Options
-------

* ``--internal`` - if set, internal tasks are listed. Internal tasks are the ones that
  have a parent or cover an operation that is invoked internally.
* ``--keyspace`` or ``-ks`` - shows only tasks on the specified keyspace.
* ``--table`` or ``-t`` - shows only tasks on the specified table.
* ``--interval`` - repeats the operation periodically at specified time intervals.
* ``--iterations`` or ``-i`` - repeats the operation specified number of times.

For example:

Shows all repair tasks on keyspace `myks` and table `mytable`:

.. code-block:: shell

   > nodetool tasks list repair --internal -ks myks --table mytable

Shows all non-internal compaction tasks and repeats the operation 3 times every 5 seconds:

.. code-block:: shell

   > nodetool tasks list compaction --interval 5 --i 3

Example output
--------------

For single list:

.. code-block:: shell

    task_id                              type   kind scope    state sequence_number keyspace table entity
    5116ddb6-85b5-4c3e-94fb-72128f15d7b4 repair node keyspace done  3               abc

With repetition:

.. code-block:: shell

    task_id                              type   kind scope    state sequence_number keyspace table entity
    d8926ee7-0faf-47b7-bfeb-82477e0c7b33 repair node keyspace done  5               abc
    1e028cb8-31a3-45ed-8728-af7a1ab586f6 repair node keyspace done  4               abc

    task_id                              type   kind scope    state sequence_number keyspace table entity
    1e535f9b-97fa-4788-a956-8f3216a6ea8d repair node keyspace done  6               abc
    d8926ee7-0faf-47b7-bfeb-82477e0c7b33 repair node keyspace done  5               abc
    1e028cb8-31a3-45ed-8728-af7a1ab586f6 repair node keyspace done  4               abc

See also
--------

-  :doc:`tasks status </operating-scylla/nodetool-commands/tasks/status>`
