``--interval <time between task runs>``

Amount of time after which a successfully completed task would be run again.
Supported time units include:

* ``d`` - days,
* ``h`` - hours,
* ``m`` - minutes,
* ``s`` - seconds.

**Default** 0 (no interval)

.. note:: The task run date is aligned with ``--start date`` value. For example, if you select ``--interval 7d`` task would run weekly at the ``--start-date`` time.

=====

``-s, --start-date <now+duration|RFC3339>``

The date can be expressed relatively to now or as a RFC3339 formatted string.
To run the task in 2 hours use ``now+2h``, supported units are:

* ``h`` - hours,
* ``m`` - minutes,
* ``s`` - seconds,
* ``ms`` - milliseconds.

If you want the task to start at a specified date use RFC3339 formatted string i.e. ``2018-01-02T15:04:05-07:00``.
If you want the repair to start immediately, use the value ``now`` or skip this flag.

**Default:**  now (start immediately)

=====

``-r, --num-retries <times to rerun a failed task>``

Number of times a task reruns following a failure. The task reruns 10 minutes following a failure.
If the task fails after the retry times have been used, it will not retry again until its next run which was scheduled according to the ``--interval`` parameter.

.. note:: If this is an ad hoc repair, the task will not run again.

**Default:** 3
