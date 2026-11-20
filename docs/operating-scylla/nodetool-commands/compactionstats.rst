Nodetool compactionstats
========================

**compactionstats** - Displays statistics for compactions. Each row represents a running compaction.

Syntax
------

.. code-block:: console

   nodetool compactionstats [(-H | --human-readable)]

Options
-------

* ``-H`` or ``--human-readable`` - Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB.

For example:

.. code-block:: shell

   nodetool compactionstats

Example output:

.. code-block:: shell

   pending tasks: 2
   - nba.player_name: 1
   - nba.player_stats: 1

   id                                   compaction type keyspace table        completed  total       unit  progress
   9a1b2c30-5358-11e6-9d5f-000000000000 MAJOR           nba      player_name  5770708712 55239418023 bytes 10.45%
   9a1b2c31-5358-11e6-9d5f-000000000000 COMPACTION      nba      player_stats 18569428   3610548974  bytes 0.51%
   Active compaction remaining time :        n/a

================================================  =================================================================================
Parameter                                         Description
================================================  =================================================================================
pending tasks                                     Number of pending compactions, followed by the number of pending compactions per table
------------------------------------------------  ---------------------------------------------------------------------------------
id                                                The ID of the compaction task
------------------------------------------------  ---------------------------------------------------------------------------------
compaction type                                   Type of compaction
------------------------------------------------  ---------------------------------------------------------------------------------
keyspace                                          The keyspace name
------------------------------------------------  ---------------------------------------------------------------------------------
table                                             The table name
------------------------------------------------  ---------------------------------------------------------------------------------
completed                                         Completed bytes out of the total
------------------------------------------------  ---------------------------------------------------------------------------------
total                                             Total size of the compaction
------------------------------------------------  ---------------------------------------------------------------------------------
unit                                              Compaction unit type
------------------------------------------------  ---------------------------------------------------------------------------------
progress                                          Compaction progress %
------------------------------------------------  ---------------------------------------------------------------------------------
Active compaction remaining time                  Estimated time to finish the running compactions at the configured compaction throughput (``n/a`` if unthrottled)
================================================  =================================================================================


.. include:: nodetool-index.rst
