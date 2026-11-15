.. note::  ScyllaDB sets the TTL of a table's Paxos state, stored in ``system.paxos`` or
   ``<user-table>$paxos`` (see :ref:`paxos-state-tables`), to the table's
   ``paxos_grace_seconds``. The default value is 10 days. You can change it by
   altering the user table. See :ref:`Paxos grace seconds <cql-paxos-grace-seconds>`
   for details.
