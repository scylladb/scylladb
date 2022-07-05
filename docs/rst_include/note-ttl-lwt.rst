.. note::  TTL on records in system.paxos table is set with the ``paxos_grace_seconds`` value.
   If this value is not set, the value from ``gc_grace_seconds`` is used.
   The default for ``gc_grace_seconds`` and ``paxos_grace_seconds`` are both the same (10 days).
   You can have two different settings for ``paxos_grace_seconds`` and ``gc_grace_seconds``.
   You can change the ``paxos_grace_seconds`` value by altering the system.paxos table.
