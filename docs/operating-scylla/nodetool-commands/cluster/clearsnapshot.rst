Nodetool cluster clearsnapshot
==============================

**cluster clearsnapshot** - Clears the cluster snapshot with the given tag: releases the snapshot's data and deletes its entries from the snapshot catalog.

Snapshots of tables on object storage exist as bucket references and snapshot catalog entries rather than per-node snapshot directories, so they are cleared with this command rather than with the per-node :doc:`clearsnapshot </operating-scylla/nodetool-commands/clearsnapshot>`. Data still referenced by another snapshot or by a live table is kept. Local tables covered by the same snapshot have their snapshot directories removed, so one command clears a snapshot spanning both storage kinds.

  For example:

  ::

     nodetool cluster clearsnapshot -t <tag>

The command starts a task and waits for its completion. With ``--nowait`` it prints the task id and returns immediately; use the ``task`` subcommands to manage the task.
