Nodetool cluster clearsnapshot
==============================

**cluster clearsnapshot** ``-t <tag> [--force] [--nowait]`` - Clears the cluster snapshot with the given tag: the snapshot directories of its local tables, the bucket references of its tables on object storage, and its entries in the snapshot catalog.

Snapshots of tables on object storage exist as bucket references and snapshot catalog entries rather than per-node snapshot directories, so they are cleared with this command rather than with the per-node :doc:`clearsnapshot </operating-scylla/nodetool-commands/clearsnapshot>`. Data still referenced by another snapshot or by a live table is kept. Local tables covered by the same snapshot have their snapshot directories removed on every node, so one command clears a snapshot spanning both storage kinds.

A snapshot that was backed up with :doc:`cluster backup </operating-scylla/nodetool-commands/backup>` is not cleared unless ``--force`` is given: the backup is then deleted along with the snapshot, whether it was uploaded from local tables or made in place in the bucket of tables on object storage. A snapshot that failed before completion leaves catalog entries and bucket references behind without a committed snapshot; clearing such a tag needs ``--force`` as well. The command is rejected while a backup of the tag is in progress.

  For example:

  ::

     nodetool cluster clearsnapshot -t <tag>
     nodetool cluster clearsnapshot -t <tag> --force

The command starts a task and waits for its completion. With ``--nowait`` it prints the task id and returns immediately; use the ``task`` subcommands to manage the task. A clear that fails midway can be run again with the same options, it picks up what is left.
