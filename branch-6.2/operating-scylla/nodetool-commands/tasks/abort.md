# Nodetool tasks abort

**tasks abort** - Aborts a task manager task with the provided id if the task
is abortable. If the task is not abortable, the appropriate message with failure
reason will be printed.

## Syntax

```console
nodetool tasks abort <task_id>
```

For example:

```shell
> nodetool tasks abort ef1b7a61-66c8-494c-bb03-6f65724e6eee
```

## See also

- [tasks list](https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/tasks/list.md)
