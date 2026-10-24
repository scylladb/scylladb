# Nodetool tasks ttl

**tasks ttl** - Gets or sets task_ttl value in seconds, i.e. time for which tasks are kept in task manager after
they are finished.

The new value is set only in memory. To change the configuration, modify `task_ttl_in_seconds` in `scylla.yaml`.
You can also run Scylla with `--task-ttl-in-seconds` parameter.

If task_ttl == 0, tasks are unregistered from task manager immediately after they are finished.

## Syntax

```console
nodetool tasks ttl [--set <time_in_seconds>]
```

## Options

* `--set` - sets task_ttl to the specified value.

For example:

Gets task_ttl value:

```shell
> nodetool tasks ttl
```

Sets task_ttl value to 10 seconds:

```shell
> nodetool tasks ttl --set 10
```
