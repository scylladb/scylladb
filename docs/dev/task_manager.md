Task manager is a tool for tracking long-running background
operations.

# Structure overview

Task manager is divided into modules, e.g. repair or compaction
module, which keep track of operations of similar nature. Operations
are tracked with tasks.

Each task covers a logical part of the operation, e.g repair
of a keyspace or a table. Each operation is covered by a tree
of tasks, e.g. global repair task is a parent of tasks covering
a single keyspace, which are parents of table tasks.

# Time to live of a task

Root tasks are kept in task manager for `task_ttl` time after they are
finished. `task_ttl` value can be set in node configuration with
`--task-ttl-in-seconds` option or changed with task manager API
(`/task_manager/ttl`).

A task which isn't a root is unregistered immediately after it is
finished and its status is folded into its parent. When a task
is being folded into its parent, info about each of its children is
lost unless the child or any child's descendant failed.

# Internal

Tasks can be marked as `internal`, which means they are not listed
by default. A task should be marked as internal if it has a parent
or if it's supposed to be unregistered immediately after it's finished.

# Abortable

A flag which determines if a task can be aborted through API.

# Type vs scope

`type` of a task describes what operation is covered by a task,
e.g. "major compaction".

`scope` of a task describes for which part of the operation
the task is responsible, e.g. "shard".

# API

Documentation for task manager API is available under `api/api-doc/task_manager.json`.
Briefly:
- `/task_manager/list_modules` -
        lists module supported by task manager;
- `/task_manager/list_module_tasks/{module}` -
        lists (by default non-internal) tasks in the module;
- `/task_manager/task_status/{task_id}` -
        gets the task's status;
- `/task_manager/abort_task/{task_id}` -
        aborts the task if it's abortable;
- `/task_manager/wait_task/{task_id}` -
        waits for the task and gets its status;
- `/task_manager/task_status_recursive/{task_id}` -
        gets statuses of the task and all its descendants in BFS
        order;
- `/task_manager/ttl` -
        sets new ttl, returns old value.
- `/task_manager/drain/{module}` -
        unregisters all finished local tasks in the module.
