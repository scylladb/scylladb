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

There are two types of tasks supported by task manager - regular tasks
(task_manager::task) and virtual tasks (task_manager::virtual_task).
Regular tasks cover local operations (or their parts) and virtual
tasks - global, cluster-wide operations.

# Time to live of a task

Regular root tasks are kept in task manager for `task_ttl` time after
they are finished. `task_ttl` value can be set in node configuration
with `--task-ttl-in-seconds` option or changed with task manager API
(`/task_manager/ttl`).

A task which isn't a root is unregistered immediately after it is
finished and its status is folded into its parent. When a task
is being folded into its parent, info about each of its children is
lost unless the child or any child's descendant failed.

Time for which a virtual task is shown in task manager depends
on a specific implementation.

# Internal

Tasks can be marked as `internal`, which means they are not listed
by default. A task should be marked as internal if it has a parent
which is a regular task or if it's supposed to be unregistered
immediately after it's finished.

# Abortable

A flag which determines if a task can be aborted through API.

# Type vs scope vs kind

`type` of a task describes what operation is covered by a task,
e.g. "major compaction".

`scope` of a task describes for which part of the operation
the task is responsible, e.g. "shard".

`kind` of a task indicates whether a task is regular ("local") or virtual ("global").

# API

Documentation for task manager API is available under `api/api-doc/task_manager.json`.
Briefly:
- `/task_manager/list_modules` -
        lists module supported by task manager;
- `/task_manager/list_module_tasks/{module}` -
        lists (by default non-internal) tasks in the module;
- `/task_manager/task_status/{task_id}` -
        gets the task's status, unregisters the task if it's finished;
- `/task_manager/abort_task/{task_id}` -
        aborts the task if it's abortable;
- `/task_manager/wait_task/{task_id}` -
        waits for the task and gets its status;
- `/task_manager/task_status_recursive/{task_id}` -
        gets statuses of the task and all its descendants in BFS
        order, unregisters the task;
- `/task_manager/ttl` -
        gets or sets new ttl.

# Virtual tasks

A virtual task is a task which covers an operation that spreads
among the whole cluster. From API perspective virtual tasks are
similar to regular tasks. The main differences are:
- a virtual task is presented on each node;
- time which virtual tasks spend in task manager is
  implementation dependent;
- number of children does not have to be monotonous (virtual tasks
  do not keep references to their children).

## Implementation

Virtual tasks aren't kept in memory and their status isn't updated
proactively as for regular tasks. Instead, the appropriate data
(e.g. task status) is created based on an associated service
(e.g. `storage_service` for `node_ops` virtual tasks) once API user
requests it.

`virtual_task` class generates statuses for all operations from one
group - it can contain many abstract virtual tasks. All virtual_tasks
are kept only on shard 0.

# Group traits of virtual tasks

- `topology_change_group`:
    - tasks are listed for `task_ttl` after they are finished,
      but their statuses can be viewed as long as they are kept
      in topology_requests table.
