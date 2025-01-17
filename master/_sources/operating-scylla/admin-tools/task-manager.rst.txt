==================
Task manager tasks
==================

Task manager tracks long-running background operations. Task manager is divided into modules. Each module is responsible
for tracking operations of similar purposes (e.g. compactions). The operation and its parts are represented by tasks.
Tasks form a tree, where the root task covers the whole operation (e.g. compaction) and its children cover
its suboperations (e.g. compaction of one table), etc.

There are two types of tasks: *cluster* and *node* tasks. Node tasks cover operations that are performed on a single
node. To see their stats, you need to request the status from a particular node. Cluster tasks are responsible 
for the operations that spread across many nodes. They are visible from all nodes in the cluster. Cluster tasks 
can't have parents. They can have children on many nodes. The status of a cluster task's child may not be accessible
even though the cluster operation is still running.

A task may be *internal*. This means that the task has a parent or is started by an internal process. By default API
calls skip the internal tasks. Cluster tasks cannot be internal.

If a task is internal, it is unregistered from task manager immediately after it is finished. If it has a non-cluster
parent, the task's status is folded into its parent and accessible only through the parent. You won't see the statuses
of indirect descendants (e.g. children of children) of a finished task unless they have failed.

Other non-cluster tasks stay in task manager for *task_ttl* seconds after they are finished or *user_task_ttl* seconds if they
were started by user. task_ttl value can be set with ``task_ttl_in_seconds`` param or through ``/task_manager/ttl`` api.
user_task_ttl value can be set with ``user_task_ttl_in_seconds`` param or through ``/task_manager/user_ttl`` api. The time
for which cluster tasks are visible in task manager isn't specified.


Task manager API
----------------

Data structures
^^^^^^^^^^^^^^^

Task data, that is returned from the task manager API, is kept in either ``task_stats`` or ``task_status``.

task_stats
..........

- *task_id* - unique task id;
- *type* - a type of the task, e.g. offstrategy compaction;
- *kind* - whether the task is per node or cluster;
- *scope* - specifies the operation's scope, e.g. keyspace, range;
- *state* - one of created, running, done, failed;
- *sequence_number* - an operation number (per module). It is shared by all tasks in a tree. Irrelevant for cluster tasks;
- *keyspace* - optional, name of a keyspace on which the task operates;
- *table* - optional, name of a table on which the task operates;
- *entity* - optional, additional info specific to the task.


task_status
...........

All fields from task_stats and additionally:

- *is_abortable* - a flag that decides whether the task can be aborted through API;
- *start_time* - relevant only if state == created;
- *end_time* - relevant only if the task is finished (state in [done, failed]);
- *error* - relevant only if the task failed;
- *parent_id* - relevant only if the task has a parent;
- *shard* - optional, shard id on which the task operates;
- *progress_units* - a unit of progress;
- *progress_total* - job size in progress_units;
- *progress_completed* - current progress in progress_units;
- *children_ids* - list of pairs of children ids and nodes on which they are created.

API calls
^^^^^^^^^^

* ``/task_manager/list_modules`` - lists modules supported by task manager;
* ``/task_manager/list_module_tasks/{module}`` - lists tasks in the module; query params:

	- *internal* - if set, internal tasks are listed, false by default;
	- *keyspace* - if set, tasks are filtered to contain only the ones working on this keyspace;
	- *table* - if set, tasks are filtered to contain only the ones working on this table;

* ``/task_manager/task_status/{task_id}`` - gets the task's status, unregisters the task if it's finished;
* ``/task_manager/abort_task/{task_id}`` - aborts the task if it's abortable, otherwise 403 status code is returned;
* ``/task_manager/wait_task/{task_id}`` - waits for the task and gets its status (does not unregister the tasks); query params:

	- *timeout* - timeout in seconds; if set - 408 status code is returned if waiting times out;

* ``/task_manager/task_status_recursive/{task_id}`` - gets statuses of the task and all its descendants in BFS order, unregisters the root task;
* ``/task_manager/ttl`` - gets or sets new ttl; query params (if setting):

	- *ttl* - new ttl value.

* ``/task_manager/user_ttl`` - gets or sets new user ttl; query params (if setting):

	- *user_ttl* - new user ttl value.

Cluster tasks are not unregistered from task manager with API calls.

Tasks API
---------

With task manager, we can have asynchronous versions of synchronous calls. Some of them are accessible from tasks API.
The calls work analogically as their synchronous versions, but instead of waiting for the operation to be done, they
return the id of the associated task. You can query the operation's status with task manager API.


See :doc:`Nodetool tasks </operating-scylla/nodetool-commands/tasks/index>`.

To learn how to interact with REST API see :doc:`REST API </operating-scylla/rest>`.
