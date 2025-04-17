# View building coordinator

The view building coordinator is responsible for building views from tablet-based keyspaces.

In contrast to vnode-based views, which are built by node-local view builder,
the view building coordinator is a single entity within the whole cluster, 
running on the raft leader alongside the topology coordinator.

The coordinator state is stored in following group0 tables and 
it's loaded into `view_building_state_machine` while group0 state is applied.

Currently the coordinator processes at most only one base table at the time, building all views for this base table.

## View building task

Whole view building process is splitted into smaller view building tasks.
Each task is associated with particular tablet replica `(host_id, shard, tablet_id)` of a certain base table.

There are 2 types of tasks:
- `build_range` - generate view updates from the tablet replica of the base table to build view
- `process_staging` - process (generate view updates and move to base directory) 
                      all staging sstables associated with the tablet replica of the base table

State of alive task can be either:
- IDLE
- STARTED
- ABORTED

If the task doesn't exist in the state, this means it was finished or aborted.

View building tasks are created when:
- `build_range` tasks:
  - a view/index is created
  - during tablet operations
    - at the end of tablet migration / merge or split / keyspace RF change
    - during rollback if the operation failed
  - keyspace RF was increased
- `process_staging` tasks:
  - a staging sstable was registered to `view_building_worker`

A task might be aborted in two ways: by deleting it or by setting its state to `ABORTED`.
If a view/keyspace is dropped, then its tasks are aborted by deleting them as they are no longer needed.
On the other hand, at the begining of a tablet operation (migration/resize/RF change), relevant view building tasks are aborted using `ABORTED` state.
This intermediate state is needed to create new tasks at the end of the operation or in case of failure and rollback (aborted tasks are also deleted then).

The view building coordinator starts a task only if its tablet is not in transition (`tablet_map.get_tablet_transition_info(tid) == nullptr`).

View building task struct:
```c++
struct view_building_task {
    enum class task_type {
        build_range,
        process_staging,
    };

    enum class task_state {
        idle,
        started,
        aborted
    };
    utils::UUID id;
    task_type type;
    task_state state;

    table_id base_id;
    table_id view_id; // is default value when `type == task_type::process_staging`
    locator::tablet_replica replica;
    locator::tablet_id tid;
};
```

State machine:

```mermaid
stateDiagram-v2
    [*] --> IDLE
    IDLE --> [*]: aborted due to drop view
    IDLE --> STARTED: vbc chooses to work on the task
    STARTED --> ABORTED: aborted due to tablet operation
    STARTED --> [*]: done or aborted due to drop view
    IDLE --> ABORTED: aborted due to tablet operation
    ABORTED --> [*]
```

## Schema 

The most important table is `system.view_building_tasks`, which stores all unfinished view building tasks 
```sql
CREATE TABLE system.view_building_tasks (
    key text,
    id timeuuid,
    type text,
    state text,
    base_id uuid,
    view_id uuid,         -- NULL for "process_staging" tasks
    base_tablet_id bigint,
    host_id uuid,         -- Host of the tablet replica
    shard int,            -- Shard of the tablet replica
    PRIMARY KEY (key, id)
)
```

The view building coordinator stores currently processing base table in `system.scylla_local` 
under `view_building_processing_base` key. 
The entry is managed by group0.

The coordinator also updates view build statuses in `system.view_build_status_v2`.
When it selects new base table to process, it marks build statuses for all base table's views on all hosts as `STARTED`.
When there are no more task for the some view (for all hosts!) and there are no `process_staging` tasks for the base table,
then the view is marked as `SUCCESS` on all hosts.

Once the view is built, an entry in `system.built_views` is created. Before the view building coordinator,
this table was node-local one. But now the table is partially managed by group0, 
meaning that all entries from tablet-based keyspaces are managed by group0 and
entries from vnode-based keyspaces are still node local.

## View building worker

The view building worker is node-local service responsible for executing view building tasks.
It observers view building state machine and executed the tasks once they enter `STARTED` state.

The worker groups multiple view building tasks into a batch and it can execute only one batch per shard
(it's the coordinator responsibility to schedule only batch per tablet replica).

Tasks can be in one batch only if they have the same:
- type
- state (obviously it has to be STARTED)
- base_id
- tablet replica
- tablet id

### RPC

The view building worker doesn't mark tasks as finished (it doesn't do group0 operation with one exception).
Instead, it saves ids of finished and aborted tasks and it is the coordinator who asks the worker
what is the result of some tasks using following RPC call:

```c++
struct task_result {
    enum class command_status: uint8_t {
        success,
        abort,
    };
    service::view_build_coordinator::command_status status;
};

verb [[cancellable]] work_on_view_building_tasks(std::vector<utils::UUID> tasks_ids) -> std::vector<service::view_building::view_task_result>
```

The worker registers handler for the RPC, which:
- attaches to the tasks and waits for the result
- returns result when the tasks are finished/aborted

## Tablet operations

The view building coordinator needs to react to following tablet operations:
- tablet migration / move_tablet REST API
  - For each task which `base_id == tablet operation table_id` and `replica == source replica` and tablet ids are matched:
    - Abort the task
    - Create new task on destination replica
- tablet resize
  - For each task which `base_id == tablet operation table_id`:
    - In case of tablet split: abort the task and create new tasks with new tablet ids `n -> (2n, 2n+1)`
    - In case of tablet merge: abort the task and create new task with new tablet id `n -> n/2` if such task doesn't exist
- keyspace RF change / add_tablet_replica REST API / del_tablet_replica REST API
  - In case of RF decrease: abort tasks on abandoning replicas
  - In case of RF increase: create tasks for new replica 
    - (Maybe this can be optimized? It depends on how data is copied to the new replicas)

In all of the cases, the tasks are aborted at the start of the operation
and new tasks are created at the end.
In case of failure, new copies of aborted tasks are created during rollback.

## Staging sstables

The view building coordinator can also handle staging sstables using `process_staging` view building tasks.
We do this because we don't want to start generating view updates from a staging sstable prematurely,
before the writes are directed to the new replica (https://github.com/scylladb/scylladb/issues/19149).

Firstly, `db::view::check_needs_view_update_path()` now returns `db::view::sstable_destination_decision`,
instead of bool value determining if the sstable should go to base or staging directory.

```c++
enum class sstable_destination_decision {
    normal_directory,               // use normal sstable directory
    staging_directly_to_generator,  // use staging directory and notify view building worker
    staging_managed_by_vbc          // use staging directory and register the sstable to view update generator
};
```

For vnode-based sstables, the function works the same, but for tablet-based sstables the logic is:
- Does the table have any views?
  - NO: use normal directory
- Are all views not started or built yet?
  - YES: use normal directory
- Is any view started but not finished yet?
  - YES: create view building tasks for this sstable
- All views built
  - If the streaming reason is repair: create view building tasks for this sstable
  - Otherwise: use normal directory
