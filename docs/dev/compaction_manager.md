# The Compaction Manager

We model different maintenance operations around SSTables as different kinds of
compaction task executor. What they do is read SSTables, process them and write the
processed version.

```mermaid
classDiagram
    class compaction_task_executor {
        +run_compaction()
        #do_run()
    }

    compaction_task_executor <|-- sstable_task_executor
    sstable_task_executor <|-- rewrite_sstables_compaction_task_executor
    sstable_task_executor <|-- validate_sstables_compaction_task_executor

    compaction_task_executor <|-- major_compaction_task_executor
    compaction_task_executor <|-- custom_compaction_task_executor
    compaction_task_executor <|-- regular_compaction_task_executor

    class compaction_manager {
        +submit(table_state)
        +perform_sstable_upgrade(table_state)
        +perform_sstable_scrub(table_state)
        +perform_major_compaction(table_state)
        +run_custom_job(table_state, job)
        -list~compaction_task_executor~ _tasks
        -unordered_map~shared_sstable~ _compacting_sstables
        #do_run()
    }
    compaction_manager "1" --* "*" compaction_task_executor : manages
```

`compaction_manager` keeps all the ongoing compaction tasks in its task queue,
so that it is able to check if a certain operation is being performed on
given table.

When a task is about to run, the `compaction_manager` sets its state to `none`:

```mermaid
---
compaction task states
---

stateDiagram-v2
    [*] --> none
    none --> pending: the compaction is not disabled, will acquire the read lock
    none --> [*]: compaction is disabled or we are stopping
    pending --> [*]: the compaction is disabled, or there is no sstables to compact
    pending --> active: read lock acquired, let's get down to business
    pending --> postponed: the efficiency of the compaction is not worth it
    postponed --> [*]
    active --> done: hooray!
    active --> failed: an exception is thrown when consuming the input sstables
    done --> [*]
    failed --> [*]
```

But the regular compaction is an exception, it repeats the compaction by itself until
nothing is worthy compacting. In other words, it does not finish the compaction when
its state moves to `done` , instead, it moves back to `pending` and check if there
is any sstable to be compacted, and return only if there is no more jobs.
