# Module map

Grounding for "module" and "submodule" when scoping a run. Directories are
verified against the current tree; exact file names inside them drift, so
the Scope and Research stages should confirm current structure via
`codegraph_explore` rather than trust a stale line number from here.

| Module | Top-level anchor(s) | Known submodules |
|---|---|---|
| Compaction | `compaction/` | ICS (`incremental_compaction_strategy.*`, `incremental_backlog_tracker.*`), TWCS (`time_window_compaction_strategy.*`), STCS (`size_tiered_compaction_strategy.*`, `size_tiered_backlog_tracker.hh`), LCS (`leveled_compaction_strategy.*`, `leveled_manifest.hh`), backlog/manager (`compaction_manager.*`, `compaction_backlog_manager.hh`) |
| Repair | `repair/` | Tablet/incremental repair (`row_level.cc`, `incremental.*`) — the active path. Legacy vnode repair (`repair.cc`) — **do not spend research/reproduction budget here**, see SKILL.md boundaries. |
| CQL | `cql3/`, `auth/`, `transport/` | Parser/grammar (`Cql.g`, `cql3/statements`), expressions (`cql3/expr`), functions (`cql3/functions`), authorization (`auth/`, permission-checking in `cql3/statements`), prepared-statement handling (`authorized_prepared_statements_cache.hh` and friends), wire protocol (`transport/`) |
| Memtables & cache | `replica/`, cache-related files under `replica/`/`db/` | Let Scope enumerate — memtable and row-cache boundaries move; don't hardcode paths |
| Read/write path | cross-cutting: `replica/`, `query/`, `mutation/`, `db/` | Cross-cutting by nature; treat as its own module only when the user explicitly asks for read-path or write-path, and let Scope trace the call path from the CQL/storage-proxy entry point instead of guessing file boundaries |
| Streaming / node ops | `streaming/`, `node_ops/` | Bootstrap, decommission, rebuild — let Scope enumerate |
| Tablets / locator | `locator/`, `service/` (tablet allocator, load balancer) | Tablet metadata, load balancing, migration — high scale-relevance (100k tablets is one of the four reference scale points) |
| Gossip / topology | `gms/`, `service/` | Fan-out cost per node/table is scale-relevant here |
| SSTables | `sstables/` | Index/summary structures, bloom filters, reader/writer — high scale-relevance (100k sstables/node reference point) |

If the user names something not in this table, that's fine — have the
Scope stage discover its structure via `codegraph_explore` rather than
refusing to proceed.
