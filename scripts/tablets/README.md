<!--
Copyright (C) 2026-present ScyllaDB
SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-->

# Tablet Scripts

The scripts in this directory help inspect tablet placement, tablet load, and load-balancing state
in Scylla clusters.

## Design

The scripts are written in Python so they need no recompilation and are quick to extend with new
scripts. This is important when analyzing production incidents, where the ability to inspect a new aspect
on short notice is crucial.

### Snapshots

All analysis scripts work with either a **live** cluster or a **snapshot** of the cluster's state.

This supports offline and multi-step analysis where cluster access is limited (for example, during
production incidents) or where data must stay stable across steps. Because snapshots hold complete
topology information, they can also seed load-balancing simulations on customer topology.

A snapshot contains the captured state of system tables relevant for topology and load statistics, with one
CSV file per table. It should be cheap to capture.

`snapshot.py` captures a snapshot either by connecting to a live CQL server or by printing manual
`cqlsh` commands to run wherever cluster access is available.

### Live monitoring

The model also supports connecting to a live cluster, bypassing storing a snapshot on disk. This is
useful for quick inspection of the cluster where scripts are used in one-shot mode or as
live-monitors in a similar manner as `nodetool`.

`../tablet-mon.py` is an example of a live-monitoring use case.

## Tools

At a high level, the tools fall into groups:

- Internal table load tools: focus on the load shape inside a table, or on comparing tables to each
  other.
  - `table-load.py`: drill down into one table's token ranges, sizes, imbalance between replicas,
    and replica placement
  - `table-summary.py`: one row per table, useful for comparing tables to other tables
- Load distribution tools: focus on how load is spread across cluster resources.
  - `cluster-load.py`: datacenter, rack, node, and shard distribution of token load, size load, and
    utilization

Supporting tools:

- `snapshot.py`: capture a snapshot from a live cluster, or print manual `cqlsh COPY` commands

### Running the tools

Each tool can be run directly from the source tree:

```bash
./scripts/tablets/cluster-load.py
```

Scripts are also shipped with the scylla package and exposed in installs via dispatch wrapper: `scylla-tablets <tool> <args...>`.
For example, the above command is also:

```bash
scylla-tablets cluster-load
```

The `scylla-tablets` command aliases in installs to the `scylla-tablets.py` script.

### Snapshot capture

Use `./snapshot.py` (`scylla-tablets snapshot`) to capture a snapshot from a live cluster.

It chooses a timestamped name automatically and prints the absolute path to the snapshot on success.

The snapshot is a directory containing CSV files for each system table relevant to topology and load statistics:
```
  tablets_snap_YYMMDD_HHMMSS[_sss]/
  ├── system_tablets.csv
  ├── system_topology.csv
  ├── system_tablet_sizes.csv
  └── system_load_per_node.csv
```

You can capture a compressed tarball snapshot with `--gz`:

```bash
$ ./snapshot.py --gz
/home/tgrabiec/tablets_snap_260807_123456.tar.gz
```

All scripts accept a compressed tarball snapshot as input, and will decompress it on the fly.

If you don't have direct access to the cluster, you can use `--manual` to print the commands that capture a snapshot, to be run by someone who does:

```bash
$ ./snapshot.py --manual --gz -u scylla_admin -p xxxx
mkdir -p tablet_snap_260807_144540
cqlsh -u scylla_admin -p xxxx -e 'COPY system.topology TO '"'"'tablet_snap_260807_144540/system_topology.csv'"'"' WITH DELIMITER='"'"';'"'"' AND HEADER=TRUE'
cqlsh -u scylla_admin -p xxxx -e 'COPY system.tablets TO '"'"'tablet_snap_260807_144540/system_tablets.csv'"'"' WITH DELIMITER='"'"';'"'"' AND HEADER=TRUE'
cqlsh -u scylla_admin -p xxxx -e 'COPY system.load_per_node TO '"'"'tablet_snap_260807_144540/system_load_per_node.csv'"'"' WITH DELIMITER='"'"';'"'"' AND HEADER=TRUE'
cqlsh -u scylla_admin -p xxxx -e 'COPY system.tablet_sizes TO '"'"'tablet_snap_260807_144540/system_tablet_sizes.csv'"'"' WITH DELIMITER='"'"';'"'"' AND HEADER=TRUE'
tar -czf tablet_snap_260807_144540.tar.gz tablet_snap_260807_144540 && rm -r tablet_snap_260807_144540
```

### Common Interface

#### Data source

Most scripts share the same data-source pattern:

- `--cluster HOST` to read from a live cluster
- `--snapshot PATH` to read from a captured snapshot

When neither is given, the snapshot path is taken from the `SCYLLA_TABLET_SNAPSHOT` environment
variable. This avoids repeating `--snapshot` on every command while analyzing one snapshot:

```bash
export SCYLLA_TABLET_SNAPSHOT=tablet_snap_YYMMDD_HHMMSS
./cluster-load.py
```

With no snapshot from either source, the scripts connect to a live cluster on `localhost:9042`.

#### Filtering

Most analysis scripts accept narrowing aggregation based on location in the cluster:

- `--host <host-id|ip>`
- `--shard <host-id|ip>:<shard>` or `--shard <shard-number>` which selects `*:<shard-number>`
- `--rack <DC>/<rack>`
- `--dc <DC>`

Also, filtering by keyspace/table is commonly supported:
- `--table <keyspace.table|table-id|table>`.
- `--keyspace <keyspace>`

These filters narrow down tablet replicas considered by scripts. Only matching replicas contribute data to results.

The convention is that filters accept names in the same formats as scripts may print them.
For example, you can copy `<keyspace>.<table>` from `table-summary.py` output and pass it to `table-load.py`.

#### Presentation options

By default, the scripts present data in a human-friendly form:

- table names are shown as `keyspace.table`
- hosts are shown as IP addresses when available
- sizes are shown with binary units such as `Ki`, `Mi`, `Gi`
- data is shown in a table format

This can be changed with command-line flags:

- `--host-id`: show host ids instead of IPs
- `--table-id`: show table ids instead of `keyspace.table`
- `--no-hr`: disable human-readable size formatting
- `--csv`: output in CSV format instead of a table

You can pass `--anonymize` to anonymize customer-specific names in the snapshot, e.g. keyspace and table names.
The names will still be consistent for a given snapshot.
This is useful when sharing output publicly.

## Example Workflow

1.  Capture a snapshot and point the other scripts at it. `snapshot.py` prints the path of the
    snapshot it captured.

    ```bash
    export SCYLLA_TABLET_SNAPSHOT=$(./snapshot.py)
    ```

2.  Compare tables at a high level.

    ```bash
    ./table-summary.py
    ```

    Use this to find large tables, tables with uneven tablet sizes, or tables worth drilling into.

3.  Inspect cluster-wide distribution.

    ```bash
    ./cluster-load.py
    ```

    Use this to see whether load is concentrated on particular racks, nodes, or shards.

4.  Drill into one table.

    ```bash
    ./table-load.py ks.tbl
    ```

    Use this to inspect tablet boundaries, token-space coverage, replica sizes, and per-replica
    placement.

5. Inspect distribution of a single table across the cluster:

    ```bash
    ./cluster-load.py --table ks.tbl
    ```

   Use this to see if table's data or tablets are spread uniformly across the cluster.

## Load Measures

The scripts use a few related but distinct measures of load.

### Replicated vs unreplicated load

Load can be measured in two ways: before replication (unreplicated) or after replication
(replicated). It's often context-dependent which one is relevant.

For example, the name "tablet" can refer to two different things:

- a token-range unit before replication
- a replicated unit of a table, placed on a specific host and shard.

`table-load.py` and `table-summary.py` use `tablet` in the logical sense: token-range units before
replication. This is useful for understanding how many tablets a table has and how tablet boundaries
are distributed in token space.

`cluster-load.py` focuses on replicated load, so its count-based metrics are about tablet replicas:
placements of tablets on specific hosts and shards. This is the relevant count when comparing nodes
or shards for balancing purposes.

When there is ambiguity, the scripts should explicitly say "replicated" or "unreplicated" in the
output.

### Token load

Token load is the share of token space owned by an entity. It answers: how much of the ring is
assigned here?

Token ownership can differ from the actual share of key ownership, if present keys are not spread
uniformly in the token space.

Like tablet count, tokens can be counted before replication (token load) or after replication
(replicated token load), and which one is relevant is context-dependent.

For replicated data, token load can exceed `100%` when summed across racks, because the measure
counts every replica copy.

### Storage utilization

Storage utilization is size load divided by storage capacity.

We distinguish between absolute storage capacity and effective capacity. Effective capacity is the
part of storage capacity that can be used for storing data by tablets. It's computed by scylla server as free space
plus the size of tablets currently on the node.

### Overcommit (OVC)

Overcommit is a measure of imbalance.

It answers: how far is a node, shard, rack, or token range from the average of its peers?

Computed as:

    OVC[%] = (value - peer_average) / peer_average * 100%

The scripts present OVC as percentage deviation from the peer average, shown as `[%]`:

- `0%`: exactly at the peer average
- positive value: above the peer average
- negative value: below the peer average

Maximum OVC within the group is used as a measure of how imbalanced the whole group is. When scripts
show aggregated OVC, they show the maximum OVC within the group.

The average OVC within the peer group is always `0%` and maximum OVC is always >= 0%.

Different tools apply OVC at different levels:

- `table-load.py`: imbalance between tablet or token-range rows within a table
- `table-summary.py`: imbalance between tablets in token space for a table
- `cluster-load.py`: imbalance between peer racks, nodes, or shards in the selected section

## Running tests

### Pure python unit tests

The tests live in `test/tablet_scripts/`. They exercise the scripts as Python modules, without a
Scylla server, so the whole suite runs in a few seconds.

Run it like any other Scylla test suite, from the repository root:

```bash
tools/toolchain/dbuild ./test.py --mode dev test/tablet_scripts --no-gather-metrics
```

`--no-gather-metrics` is needed under `dbuild`, where metrics gathering fails trying to create a
cgroup directory the container cannot see.

### Cluster tests

`test/cluster/test_tablet_snapshot.py` verifies the topology sources against a real cluster, in particular
snapshot collection.

```bash
tools/toolchain/dbuild ./test.py --mode dev test/cluster/test_tablet_snapshot.py --no-gather-metrics
```
