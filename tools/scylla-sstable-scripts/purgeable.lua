--
-- Copyright (C) 2025-present ScyllaDB
--
-- SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
--

-- Print statistics on what how much data is purgeable in the sstable(s).
--
-- Assumes tombstone_gc = {'mode': 'timeout'}
-- gc_grace_seconds is hardcoded below, see the tombstone_gc_grace_seconds variable.
-- Can be overridden by setting the GC_GRACE_SECONDS environment variable.

partition = {total=0, live=0, purgeable=0}
partitions = {total=0, live=0, purgeable=0}
rows = {total=0, live=0, purgeable=0}
cells = {total=0, live=0, purgeable=0}
tombstones = {total=0, live=0, purgeable=0}

now = os.time()
tombstone_gc_grace_seconds = 1 * 24 * 60 * 60 -- 10 days

gc_grace_seconds_env = os.getenv('GC_GRACE_SECONDS')
if gc_grace_seconds_env then
    tombstone_gc_grace_seconds = gc_grace_seconds_env
end

function handle_tombstone(tomb, stats)
    stats.total = stats.total + 1
    if os.time(tomb.deletion_time) + tombstone_gc_grace_seconds < now then
        stats.purgeable = stats.purgeable + 1
    end
end

function handle_atomic_cell(cell)
    if cell.is_live then
        cells.total = cells.total + 1
        cells.live = cells.live + 1
    else
        handle_tombstone(cell.tombstone, tombstones)
    end
end

function handle_collection(cell)
    if cell.tombstone then
        handle_tombstone(cell.tombstone, tombstones)
    end
    for _, v in ipairs(cell.values) do
        handle_atomic_cell(v.value)
    end
end

function handle_cells(cells, has_live_marker)
    rows.total = rows.total + 1
    partition.total = partition.total + 1

    total = 0
    live = 0
    purgeable = 0

    for name, cell in pairs(cells) do
        if cell.type == "collection" then
            handle_collection(cell)
        else
            handle_atomic_cell(cell)
        end
    end

    if live > 0 or has_live_marker then
        rows.live = rows.live + 1
        partition.live = partition.live + 1
    elseif purgeable == total then
        rows.purgeable = rows.purgeable + 1
        partition.purgeable = partition.purgeable + 1
    else
        -- dead, can be derived from total - (live + purgeable)
    end
end

function consume_sstable_start(sst)
    if sst == nil then
        print("Sstable (combined)")
    else
        print("Sstable " .. sst.filename)
    end
end

function consume_partition_start(ps)
    partitions.total = partitions.total + 1
    partition = {total = 0, live = 0, purgeable = 0}
    if ps.tombstone then
        handle_tombstone(ps.tombstone, tombstones)
    end
end

function consume_static_row(sr)
    handle_cells(sr.cells, false)
end

function consume_clustering_row(cr)
    if cr.tombstone then
        handle_tombstone(cr.tombstone, tombstones)
        handle_tombstone(cr.shadowable_tombstone, tombstones)
    end
    handle_cells(cr.cells, cr.marker and cr.marker.is_live)
end

function consume_range_tombstone_change(rtc)
    if rtc.tombstone then
        handle_tombstone(rtc.tombstone, tombstones)
    end
end

function consume_partition_end()
    if partition.live > 0 then
        partitions.live = partitions.live + 1
    elseif partition.purgeable == partition.total then
        partitions.purgeable = partitions.purgeable + 1
    end
end

function consume_sstable_end()
    print(string.format("             %9s %9s %9s %9s", "total", "live", "dead", "purgeable"))
    print(string.format("  cells      %9d %9d %9d %9d", cells.total, cells.live, cells.total - (cells.live + cells.purgeable), cells.purgeable))
    print(string.format("  tombstones %9d %9d %9d %9d", tombstones.total, tombstones.live, tombstones.total - (tombstones.live + tombstones.purgeable), tombstones.purgeable))
    print(string.format("  rows       %9d %9d %9d %9d", rows.total, rows.live, rows.total - (rows.live + rows.purgeable), rows.purgeable))
    print(string.format("  partitions %9d %9d %9d %9d", partitions.total, partitions.live, partitions.total - (partitions.live + partitions.purgeable), partitions.purgeable))
end
