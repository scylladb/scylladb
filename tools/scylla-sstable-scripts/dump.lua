--
-- Copyright (C) 2022-present ScyllaDB
--
-- SPDX-License-Identifier: AGPL-3.0-or-later
--

-- Dumps the content of the sstable(s).
--
-- Mirrors the dump-data operation. Useful for testing the lua bindings and
-- showcasing how to use the lua API to traverse all corners of the data, as well
-- as how to generate custom JSON.
-- For dumping the content of sstables, prefer the dump-data operation, it is
-- much more performant.

writer = Scylla.new_json_writer()

clustering_array_created = false

function write_key(obj)
    writer:start_object()

    if obj.token then
        writer:key("token")
        writer:string(tostring(obj.token))
    end

    writer:key("raw")
    writer:string(obj.key:to_hex())

    writer:key("value")
    writer:string(tostring(obj.key))

    writer:end_object()
end

function write_tombstone(tombstone)
    writer:start_object()

    if tombstone then
        writer:key("timestamp")
        writer:int(tombstone.timestamp)

        writer:key("deletion_time")
        writer:string(tostring(tombstone.deletion_time))
    end

    writer:end_object()
end

function write_ttl(obj)
    writer:key("ttl")
    writer:string(string.format("%is", obj.ttl))
    writer:key("expiry")
    writer:string(tostring(obj.expiry))
end

function maybe_start_clustering_array()
    if clustering_array_created then
        return
    end
    writer:key("clustering_elements")
    writer:start_array()
    clustering_array_created = true
end

function write_atomic_cell(cell)
    writer:key("is_live")
    writer:bool(cell.is_live)

    writer:key("type")
    writer:string(cell.type)

    writer:key("timestamp")
    writer:int(cell.timestamp)

    if cell.type == "counter-shards" then
        writer:key("value")
        writer:start_array()
        for _, shard in ipairs(cell.value.shards) do
            writer:start_object()
            writer:key("id")
            writer:string(shard.id)
            writer:key("value")
            writer:int(shard.value)
            writer:key("clock")
            writer:int(shard.clock)
            writer:end_object()
        end
        writer:end_array()
    elseif cell.is_live then -- type == "regular" | "frozen-collection" | "counter-update"
        writer:key("value")
        writer:string(tostring(cell.value))
    end

    if cell.is_live and cell.has_ttl then
        write_ttl(cell)
    end
    if not cell.is_live then
        writer:key("deletion_time")
        writer:string(tostring(cell.deletion_time))
    end
end

function write_collection(cell)
    if cell.tombstone then
        writer:key("tombstone")
        write_tombstone(cell.tombstone)
    end
    writer:key("cells")
    writer:start_array()
    for _, v in ipairs(cell.values) do
        writer:start_object()

        writer:key("key")
        writer:string(tostring(v.key))

        writer:key("value")
        writer:start_object()
        write_atomic_cell(v.value)
        writer:end_object()

        writer:end_object()
    end
    writer:end_array()
end

function write_cells(cells)
    writer:start_object()

    for name, cell in pairs(cells) do
        writer:key(name)
        writer:start_object()

        if cell.type == "collection" then
            write_collection(cell)
        else
            write_atomic_cell(cell)
        end

        writer:end_object()
    end

    writer:end_object()
end

function consume_stream_start()
    writer:start_object()
    writer:key("sstables")
    writer:start_object()
end

function consume_sstable_start(sst)
    if sst == nil then
        writer:key("anonymous")
    else
        writer:key(sst.filename)
    end
    writer:start_array()
end

function consume_partition_start(ps)
    writer:start_object()

    clustering_array_created = false

    writer:key("key")
    write_key(ps)

    if ps.tombstone then
        writer:key("tombstone")
        write_tombstone(ps.tombstone)
    end
end

function consume_static_row(sr)
    writer:key("static_row")
    write_cells(sr.cells)
end

function consume_clustering_row(cr)
    maybe_start_clustering_array()

    writer:start_object()

    writer:key("type")
    writer:string("clustering-row")

    writer:key("key")
    write_key(cr)

    if cr.tombstone then
        writer:key("tombstone")
        write_tombstone(cr.tombstone)
        writer:key("shadowable_tombstone")
        write_tombstone(cr.shadowable_tombstone)
    end

    if cr.marker then
        writer:key("marker")
        writer:start_object()

        writer:key("timestamp")
        writer:int(cr.marker.timestamp)

        if cr.marker.is_live and cr.marker.has_ttl then
            write_ttl(cr.marker)
        end

        writer:end_object()
    end

    writer:key("columns")
    write_cells(cr.cells)

    writer:end_object()
end

function consume_range_tombstone_change(crt)
    maybe_start_clustering_array()

    writer:start_object()

    writer:key("type")
    writer:string("range-tombstone-change")

    if crt.key then
        writer:key("key")
        write_key(crt)
    end

    writer:key("weight")
    writer:int(crt.key_weight)

    writer:key("tombstone")
    write_tombstone(crt.tombstone)

    writer:end_object()
end

function consume_partition_end()
    if clustering_array_created then
        writer:end_array()
    end
    writer:end_object()
end

function consume_sstable_end()
    writer:end_array()
end

function consume_stream_end()
    writer:end_object()
    writer:end_object()
end
