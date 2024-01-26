--
-- Copyright (C) 2024-present ScyllaDB
--
-- SPDX-License-Identifier: AGPL-3.0-or-later
--

-- Find the largest key in an sstable
--
-- Only partition and cluster keys are considered.
-- Example output -- a run on an sstable from system_schema.columns:
--
-- Largest key is of size 52, found in partition system
--         token: 2008276574632865675
--         raw partition key: 000673797374656d
--         mutation fragment kind: clustering-row
--         key: scylla_views_builds_in_progress:generation_number
--         raw key: 001f7363796c6c615f76696577735f6275696c64735f696e5f70726f6772657373001167656e65726174696f6e5f6e756d626572

current_partition = nil
largest_key = {
    size=0,
    partition=nil,
    fragment_kind=nil,
    raw=nil,
    value=nil,
}

function handle_key(obj, fragment_kind)
    raw_key = obj.key:to_hex()
    key_size = raw_key:len() / 2 -- to_hex() converts each byte to 2 characters

    if key_size <= largest_key.size then
        return
    end

    largest_key.size = key_size
    largest_key.partition = current_partition
    largest_key.fragment_kind = fragment_kind

    if obj.token then
        largest_key.raw = nil
        largest_key.value = nil
    else
        largest_key.raw = raw_key
        largest_key.value = tostring(obj.key)
    end
end

function consume_partition_start(ps)
    current_partition = {token = ps.token, raw = ps.key:to_hex(), value = tostring(ps.key)}
    handle_key(ps, "partition-start")
end

function consume_clustering_row(cr)
    handle_key(cr, "clustering-row")
end

function consume_range_tombstone_change(crt)
    if crt.key then
        handle_key(crt, "range-tombstone-change")
    end
end

function consume_stream_end()
    print(string.format("Largest key is of size %d, found in partition %s",
        largest_key.size,
        largest_key.partition.value))
    print(string.format("\ttoken: %s", largest_key.partition.token))
    print(string.format("\traw partition key: %s", largest_key.partition.raw))
    print(string.format("\tmutation fragment kind: %s", largest_key.fragment_kind))
    if largest_key.raw and largest_key.value then
        print(string.format("\tkey: %s", largest_key.value))
        print(string.format("\traw key: %s", largest_key.raw))
    end
end
