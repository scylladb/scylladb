--
-- Copyright (C) 2024-present ScyllaDB
--
-- SPDX-License-Identifier: AGPL-3.0-or-later
--

-- Dumps all keys from an sstable
--
-- Only partition and cluster keys are dumped.
-- Example output -- excerpt from a run on an sstable from system_schema.columns:
--
-- [
--   {
--     "key": {
--       "token": "5501786289152180687",
--       "raw": "000d73797374656d5f747261636573",
--       "key_size": 15,
--       "value": "system_traces"
--     },
--     "clustering_elements": [
--       {
--         "type": "range-tombstone-change",
--         "key": {
--           "raw": "00066576656e7473",
--           "key_size": 8,
--           "value": "events"
--         }
--       },
--       {
--         "type": "clustering-row",
--         "key": {
--           "raw": "00066576656e747300086163746976697479",
--           "key_size": 18,
--           "value": "events:activity"
--         }
--       }
--     ]
--   }
-- ]

writer = Scylla.new_json_writer()

function write_key(obj)
    writer:start_object()

    if obj.token then
        writer:key("token")
        writer:string(tostring(obj.token))
    end

    raw_key = obj.key:to_hex()

    writer:key("raw")
    writer:string(raw_key)

    writer:key("key_size")
    writer:int(raw_key:len() / 2) -- to_hex() converts each byte to 2 characters

    writer:key("value")
    writer:string(tostring(obj.key))

    writer:end_object()
end

function consume_stream_start()
    writer:start_array()
end

function consume_partition_start(ps)
    writer:start_object()
    writer:key("key")
    write_key(ps)
    writer:key("clustering_elements")
    writer:start_array()
end

function consume_clustering_row(cr)
    writer:start_object()
    writer:key("type")
    writer:string("clustering-row")
    writer:key("key")
    write_key(cr)
    writer:end_object()
end

function consume_range_tombstone_change(crt)
    writer:start_object()
    writer:key("type")
    writer:string("range-tombstone-change")
    if crt.key then
        writer:key("key")
        write_key(crt)
    end
    writer:end_object()
end

function consume_partition_end()
    writer:end_array()
    writer:end_object()
end

function consume_stream_end()
    writer:end_array()
end
