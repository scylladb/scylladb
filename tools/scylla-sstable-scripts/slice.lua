--
-- Copyright (C) 2022-present ScyllaDB
--
-- SPDX-License-Identifier: AGPL-3.0-or-later
--

-- Filters and dumps the content of the sstable(s).
--
-- With no arguments, this script is identical to the dump-data operation.
-- It demonstrates how to use the high-level JSON write API, as well as how to
-- work with partition and clustering keys and how to accepts arguments from the
-- command-line
-- The script accepts two kind of arguments from the command-line: partition
-- ranges and clustering ranges.
-- Partition ranges are expected to have keys with the format of `prN`, where N
-- is an integer. N can have any value but it should be unique across all other
-- partition-ranges passed to the script. A simple scheme is to use a running
-- counter to number them.
-- Clustering ranges are expected to have keys with the format of `crN`. The same
-- restrictions apply to `N`.
-- The ranges themselves have the format:
-- * [X,Y] - inclusive range from X to Y
-- * [X,Y) - from X (inclusive) to Y (exclusive)
-- * (X,Y] - from X (exclusive) to Y (inclusive)
-- * (X,Y) - exclusive range from X to Y
--
-- The key values should be hex encoded serialized keys. For partition ranges, it
-- is possible to pass tokens, as `tTOKEN`. The special values of `-inf` and
-- `+inf` can be used to denote infinity, but note that infinity should always be
-- an exclusive bound.
--
-- Examples:
--
-- # a single partition range, from key 000400000005 to inf
-- $ scylla sstable script --script-file slice.lua --script-args "pk0=[000400000005,+inf)"
--
-- # two partition ranges
-- $ scylla sstable script --script-file slice.lua --script-args "pk0=(-inf,000400000002):pk1=[000400000005,+inf)"
--
-- # token-range
-- $ scylla sstable script --script-file slice.lua --script-args "pk0=(t-1000,t1000)"
--
-- # a single clustering range, from key 000400000005 to inf
-- $ scylla sstable script --script-file slice.lua --script-args "ck0=[000400000005,+inf)"
--
-- # partition (mixed key and token) and clustering range
-- $ scylla sstable script --script-file slice.lua --script-args "pk0=(000400000001,t1000):ck0=[000400000005,+inf)"

wr = Scylla.new_json_writer()

partition_ranges = {}
clustering_ranges = {}

arg_key_pattern = "^([pc]r)(%d*)$"
arg_value_pattern = "^([%(%[])(.+),(.+)([%]%)])$"
key_pattern = "^([0-9a-f]+)$"
token_pattern = "^t(-?%d+)$"

paren_to_key_weight = {
    ["["] = 0,
    ["]"] = 0,
    ["("] = 1,
    [")"] = -1,
}

paren_to_token_weight = {
    ["["] = -1,
    ["]"] = 1,
    ["("] = 1,
    [")"] = -1,
}

function make_ring_position(paren, bound)
    if bound == '-inf' or bound == 'inf' or bound == '+inf' then
        return Scylla.new_ring_position(paren_to_token_weight[paren], nil)
    end

    local serialized_key = string.match(bound, key_pattern)
    if serialized_key ~= nil then
        return Scylla.new_ring_position(paren_to_key_weight[paren], Scylla.unserialize_partition_key(serialized_key))
    end

    local token = string.match(bound, token_pattern)
    if token ~= nil then
        return Scylla.new_ring_position(paren_to_token_weight[paren], tonumber(token))
    end

    error(string.format("failed to parse %s as a partition-range bound, expected t$TOKEN, +-inf or a serialized key value", bound))
end

function make_position_in_partition(paren, bound)
    local weight = paren_to_key_weight[paren]

    if bound == '-inf' or bound == 'inf' or bound == '+inf' then
        return Scylla.new_position_in_partition(weight, nil)
    end

    local serialized_key = string.match(bound, key_pattern)
    if serialized_key ~= nil then
        return Scylla.new_position_in_partition(weight, Scylla.unserialize_clustering_key(serialized_key))
    end

    error(string.format("failed to parse %s as a clustering-range bound, expected +-inf or a serialized key value", bound))
end

function parse_ranges(args)
    for k, v in pairs(args) do
        local kind, index = string.match(k, arg_key_pattern)
        if kind == nil then
            error(string.format("failed to parse command line argument key: %s", k))
        end
        local start_paren, start_bound, end_bound, end_paren = string.match(v, arg_value_pattern)
        if start_paren == nil then
            error(string.format("failed to parse command line argument value for key %s: %s", k, v))
        end
        if kind == 'pr' then
            partition_ranges[#partition_ranges + 1] = {make_ring_position(start_paren, start_bound), make_ring_position(end_paren, end_bound)}
        else
            clustering_ranges[#clustering_ranges + 1] = {make_position_in_partition(start_paren, start_bound), make_position_in_partition(end_paren, end_bound)}
        end
    end
end

function filter(point, ranges)
    if #ranges == 0 then
        return true
    end
    for _, range in ipairs(ranges) do
        if range[1]:tri_cmp(point) <= 0 and range[2]:tri_cmp(point) >= 0 then
            return true
        end
    end
    return false
end

function consume_stream_start(args)
    parse_ranges(args)
    wr:start_stream()
end

function consume_sstable_start(sst)
    wr:start_sstable(sst)
end

skip_partition = false

function consume_partition_start(ps)
    skip_partition = not filter(Scylla.new_ring_position(0, ps.key, ps.token), partition_ranges)
    if skip_partition then
        return false
    end
    wr:start_partition(ps)
end

function consume_static_row(sr)
    wr:static_row(sr)
end

function consume_clustering_row(cr)
    if filter(Scylla.new_position_in_partition(0, cr.key), clustering_ranges) then
        wr:clustering_row(cr)
    end
end

function consume_range_tombstone_change(rtc)
    wr:range_tombstone_change(rtc)
end

function consume_partition_end()
    if skip_partition then
        return
    end
    wr:end_partition()
end

function consume_sstable_end()
    wr:end_sstable()
end

function consume_stream_end()
    wr:end_stream()
end

