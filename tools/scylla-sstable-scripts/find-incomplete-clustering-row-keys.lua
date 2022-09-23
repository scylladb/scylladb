--
-- Copyright (C) 2022-present ScyllaDB
--
-- SPDX-License-Identifier: AGPL-3.0-or-later
--

-- Finds clustering rows which have incomplete (prefix) keys.
--
-- Such keys can be created in tables created with the `WITH COMPACT STORAGE`
-- legacy CQL option.
-- Found keys are printed to the standard output.

partition_key = nil

function format_key(key)
    key_str = ""
    for i, component in ipairs(key.components) do
        key_str = key_str..tostring(component)
        if i < #key.components then
            key_str = key_str..":"
        end
    end
    return key_str
end

function consume_partition_start(ps)
    partition_key = format_key(ps.key)
end

function consume_clustering_row(cr)
    if #cr.key.components < #schema.clustering_key_columns then
        print(string.format("Incomplete key in partition %s: %s (%s)", partition_key, format_key(cr.key), cr.key:to_hex()))
    end
end
