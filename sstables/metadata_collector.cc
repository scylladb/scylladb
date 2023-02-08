/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "log.hh"
#include "metadata_collector.hh"
#include "mutation/position_in_partition.hh"

logging::logger mdclogger("metadata_collector");

namespace sstables {

void metadata_collector::convert(disk_array<uint32_t, disk_string<uint16_t>>& to, const std::optional<position_in_partition>& from) {
    if (!from) {
        mdclogger.trace("{}: convert: empty", _name);
        return;
    }
    mdclogger.trace("{}: convert: {}", _name, position_in_partition_view::printer(_schema, *from));
    for (auto& value : from->key().components()) {
        to.elements.push_back(disk_string<uint16_t>{to_bytes(value)});
    }
}

void metadata_collector::update_min_max_components(position_in_partition_view pos) {
    if (pos.region() != partition_region::clustered) {
        throw std::runtime_error(fmt::format("update_min_max_components() expects positions in the clustering region, got {}", pos));
    }

    const position_in_partition::tri_compare cmp(_schema);

    // We need special treatment for non-full clustering row keys.
    // We want to treat these like a range: {before_key(pos), after_key(pos)}
    // for the purpose of calculating min and max respectively.
    // This is how callers expect prefixes to be interpreted.
    const auto is_prefix_row = pos.is_clustering_row() && !pos.key().is_full(_schema);
    const auto min_pos = is_prefix_row ? position_in_partition_view::before_key(pos) : pos;
    const auto max_pos = is_prefix_row ? position_in_partition_view::after_all_prefixed(pos) : pos;

    if (!_min_clustering_pos || cmp(min_pos, *_min_clustering_pos) < 0) {
        mdclogger.trace("{}: setting min_clustering_key={}", _name, position_in_partition_view::printer(_schema, min_pos));
        _min_clustering_pos.emplace(min_pos);
    }

    if (!_max_clustering_pos || cmp(max_pos, *_max_clustering_pos) > 0) {
        mdclogger.trace("{}: setting max_clustering_key={}", _name, position_in_partition_view::printer(_schema, max_pos));
        _max_clustering_pos.emplace(max_pos);
    }
}

} // namespace sstables
