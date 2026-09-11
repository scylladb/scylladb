/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/log.hh"
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
    update_min(pos);
    update_max(pos);
}

void metadata_collector::update_min(position_in_partition_view pos) {
    if (pos.region() != partition_region::clustered) {
        throw std::runtime_error(fmt::format("update_min() expects positions in the clustering region, got {}", pos));
    }

    const position_in_partition::tri_compare cmp(_schema);

    // Positions are compared in natural order, prefix (non-full) keys included.
    // Only the winner's key components are stored (see convert()); the reader
    // extends them back to a range covering all prefixed keys, in
    // sstable::set_min_max_position_range(), so prefixes need no special
    // treatment here.

    if (!_min_clustering_pos || cmp(pos, *_min_clustering_pos) < 0) {
        mdclogger.trace("{}: setting min_clustering_key={}", _name, position_in_partition_view::printer(_schema, pos));
        _min_clustering_pos.emplace(pos);
    }
}

void metadata_collector::update_max(position_in_partition_view pos) {
    if (pos.region() != partition_region::clustered) {
        throw std::runtime_error(fmt::format("update_max() expects positions in the clustering region, got {}", pos));
    }

    const position_in_partition::tri_compare cmp(_schema);

    if (!_max_clustering_pos || cmp(pos, *_max_clustering_pos) > 0) {
        mdclogger.trace("{}: setting max_clustering_key={}", _name, position_in_partition_view::printer(_schema, pos));
        _max_clustering_pos.emplace(pos);
    }
}

} // namespace sstables
