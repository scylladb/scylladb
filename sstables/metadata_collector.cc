/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "log.hh"
#include "metadata_collector.hh"
#include "range_tombstone.hh"

logging::logger mdclogger("metadata_collector");

namespace sstables {

void metadata_collector::convert(disk_array<uint32_t, disk_string<uint16_t>>& to, const std::optional<clustering_key_prefix>& from) {
    if (!from) {
        mdclogger.trace("{}: convert: empty", _name);
        return;
    }
    mdclogger.trace("{}: convert: {}", _name, clustering_key_prefix::with_schema_wrapper(_schema, *from));
    for (auto& value : from->components()) {
        to.elements.push_back(disk_string<uint16_t>{to_bytes(value)});
    }
}

void metadata_collector::update_min_max_components(const clustering_key_prefix& key) {
    if (!_min_clustering_key) {
        mdclogger.trace("{}: initializing min/max clustering keys={}", _name, clustering_key_prefix::with_schema_wrapper(_schema, key));
        _min_clustering_key.emplace(key);
        _max_clustering_key.emplace(key);
        return;
    }

    const bound_view::tri_compare cmp(_schema);

    auto res = cmp(bound_view(key, bound_kind::incl_start), bound_view(*_min_clustering_key, bound_kind::incl_start));
    if (res < 0) {
        mdclogger.trace("{}: setting min_clustering_key={}", _name, clustering_key_prefix::with_schema_wrapper(_schema, key));
        _min_clustering_key.emplace(key);
    }

    res = cmp(bound_view(key, bound_kind::incl_end), bound_view(*_max_clustering_key, bound_kind::incl_end));
    if (res > 0) {
        mdclogger.trace("{}: setting max_clustering_key={}", _name, clustering_key_prefix::with_schema_wrapper(_schema, key));
        _max_clustering_key.emplace(key);
    }
}

void metadata_collector::update_min_max_components(const range_tombstone& rt) {
    const bound_view::tri_compare cmp(_schema);

    if (!_min_clustering_key) {
        mdclogger.trace("{}: initializing min_clustering_key to rt.start={}", _name, clustering_key_prefix::with_schema_wrapper(_schema, rt.start));
        _min_clustering_key.emplace(rt.start);
    } else if (cmp(rt.start_bound(), bound_view(*_min_clustering_key, bound_kind::incl_start)) < 0) {
        mdclogger.trace("{}: updating min_clustering_key to rt.start={}", _name, clustering_key_prefix::with_schema_wrapper(_schema, rt.start));
        _min_clustering_key.emplace(rt.start);
    }

    if (!_max_clustering_key) {
        mdclogger.trace("{}: initializing max_clustering_key to rt.end={}", _name, clustering_key_prefix::with_schema_wrapper(_schema, rt.end));
        _max_clustering_key.emplace(rt.end);
    } else if (cmp(rt.end_bound(), bound_view(*_max_clustering_key, bound_kind::incl_end)) > 0) {
        mdclogger.trace("{}: updating max_clustering_key to rt.end={}", _name, clustering_key_prefix::with_schema_wrapper(_schema, rt.end));
        _max_clustering_key.emplace(rt.end);
    }
}

} // namespace sstables
