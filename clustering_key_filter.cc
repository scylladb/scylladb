/*
 * Copyright (C) 2016 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "clustering_key_filter.hh"
#include "keys.hh"
#include "query-request.hh"

namespace query {

const clustering_row_ranges&
clustering_key_filtering_context::get_ranges(const partition_key& key) const {
    static thread_local clustering_row_ranges full_range = {{}};
    return _factory ? _factory->get_ranges(key) : full_range;
}

clustering_key_filtering_context clustering_key_filtering_context::create_no_filtering() {
    return clustering_key_filtering_context{};
}

const clustering_key_filtering_context no_clustering_key_filtering =
    clustering_key_filtering_context::create_no_filtering();

class partition_slice_clustering_key_filter_factory : public clustering_key_filter_factory {
    schema_ptr _schema;
    const partition_slice& _slice;
    clustering_row_ranges _ck_ranges;
public:
    partition_slice_clustering_key_filter_factory(schema_ptr s, const partition_slice& slice)
        : _schema(std::move(s)), _slice(slice) {}

    virtual const clustering_row_ranges& get_ranges(const partition_key& key) override {
        if (_slice.options.contains(query::partition_slice::option::reversed)) {
            _ck_ranges = _slice.row_ranges(*_schema, key);
            std::reverse(_ck_ranges.begin(), _ck_ranges.end());
            return _ck_ranges;
        }
        return _slice.row_ranges(*_schema, key);
    }
};

static const shared_ptr<clustering_key_filter_factory>
create_partition_slice_filter(schema_ptr s, const partition_slice& slice) {
    return ::make_shared<partition_slice_clustering_key_filter_factory>(std::move(s), slice);
}

const clustering_key_filtering_context
clustering_key_filtering_context::create(schema_ptr schema, const partition_slice& slice) {
    return clustering_key_filtering_context(create_partition_slice_filter(schema, slice));
}

}
