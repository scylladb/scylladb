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
#include "range.hh"

namespace query {

const std::vector<range<clustering_key_prefix>>&
clustering_key_filtering_context::get_ranges(const partition_key& key) const {
    static thread_local std::vector<range<clustering_key_prefix>> full_range = {{}};
    return _factory ? _factory->get_ranges(key) : full_range;
}

clustering_key_filtering_context clustering_key_filtering_context::create_no_filtering() {
    return clustering_key_filtering_context{};
}

const clustering_key_filtering_context no_clustering_key_filtering =
    clustering_key_filtering_context::create_no_filtering();

class stateless_clustering_key_filter_factory : public clustering_key_filter_factory {
    clustering_key_filter _filter;
    std::vector<range<clustering_key_prefix>> _ranges;
public:
    stateless_clustering_key_filter_factory(std::vector<range<clustering_key_prefix>>&& ranges,
                                    clustering_key_filter&& filter)
        : _filter(std::move(filter)), _ranges(std::move(ranges)) {}

    virtual clustering_key_filter get_filter(const partition_key& key) override {
        return _filter;
    }

    virtual clustering_key_filter get_filter_for_sorted(const partition_key& key) override {
        return _filter;
    }

    virtual const std::vector<range<clustering_key_prefix>>& get_ranges(const partition_key& key) override {
        return _ranges;
    }
};

class partition_slice_clustering_key_filter_factory : public clustering_key_filter_factory {
    schema_ptr _schema;
    const partition_slice& _slice;
    clustering_key_prefix::prefix_equal_tri_compare _cmp;
public:
    partition_slice_clustering_key_filter_factory(schema_ptr s, const partition_slice& slice)
        : _schema(std::move(s)), _slice(slice), _cmp(*_schema) {}

    virtual clustering_key_filter get_filter(const partition_key& key) override {
        const clustering_row_ranges& ranges = _slice.row_ranges(*_schema, key);
        return [this, &ranges] (const clustering_key& key) {
            return std::any_of(std::begin(ranges), std::end(ranges),
                [this, &key] (const range<clustering_key_prefix>& r) { return r.contains(key, _cmp); });
        };
    }

    virtual clustering_key_filter get_filter_for_sorted(const partition_key& key) override {
        const clustering_row_ranges& ranges = _slice.row_ranges(*_schema, key);
        return [this, &ranges] (const clustering_key& key) {
            return std::any_of(std::begin(ranges), std::end(ranges),
                [this, &key] (const range<clustering_key_prefix>& r) { return r.contains(key, _cmp); });
        };
    }

    virtual const std::vector<range<clustering_key_prefix>>& get_ranges(const partition_key& key) override {
        return _slice.row_ranges(*_schema, key);
    }
};

static const shared_ptr<clustering_key_filter_factory>
create_partition_slice_filter(schema_ptr s, const partition_slice& slice) {
    return ::make_shared<partition_slice_clustering_key_filter_factory>(std::move(s), slice);
}

const clustering_key_filtering_context
clustering_key_filtering_context::create(schema_ptr schema, const partition_slice& slice) {
    static thread_local clustering_key_filtering_context accept_all = clustering_key_filtering_context(
        ::make_shared<stateless_clustering_key_filter_factory>(std::vector<range<clustering_key_prefix>>{{}},
                                                       [](const clustering_key&) { return true; }));
    static thread_local clustering_key_filtering_context reject_all = clustering_key_filtering_context(
        ::make_shared<stateless_clustering_key_filter_factory>(std::vector<range<clustering_key_prefix>>{},
                                                       [](const clustering_key&) { return false; }));

    if (slice.get_specific_ranges()) {
        return clustering_key_filtering_context(create_partition_slice_filter(schema, slice));
    }

    const clustering_row_ranges& ranges = slice.default_row_ranges();

    if (ranges.empty()) {
        return reject_all;
    }

    if (ranges.size() == 1 && ranges[0].is_full()) {
        return accept_all;
    }
    return clustering_key_filtering_context(create_partition_slice_filter(schema, slice));
}

}
