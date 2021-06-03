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

#pragma once

#include "schema_fwd.hh"
#include "position_in_partition.hh"
#include <boost/icl/interval_set.hpp>

// Represents a non-contiguous subset of clustering_key domain of a particular schema.
// Can be treated like an ordered and non-overlapping sequence of position_range:s.
class clustering_interval_set {
    // Needed to make position_in_partition comparable, required by boost::icl::interval_set.
    class position_in_partition_with_schema {
        schema_ptr _schema;
        position_in_partition _pos;
    public:
        position_in_partition_with_schema()
            : _pos(position_in_partition::for_static_row())
        { }
        position_in_partition_with_schema(schema_ptr s, position_in_partition pos)
            : _schema(std::move(s))
            , _pos(std::move(pos))
        { }
        bool operator<(const position_in_partition_with_schema& other) const {
            return position_in_partition::less_compare(*_schema)(_pos, other._pos);
        }
        bool operator==(const position_in_partition_with_schema& other) const {
            return position_in_partition::equal_compare(*_schema)(_pos, other._pos);
        }
        const position_in_partition& position() const { return _pos; }
    };
private:
    // We want to represent intervals of clustering keys, not position_in_partitions,
    // but clustering_key domain is not enough to represent all kinds of clustering ranges.
    // All intervals in this set are of the form [x, y).
    using set_type = boost::icl::interval_set<position_in_partition_with_schema>;
    using interval = boost::icl::interval<position_in_partition_with_schema>;
    set_type _set;
public:
    clustering_interval_set() = default;
    // Constructs from legacy clustering_row_ranges
    clustering_interval_set(const schema& s, const query::clustering_row_ranges& ranges) {
        for (auto&& r : ranges) {
            add(s, position_range::from_range(r));
        }
    }
    query::clustering_row_ranges to_clustering_row_ranges() const {
        query::clustering_row_ranges result;
        for (position_range r : *this) {
            result.push_back(query::clustering_range::make(
                {r.start().key(), r.start()._bound_weight != bound_weight::after_all_prefixed},
                {r.end().key(), r.end()._bound_weight == bound_weight::after_all_prefixed}));
        }
        return result;
    }
    class position_range_iterator {
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = const position_range;
        using difference_type = std::ptrdiff_t;
        using pointer = const position_range*;
        using reference = const position_range&;
    private:
        set_type::iterator _i;
    public:
        position_range_iterator(set_type::iterator i) : _i(i) {}
        position_range operator*() const {
            // FIXME: Produce position_range view. Not performance critical yet.
            const interval::interval_type& iv = *_i;
            return position_range{iv.lower().position(), iv.upper().position()};
        }
        bool operator==(const position_range_iterator& other) const { return _i == other._i; }
        bool operator!=(const position_range_iterator& other) const { return _i != other._i; }
        position_range_iterator& operator++() {
            ++_i;
            return *this;
        }
        position_range_iterator operator++(int) {
            auto tmp = *this;
            ++_i;
            return tmp;
        }
    };
    static interval::type make_interval(const schema& s, const position_range& r) {
        assert(r.start().has_clustering_key());
        assert(r.end().has_clustering_key());
        return interval::right_open(
            position_in_partition_with_schema(s.shared_from_this(), r.start()),
            position_in_partition_with_schema(s.shared_from_this(), r.end()));
    }
public:
    bool equals(const schema& s, const clustering_interval_set& other) const {
        return boost::equal(_set, other._set);
    }
    bool contains(const schema& s, position_in_partition_view pos) const {
        // FIXME: Avoid copy
        return _set.find(position_in_partition_with_schema(s.shared_from_this(), position_in_partition(pos))) != _set.end();
    }
    // Returns true iff this set is fully contained in the other set.
    bool contained_in(clustering_interval_set& other) const {
        return boost::icl::within(_set, other._set);
    }
    bool overlaps(const schema& s, const position_range& range) const {
        // FIXME: Avoid copy
        auto r = _set.equal_range(make_interval(s, range));
        return r.first != r.second;
    }
    // Adds given clustering range to this interval set.
    // The range may overlap with this set.
    void add(const schema& s, const position_range& r) {
        _set += make_interval(s, r);
    }
    void add(const schema& s, const clustering_interval_set& other) {
        for (auto&& r : other) {
            add(s, r);
        }
    }
    position_range_iterator begin() const { return {_set.begin()}; }
    position_range_iterator end() const { return {_set.end()}; }
    friend std::ostream& operator<<(std::ostream&, const clustering_interval_set&);
};

