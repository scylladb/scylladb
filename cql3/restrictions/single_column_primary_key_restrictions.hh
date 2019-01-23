/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015 ScyllaDB
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

#pragma once

#include <vector>
#include "schema.hh"
#include "cartesian_product.hh"
#include "cql3/restrictions/primary_key_restrictions.hh"
#include "cql3/restrictions/single_column_restrictions.hh"
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/map.hpp>

namespace cql3 {

namespace restrictions {

/**
 * A set of single column restrictions on a primary key part (partition key or clustering key).
 */
template<typename ValueType>
class single_column_primary_key_restrictions : public primary_key_restrictions<ValueType> {
    using range_type = query::range<ValueType>;
    using range_bound = typename range_type::bound;
    using bounds_range_type = typename primary_key_restrictions<ValueType>::bounds_range_type;
    template<typename OtherValueType>
    friend class single_column_primary_key_restrictions;
private:
    schema_ptr _schema;
    bool _allow_filtering;
    ::shared_ptr<single_column_restrictions> _restrictions;
    bool _slice;
    bool _contains;
    bool _in;
public:
    single_column_primary_key_restrictions(schema_ptr schema, bool allow_filtering)
        : _schema(schema)
        , _allow_filtering(allow_filtering)
        , _restrictions(::make_shared<single_column_restrictions>(schema))
        , _slice(false)
        , _contains(false)
        , _in(false)
    { }

    // Convert another primary key restrictions type into this type, possibly using different schema
    template<typename OtherValueType>
    explicit single_column_primary_key_restrictions(schema_ptr schema, const single_column_primary_key_restrictions<OtherValueType>& other)
        : _schema(schema)
        , _allow_filtering(other._allow_filtering)
        , _restrictions(::make_shared<single_column_restrictions>(schema))
        , _slice(other._slice)
        , _contains(other._contains)
        , _in(other._in)
    {
        for (const auto& entry : other._restrictions->restrictions()) {
            const column_definition* other_cdef = entry.first;
            const column_definition* this_cdef = _schema->get_column_definition(other_cdef->name());
            if (!this_cdef) {
                throw exceptions::invalid_request_exception(sprint("Base column %s not found in view index schema", other_cdef->name_as_text()));
            }
            ::shared_ptr<single_column_restriction> restriction = entry.second;
            _restrictions->add_restriction(restriction->apply_to(*this_cdef));
        }
    }

    virtual bool is_on_token() const override {
        return false;
    }

    virtual bool is_multi_column() const override {
        return false;
    }

    virtual bool is_slice() const override {
        return _slice;
    }

    virtual bool is_contains() const override {
        return _contains;
    }

    virtual bool is_IN() const override {
        return _in;
    }

    virtual bool is_all_eq() const override {
        return _restrictions->is_all_eq();
    }

    virtual bool has_bound(statements::bound b) const override {
        return boost::algorithm::all_of(_restrictions->restrictions(), [b] (auto&& r) { return r.second->has_bound(b); });
    }

    virtual bool is_inclusive(statements::bound b) const override {
        return boost::algorithm::all_of(_restrictions->restrictions(), [b] (auto&& r) { return r.second->is_inclusive(b); });
    }

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        return _restrictions->uses_function(ks_name, function_name);
    }

    void do_merge_with(::shared_ptr<single_column_restriction> restriction) {
        if (!_restrictions->empty() && !_allow_filtering) {
            auto last_column = *_restrictions->last_column();
            auto new_column = restriction->get_column_def();

            if (_slice && _schema->position(new_column) > _schema->position(last_column)) {
                throw exceptions::invalid_request_exception(sprint(
                    "Clustering column \"%s\" cannot be restricted (preceding column \"%s\" is restricted by a non-EQ relation)",
                    new_column.name_as_text(), last_column.name_as_text()));
            }

            if (_schema->position(new_column) < _schema->position(last_column)) {
                if (restriction->is_slice()) {
                    throw exceptions::invalid_request_exception(sprint(
                        "PRIMARY KEY column \"%s\" cannot be restricted (preceding column \"%s\" is restricted by a non-EQ relation)",
                        last_column.name_as_text(), new_column.name_as_text()));
                }
            }
        }

        _slice |= restriction->is_slice();
        _in |= restriction->is_IN();
        _contains |= restriction->is_contains();
        _restrictions->add_restriction(restriction);
    }

    virtual size_t prefix_size() const override {
        return primary_key_restrictions<ValueType>::prefix_size(_schema);
    }

    ::shared_ptr<single_column_primary_key_restrictions<clustering_key>> get_longest_prefix_restrictions() {
        static_assert(std::is_same_v<ValueType, clustering_key>, "Only clustering key can produce longest prefix restrictions");
        size_t current_prefix_size = prefix_size();
        if (current_prefix_size == _restrictions->restrictions().size()) {
            return dynamic_pointer_cast<single_column_primary_key_restrictions<clustering_key>>(this->shared_from_this());
        }

        auto longest_prefix_restrictions = ::make_shared<single_column_primary_key_restrictions<clustering_key>>(_schema, _allow_filtering);
        auto restriction_it = _restrictions->restrictions().begin();
        for (size_t i = 0; i < current_prefix_size; ++i) {
            longest_prefix_restrictions->merge_with((restriction_it++)->second);
        }
        return longest_prefix_restrictions;
    }

    virtual void merge_with(::shared_ptr<restriction> restriction) override {
        if (restriction->is_multi_column()) {
            throw exceptions::invalid_request_exception(
                "Mixing single column relations and multi column relations on clustering columns is not allowed");
        }
        if (restriction->is_on_token()) {
            throw exceptions::invalid_request_exception(
                    sprint(
                            "Columns \"%s\" cannot be restricted by both a normal relation and a token relation",
                            join(", ", get_column_defs())));
        }
        do_merge_with(::static_pointer_cast<single_column_restriction>(restriction));
    }

    virtual std::vector<ValueType> values_as_keys(const query_options& options) const override {
        std::vector<std::vector<bytes_opt>> value_vector;
        value_vector.reserve(_restrictions->size());
        for (auto&& e : _restrictions->restrictions()) {
            const column_definition* def = e.first;
            auto&& r = e.second;
            assert(!r->is_slice());

            std::vector<bytes_opt> values = r->values(options);
            for (auto&& val : values) {
                if (!val) {
                    throw exceptions::invalid_request_exception(sprint("Invalid null value for column %s", def->name_as_text()));
                }
            }
            if (values.empty()) {
                return {};
            }
            value_vector.emplace_back(std::move(values));
        }

        std::vector<ValueType> result;
        result.reserve(cartesian_product_size(value_vector));
        for (auto&& v : make_cartesian_product(value_vector)) {
            result.emplace_back(ValueType::from_optional_exploded(*_schema, std::move(v)));
        }
        return result;
    }

private:
    std::vector<range_type> compute_bounds(const query_options& options) const {
        std::vector<range_type> ranges;

        static constexpr auto invalid_null_msg = std::is_same<ValueType, partition_key>::value
            ? "Invalid null value for partition key part %s" : "Invalid null value for clustering key part %s";

        if (_restrictions->is_all_eq()) {
            ranges.reserve(1);
            if (_restrictions->size() == 1) {
                auto&& e = *_restrictions->restrictions().begin();
                const column_definition* def = e.first;
                auto&& r = e.second;
                auto&& val = r->value(options);
                if (!val) {
                    throw exceptions::invalid_request_exception(sprint(invalid_null_msg, def->name_as_text()));
                }
                ranges.emplace_back(range_type::make_singular(ValueType::from_single_value(*_schema, std::move(*val))));
                return ranges;
            }
            std::vector<bytes> components;
            components.reserve(_restrictions->size());
            for (auto&& e : _restrictions->restrictions()) {
                const column_definition* def = e.first;
                auto&& r = e.second;
                assert(components.size() == _schema->position(*def));
                auto&& val = r->value(options);
                if (!val) {
                    throw exceptions::invalid_request_exception(sprint(invalid_null_msg, def->name_as_text()));
                }
                components.emplace_back(std::move(*val));
            }
            ranges.emplace_back(range_type::make_singular(ValueType::from_exploded(*_schema, std::move(components))));
            return ranges;
        }

        std::vector<std::vector<bytes_opt>> vec_of_values;
        for (auto&& e : _restrictions->restrictions()) {
            const column_definition* def = e.first;
            auto&& r = e.second;

            if (vec_of_values.size() != _schema->position(*def) || r->is_contains()) {
                // The prefixes built so far are the longest we can build,
                // the rest of the constraints will have to be applied using filtering.
                break;
            }

            if (r->is_slice()) {
                if (cartesian_product_is_empty(vec_of_values)) {
                    auto read_bound = [r, &options, this] (statements::bound b) -> std::experimental::optional<range_bound> {
                        if (!r->has_bound(b)) {
                            return {};
                        }
                        auto value = r->bounds(b, options)[0];
                        if (!value) {
                            throw exceptions::invalid_request_exception(sprint(invalid_null_msg, r->to_string()));
                        }
                        return {range_bound(ValueType::from_single_value(*_schema, *value), r->is_inclusive(b))};
                    };
                    ranges.emplace_back(range_type(
                        read_bound(statements::bound::START),
                        read_bound(statements::bound::END)));
                    if (def->type->is_reversed()) {
                        ranges.back().reverse();
                    }
                    return std::move(ranges);
                }

                ranges.reserve(cartesian_product_size(vec_of_values));
                for (auto&& prefix : make_cartesian_product(vec_of_values)) {
                    auto read_bound = [r, &prefix, &options, this](statements::bound bound) -> range_bound {
                        if (r->has_bound(bound)) {
                            auto value = std::move(r->bounds(bound, options)[0]);
                            if (!value) {
                                throw exceptions::invalid_request_exception(sprint(invalid_null_msg, r->to_string()));
                            }
                            prefix.emplace_back(std::move(value));
                            auto val = ValueType::from_optional_exploded(*_schema, prefix);
                            prefix.pop_back();
                            return range_bound(std::move(val), r->is_inclusive(bound));
                        } else {
                            return range_bound(ValueType::from_optional_exploded(*_schema, prefix));
                        }
                    };

                    ranges.emplace_back(range_type(
                        read_bound(statements::bound::START),
                        read_bound(statements::bound::END)));

                    if (def->type->is_reversed()) {
                        ranges.back().reverse();
                    }
                }

                return std::move(ranges);
            }

            auto values = r->values(options);
            for (auto&& val : values) {
                if (!val) {
                    throw exceptions::invalid_request_exception(sprint(invalid_null_msg, def->name_as_text()));
                }
            }
            if (values.empty()) {
                return {};
            }
            vec_of_values.emplace_back(std::move(values));
        }

        ranges.reserve(cartesian_product_size(vec_of_values));
        for (auto&& prefix : make_cartesian_product(vec_of_values)) {
            ranges.emplace_back(range_type::make_singular(ValueType::from_optional_exploded(*_schema, std::move(prefix))));
        }

        return std::move(ranges);
    }

public:
    std::vector<bounds_range_type> bounds_ranges(const query_options& options) const override;

    std::vector<bytes_opt> values(const query_options& options) const override {
        auto src = values_as_keys(options);
        std::vector<bytes_opt> res;
        for (const ValueType& r : src) {
            for (const auto& component : r.components()) {
                res.emplace_back(component);
            }
        }
        return res;
    }

    virtual bytes_opt value_for(const column_definition& cdef, const query_options& options) const override {
        return _restrictions->value_for(cdef, options);
    }

    std::vector<bytes_opt> bounds(statements::bound b, const query_options& options) const override {
        // TODO: if this proved to be required.
        fail(unimplemented::cause::LEGACY_COMPOSITE_KEYS); // not 100% correct...
    }

    const single_column_restrictions::restrictions_map& restrictions() const {
        return _restrictions->restrictions();
    }

    virtual bool has_supporting_index(const secondary_index::secondary_index_manager& index_manager) const override {
        return _restrictions->has_supporting_index(index_manager);
    }

#if 0
    virtual void addIndexExpressionTo(List<IndexExpression> expressions, QueryOptions options) override {
        restrictions.addIndexExpressionTo(expressions, options);
    }
#endif

    virtual std::vector<const column_definition*> get_column_defs() const override {
        return _restrictions->get_column_defs();
    }

    virtual bool empty() const override {
        return _restrictions->empty();
    }

    virtual uint32_t size() const override {
        return _restrictions->size();
    }
    sstring to_string() const override {
        return sprint("Restrictions(%s)", join(", ", get_column_defs()));
    }

    virtual bool is_satisfied_by(const schema& schema,
                                 const partition_key& key,
                                 const clustering_key_prefix& ckey,
                                 const row& cells,
                                 const query_options& options,
                                 gc_clock::time_point now) const override {
        return boost::algorithm::all_of(
            _restrictions->restrictions() | boost::adaptors::map_values,
            [&] (auto&& r) { return r->is_satisfied_by(schema, key, ckey, cells, options, now); });
    }

    virtual bool needs_filtering(const schema& schema) const override;
    virtual unsigned int num_prefix_columns_that_need_not_be_filtered() const override;
};

template<>
inline dht::partition_range_vector
single_column_primary_key_restrictions<partition_key>::bounds_ranges(const query_options& options) const {
    dht::partition_range_vector ranges;
    ranges.reserve(size());
    for (query::range<partition_key>& r : compute_bounds(options)) {
        if (!r.is_singular()) {
            throw exceptions::invalid_request_exception("Range queries on partition key values not supported.");
        }
        ranges.emplace_back(std::move(r).transform(
            [this] (partition_key&& k) -> query::ring_position {
                auto token = dht::global_partitioner().get_token(*_schema, k);
                return { std::move(token), std::move(k) };
            }));
    }
    return ranges;
}

template<>
inline std::vector<query::clustering_range>
single_column_primary_key_restrictions<clustering_key_prefix>::bounds_ranges(const query_options& options) const {
    auto wrapping_bounds = compute_bounds(options);
    auto bounds = boost::copy_range<query::clustering_row_ranges>(wrapping_bounds
            | boost::adaptors::filtered([&](auto&& r) {
                auto bounds = bound_view::from_range(r);
                return !bound_view::compare(*_schema)(bounds.second, bounds.first);
              })
            | boost::adaptors::transformed([&](auto&& r) { return query::clustering_range(std::move(r));
    }));
    auto less_cmp = clustering_key_prefix::less_compare(*_schema);
    std::sort(bounds.begin(), bounds.end(), [&] (query::clustering_range& x, query::clustering_range& y) {
        if (!x.start() && !y.start()) {
            return false;
        }
        if (!x.start()) {
            return true;
        }
        if (!y.start()) {
            return false;
        }
        return less_cmp(x.start()->value(), y.start()->value());
    });
    auto eq_cmp = clustering_key_prefix::equality(*_schema);
    bounds.erase(std::unique(bounds.begin(), bounds.end(), [&] (query::clustering_range& x, query::clustering_range& y) {
        if (!x.start() && !y.start()) {
            return true;
        }
        if (!x.start() || !y.start()) {
            return false;
        }
        return eq_cmp(x.start()->value(), y.start()->value());
    }), bounds.end());
    return bounds;
}

template<>
inline bool single_column_primary_key_restrictions<partition_key>::needs_filtering(const schema& schema) const {
    return primary_key_restrictions<partition_key>::needs_filtering(schema);
}

template<>
inline bool single_column_primary_key_restrictions<clustering_key>::needs_filtering(const schema& schema) const {
    // Restrictions currently need filtering in three cases:
    // 1. any of them is a CONTAINS restriction
    // 2. restrictions do not form a contiguous prefix (i.e. there are gaps in it)
    // 3. a SLICE restriction isn't on a last place
    column_id position = 0;
    for (const auto& restriction : _restrictions->restrictions() | boost::adaptors::map_values) {
        if (restriction->is_contains() || position != restriction->get_column_def().id) {
            return true;
        }
        if (!restriction->is_slice()) {
            position = restriction->get_column_def().id + 1;
        }
    }
    return false;
}

// How many of the restrictions (in column order) do not need filtering
// because they are implemented as a slice (potentially, a contiguous disk
// read). For example, if we have the filter "c1 < 3 and c2 > 3", c1 does not
// need filtering but c2 does so num_prefix_columns_that_need_not_be_filtered
// will be 1.
// The implementation of num_prefix_columns_that_need_not_be_filtered() is
// closely tied to that of needs_filtering() above - basically, if only the
// first num_prefix_columns_that_need_not_be_filtered() restrictions existed,
// then needs_filtering() would have returned false.
template<>
inline unsigned single_column_primary_key_restrictions<clustering_key>::num_prefix_columns_that_need_not_be_filtered() const {
    column_id position = 0;
    unsigned int count = 0;
    for (const auto& restriction : _restrictions->restrictions() | boost::adaptors::map_values) {
        if (restriction->is_contains() || position != restriction->get_column_def().id) {
            return count;
        }
        if (!restriction->is_slice()) {
            position = restriction->get_column_def().id + 1;
        }
        count++;
    }
    return count;
}

template<>
inline unsigned single_column_primary_key_restrictions<partition_key>::num_prefix_columns_that_need_not_be_filtered() const {
    // skip_filtering() is currently called only for clustering key
    // restrictions, so it doesn't matter what we return here.
    return 0;
}


}
}



