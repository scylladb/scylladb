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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#pragma once

#include <vector>
#include "schema.hh"
#include "cartesian_product.hh"
#include "cql3/restrictions/primary_key_restrictions.hh"
#include "cql3/restrictions/single_column_restrictions.hh"

namespace cql3 {

namespace restrictions {

/**
 * A set of single column restrictions on a primary key part (partition key or clustering key).
 */
template<typename ValueType>
class single_column_primary_key_restrictions : public primary_key_restrictions<ValueType> {
    using range_type = query::range<ValueType>;
    using range_bound = typename range_type::bound;
private:
    schema_ptr _schema;
    ::shared_ptr<single_column_restrictions> _restrictions;
    bool _slice;
    bool _contains;
    bool _in;
public:
    single_column_primary_key_restrictions(schema_ptr schema)
        : _schema(schema)
        , _restrictions(::make_shared<single_column_restrictions>(schema))
        , _slice(false)
        , _contains(false)
        , _in(false)
    { }

    virtual bool is_on_token() override {
        return false;
    }

    virtual bool is_multi_column() override {
        return false;
    }

    virtual bool is_slice() override {
        return _slice;
    }

    virtual bool is_contains() override {
        return _contains;
    }

    virtual bool is_IN() override {
        return _in;
    }

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) override {
        return _restrictions->uses_function(ks_name, function_name);
    }

    void do_merge_with(::shared_ptr<single_column_restriction> restriction) {
        if (!_restrictions->empty()) {
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
                        _restrictions->next_column(new_column)->name_as_text(), new_column.name_as_text()));
                }
            }
        }

        _slice |= restriction->is_slice();
        _in |= restriction->is_IN();
        _contains |= restriction->is_contains();
        _restrictions->add_restriction(restriction);
    }

    virtual void merge_with(::shared_ptr<restriction> restriction) override {
        if (restriction->is_multi_column()) {
            throw exceptions::invalid_request_exception(
                "Mixing single column relations and multi column relations on clustering columns is not allowed");
        }

        if (restriction->is_on_token()) {
            fail(unimplemented::cause::TOKEN_RESTRICTION);
#if 0
            throw exceptions::invalid_request_exception("Columns \"%s\" cannot be restricted by both a normal relation and a token relation",
                      ((TokenRestriction) restriction).getColumnNamesAsString());
#endif
        }
        do_merge_with(::static_pointer_cast<single_column_restriction>(restriction));
    }

    virtual std::vector<ValueType> values(const query_options& options) override {
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

    virtual std::vector<range_type> bounds(const query_options& options) override {
        std::vector<range_type> ranges;
        std::vector<std::vector<bytes_opt>> vec_of_values;

        // TODO: optimize for all EQ case

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
                            throw exceptions::invalid_request_exception(sprint("Invalid null clustering key part %s", r->to_string()));
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
                                throw exceptions::invalid_request_exception(sprint("Invalid null clustering key part %s", r->to_string()));
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
                    throw exceptions::invalid_request_exception(sprint("Invalid null clustering key part %s", def->name_as_text()));
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

#if 0
    virtual bool hasSupportingIndex(SecondaryIndexManager indexManager) override {
        return restrictions.hasSupportingIndex(indexManager);
    }

    virtual void addIndexExpressionTo(List<IndexExpression> expressions, QueryOptions options) override {
        restrictions.addIndexExpressionTo(expressions, options);
    }
#endif

    virtual std::vector<const column_definition*> get_column_defs() override {
        return _restrictions->get_column_defs();
    }

    virtual bool empty() override {
        return _restrictions->empty();
    }

    virtual uint32_t size() override {
        return _restrictions->size();
    }
};

}
}
