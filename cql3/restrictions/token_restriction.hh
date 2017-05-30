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

#include "restriction.hh"
#include "primary_key_restrictions.hh"
#include "exceptions/exceptions.hh"
#include "term_slice.hh"
#include "keys.hh"

class column_definition;

namespace cql3 {

namespace restrictions {

/**
 * <code>Restriction</code> using the token function.
 */
class token_restriction: public primary_key_restrictions<partition_key> {
private:
    /**
     * The definition of the columns to which apply the token restriction.
     */
    std::vector<const column_definition *> _column_definitions;
public:
    token_restriction(std::vector<const column_definition *> c)
            : _column_definitions(std::move(c)) {
    }

    bool is_on_token() const override {
        return true;
    }
    std::vector<const column_definition*> get_column_defs() const override {
        return _column_definitions;
    }

    virtual bool has_supporting_index(const secondary_index::secondary_index_manager& index_manager) const override {
        return false;
    }

#if 0
    void add_index_expression_to(std::vector<::shared_ptr<index_expression>>& expressions,
                                         const query_options& options) override {
        throw exceptions::unsupported_operation_exception();
    }
#endif

    std::vector<partition_key> values_as_keys(const query_options& options) const override {
        throw exceptions::unsupported_operation_exception();
    }

    std::vector<bounds_range_type> bounds_ranges(const query_options& options) const override {
        auto get_token_bound = [this, &options](statements::bound b) {
            if (!has_bound(b)) {
                return is_start(b) ? dht::minimum_token() : dht::maximum_token();
            }
            auto buf= bounds(b, options).front();
            if (!buf) {
                throw exceptions::invalid_request_exception("Invalid null token value");
            }
            auto tk = dht::global_partitioner().from_bytes(*buf);
            if (tk.is_minimum() && !is_start(b)) {
                // The token was parsed as a minimum marker (token::kind::before_all_keys), but
                // as it appears in the end bound position, it is actually the maximum marker
                // (token::kind::after_all_keys).
                return dht::maximum_token();
            }
            return tk;
        };

        const auto start_token = get_token_bound(statements::bound::START);
        const auto end_token = get_token_bound(statements::bound::END);
        const auto include_start = this->is_inclusive(statements::bound::START);
        const auto include_end = this->is_inclusive(statements::bound::END);

        /*
         * If we ask SP.getRangeSlice() for (token(200), token(200)], it will happily return the whole ring.
         * However, wrapping range doesn't really make sense for CQL, and we want to return an empty result in that
         * case (CASSANDRA-5573). So special case to create a range that is guaranteed to be empty.
         *
         * In practice, we want to return an empty result set if either startToken > endToken, or both are equal but
         * one of the bound is excluded (since [a, a] can contains something, but not (a, a], [a, a) or (a, a)).
         */
        if (start_token > end_token
                || (start_token == end_token
                    && (!include_start || !include_end))) {
            return {};
        }

        typedef typename bounds_range_type::bound bound;

        auto start = bound(include_start
                           ? dht::ring_position::starting_at(start_token)
                           : dht::ring_position::ending_at(start_token));
        auto end = bound(include_end
                           ? dht::ring_position::ending_at(end_token)
                           : dht::ring_position::starting_at(end_token));

        return { bounds_range_type(std::move(start), std::move(end)) };
    }

    class EQ;
    class slice;
};


class token_restriction::EQ final : public token_restriction {
private:
    ::shared_ptr<term> _value;
public:
    EQ(std::vector<const column_definition*> column_defs, ::shared_ptr<term> value)
        : token_restriction(column_defs)
        , _value(std::move(value))
    {}

    bool is_EQ() const {
        return true;
    }

    bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        return abstract_restriction::term_uses_function(_value, ks_name, function_name);
    }

    void merge_with(::shared_ptr<restriction>) override {
        throw exceptions::invalid_request_exception(
                join(", ", get_column_defs())
                        + " cannot be restricted by more than one relation if it includes an Equal");
    }

    std::vector<bytes_opt> values(const query_options& options) const override {
        return { to_bytes_opt(_value->bind_and_get(options)) };
    }

    sstring to_string() const override {
        return sprint("EQ(%s)", _value->to_string());
    }

    virtual bool is_satisfied_by(const schema& schema,
                                 const partition_key& key,
                                 const clustering_key_prefix& ckey,
                                 const row& cells,
                                 const query_options& options,
                                 gc_clock::time_point now) const override;
};

class token_restriction::slice final : public token_restriction {
private:
    term_slice _slice;
public:
    slice(std::vector<const column_definition*> column_defs, statements::bound bound, bool inclusive, ::shared_ptr<term> term)
        : token_restriction(column_defs)
        , _slice(term_slice::new_instance(bound, inclusive, std::move(term)))
    {}

    bool is_slice() const override {
        return true;
    }

    bool has_bound(statements::bound b) const override {
        return _slice.has_bound(b);
    }

    std::vector<bytes_opt> values(const query_options& options) const override {
        throw exceptions::unsupported_operation_exception();
    }

    std::vector<bytes_opt> bounds(statements::bound b, const query_options& options) const override {
        return { to_bytes_opt(_slice.bound(b)->bind_and_get(options)) };
    }

    bool uses_function(const sstring& ks_name,
            const sstring& function_name) const override {
        return (_slice.has_bound(statements::bound::START)
                && abstract_restriction::term_uses_function(
                        _slice.bound(statements::bound::START), ks_name,
                        function_name))
                || (_slice.has_bound(statements::bound::END)
                        && abstract_restriction::term_uses_function(
                                _slice.bound(statements::bound::END),
                                ks_name, function_name));
    }
    bool is_inclusive(statements::bound b) const override {
        return _slice.is_inclusive(b);
    }
    void merge_with(::shared_ptr<restriction> restriction) override {
        try {
            if (!restriction->is_on_token()) {
                throw exceptions::invalid_request_exception(
                        "Columns \"%s\" cannot be restricted by both a normal relation and a token relation");
            }
            if (!restriction->is_slice()) {
                throw exceptions::invalid_request_exception(
                        "Columns \"%s\" cannot be restricted by both an equality and an inequality relation");
            }

            auto* other_slice = static_cast<slice *>(restriction.get());

            if (has_bound(statements::bound::START)
                    && other_slice->has_bound(statements::bound::START)) {
                throw exceptions::invalid_request_exception(
                        "More than one restriction was found for the start bound on %s");
            }
            if (has_bound(statements::bound::END)
                    && other_slice->has_bound(statements::bound::END)) {
                throw exceptions::invalid_request_exception(
                        "More than one restriction was found for the end bound on %s");
            }
            _slice.merge(other_slice->_slice);
        } catch (exceptions::invalid_request_exception & e) {
            throw exceptions::invalid_request_exception(
                    sprint(e.what(), join(", ", get_column_defs())));
        }
    }
    sstring to_string() const override {
        return sprint("SLICE%s", _slice);
    }

    virtual bool is_satisfied_by(const schema& schema,
                                 const partition_key& key,
                                 const clustering_key_prefix& ckey,
                                 const row& cells,
                                 const query_options& options,
                                 gc_clock::time_point now) const override;
};

}

}
