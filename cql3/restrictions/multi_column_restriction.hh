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

#include "cql3/tuples.hh"
#include "cql3/statements/request_validations.hh"
#include "cql3/restrictions/primary_key_restrictions.hh"
#include "cql3/statements/request_validations.hh"

namespace cql3 {

namespace restrictions {

class multi_column_restriction : public primary_key_restrictions<clustering_key_prefix> {
protected:
    schema_ptr _schema;
    std::vector<const column_definition*> _column_defs;
public:
    multi_column_restriction(schema_ptr schema, std::vector<const column_definition*>&& defs)
        : _schema(schema)
        , _column_defs(std::move(defs))
    { }

    virtual bool is_multi_column() const override {
        return true;
    }

    virtual std::vector<const column_definition*> get_column_defs() const override {
        return _column_defs;
    }

    virtual std::vector<bytes_opt> values(const query_options& options) const override  {
        auto src = values_as_keys(options);
        std::vector<bytes_opt> res;
        std::transform(src.begin(), src.end(), std::back_inserter(res), [this] (auto&& r) {
            auto view = r.representation();
            return bytes(view.begin(), view.end());
        });
        return res;
    }

    virtual void merge_with(::shared_ptr<restriction> other) override {
        statements::request_validations::check_true(other->is_multi_column(),
            "Mixing single column relations and multi column relations on clustering columns is not allowed");
        auto as_pkr = static_pointer_cast<primary_key_restrictions<clustering_key_prefix>>(other);
        do_merge_with(as_pkr);
    }

    bool is_satisfied_by(const schema& schema,
                         const partition_key& key,
                         const clustering_key_prefix& ckey,
                         const row& cells,
                         const query_options& options,
                         gc_clock::time_point now) const override {
        for (auto&& range : bounds_ranges(options)) {
            if (!range.contains(ckey, clustering_key_prefix::prefix_equal_tri_compare(schema))) {
                return false;
            }
        }
        return true;
    }

protected:
    virtual void do_merge_with(::shared_ptr<primary_key_restrictions<clustering_key_prefix>> other) = 0;

    /**
     * Returns the names of the columns that are specified within this <code>Restrictions</code> and the other one
     * as a comma separated <code>String</code>.
     *
     * @param otherRestrictions the other restrictions
     * @return the names of the columns that are specified within this <code>Restrictions</code> and the other one
     * as a comma separated <code>String</code>.
     */
    sstring get_columns_in_commons(::shared_ptr<restrictions> other) const {
        auto ours = get_column_defs();
        auto theirs = other->get_column_defs();

        std::sort(ours.begin(), ours.end());
        std::sort(theirs.begin(), theirs.end());
        std::vector<const column_definition*> common;
        std::set_intersection(ours.begin(), ours.end(), theirs.begin(), theirs.end(), std::back_inserter(common));

        sstring str;
        for (auto&& c : common) {
            if (!str.empty()) {
                str += " ,";
            }
            str += c->name_as_text();
        }
        return str;
    }

    virtual bool has_supporting_index(const secondary_index::secondary_index_manager& index_manager) const override {
        for (const auto& index : index_manager.list_indexes()) {
            if (is_supported_by(index))
                return true;
        }
        return false;
    }

    virtual bool is_supported_by(const secondary_index::index& index) const = 0;

#if 0
    /**
     * Check if this type of restriction is supported for the specified column by the specified index.
     * @param index the Secondary index
     *
     * @return <code>true</code> this type of restriction is supported by the specified index,
     * <code>false</code> otherwise.
     */
    protected abstract boolean isSupportedBy(SecondaryIndex index);
#endif
public:
    class EQ;
    class IN;
    class IN_with_values;
    class IN_with_marker;

    class slice;
};

class multi_column_restriction::EQ final : public multi_column_restriction {
private:
    ::shared_ptr<term> _value;
public:
    EQ(schema_ptr schema, std::vector<const column_definition*> defs, ::shared_ptr<term> value)
        : multi_column_restriction(schema, std::move(defs))
        , _value(std::move(value))
    { }

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        return abstract_restriction::term_uses_function(_value, ks_name, function_name);
    }

    virtual bool is_supported_by(const secondary_index::index& index) const override {
        for (auto* cdef : _column_defs) {
            if (index.supports_expression(*cdef, cql3::operator_type::EQ)) {
                return true;
            }
        }
        return false;
    }

    virtual sstring to_string() const override {
        return sprint("EQ(%s)", _value->to_string());
    }

    virtual void do_merge_with(::shared_ptr<primary_key_restrictions<clustering_key_prefix>> other) override {
        throw exceptions::invalid_request_exception(sprint(
            "%s cannot be restricted by more than one relation if it includes an Equal",
            get_columns_in_commons(other)));
    }

    virtual std::vector<clustering_key_prefix> values_as_keys(const query_options& options) const override {
        return { composite_value(options) };
    };

    virtual std::vector<bounds_range_type> bounds_ranges(const query_options& options) const override {
        return { bounds_range_type::make_singular(composite_value(options)) };
    }

#if 0
    @Override
    protected boolean isSupportedBy(SecondaryIndex index)
    {
        return index.supportsOperator(Operator.EQ);
    }
#endif

    clustering_key_prefix composite_value(const query_options& options) const {
        auto t = static_pointer_cast<tuples::value>(_value->bind(options));
        auto values = t->get_elements();
        std::vector<bytes> components;
        for (unsigned i = 0; i < values.size(); i++) {
            auto component = statements::request_validations::check_not_null(values[i],
                "Invalid null value in condition for column %s",
                _column_defs.at(i)->name_as_text());
            components.emplace_back(*component);
        }
        return clustering_key_prefix::from_exploded(*_schema, std::move(components));
    }

#if 0
    @Override
    public final void addIndexExpressionTo(List<IndexExpression> expressions,
                                           QueryOptions options) throws InvalidRequestException
    {
        Tuples.Value t = ((Tuples.Value) value.bind(options));
        List<ByteBuffer> values = t.getElements();
        for (int i = 0; i < values.size(); i++)
        {
            ColumnDefinition columnDef = columnDefs.get(i);
            ByteBuffer component = validateIndexedValue(columnDef, values.get(i));
            expressions.add(new IndexExpression(columnDef.name.bytes, Operator.EQ, component));
        }
    }
#endif
};

class multi_column_restriction::IN : public multi_column_restriction {
public:
    using multi_column_restriction::multi_column_restriction;

    virtual bool is_supported_by(const secondary_index::index& index) const override {
        for (auto* cdef : _column_defs) {
            if (index.supports_expression(*cdef, cql3::operator_type::IN)) {
                return true;
            }
        }
        return false;
    }

    virtual bool is_IN() const override {
        return true;
    }

    virtual std::vector<clustering_key_prefix> values_as_keys(const query_options& options) const override {
        auto split_in_values = split_values(options);
        std::vector<clustering_key_prefix> keys;
        for (auto&& components : split_in_values) {
            for (unsigned i = 0; i < components.size(); i++) {
                statements::request_validations::check_not_null(components[i], "Invalid null value in condition for column %s", _column_defs.at(i)->name_as_text());
            }
            keys.emplace_back(clustering_key_prefix::from_optional_exploded(*_schema, components));
        }
        std::sort(keys.begin(), keys.end(), clustering_key_prefix::less_compare(*_schema));
        keys.erase(std::unique(keys.begin(), keys.end(), clustering_key_prefix::equality(*_schema)), keys.end());
        return keys;
    }

    virtual std::vector<bounds_range_type> bounds_ranges(const query_options& options) const override {
        auto split_in_values = split_values(options);
        std::vector<bounds_range_type> bounds;
        for (auto&& components : split_in_values) {
            for (unsigned i = 0; i < components.size(); i++) {
                statements::request_validations::check_not_null(components[i], "Invalid null value in condition for column %s", _column_defs.at(i)->name_as_text());
            }
            auto prefix = clustering_key_prefix::from_optional_exploded(*_schema, components);
            bounds.emplace_back(bounds_range_type::make_singular(prefix));
        }
        auto less_cmp = clustering_key_prefix::less_compare(*_schema);
        std::sort(bounds.begin(), bounds.end(), [&] (bounds_range_type& x, bounds_range_type& y) {
            return less_cmp(x.start()->value(), y.start()->value());
        });
        auto eq_cmp = clustering_key_prefix::equality(*_schema);
        bounds.erase(std::unique(bounds.begin(), bounds.end(), [&] (bounds_range_type& x, bounds_range_type& y) {
            return eq_cmp(x.start()->value(), y.start()->value());
        }), bounds.end());
        return bounds;
    }

#if 0
    @Override
    public void addIndexExpressionTo(List<IndexExpression> expressions,
                                     QueryOptions options) throws InvalidRequestException
    {
        List<List<ByteBuffer>> splitInValues = splitValues(options);
        checkTrue(splitInValues.size() == 1, "IN restrictions are not supported on indexed columns");

        List<ByteBuffer> values = splitInValues.get(0);
        checkTrue(values.size() == 1, "IN restrictions are not supported on indexed columns");

        ColumnDefinition columnDef = columnDefs.get(0);
        ByteBuffer component = validateIndexedValue(columnDef, values.get(0));
        expressions.add(new IndexExpression(columnDef.name.bytes, Operator.EQ, component));
    }
#endif

    virtual void do_merge_with(::shared_ptr<primary_key_restrictions<clustering_key_prefix>> other) override {
        throw exceptions::invalid_request_exception(sprint("%s cannot be restricted by more than one relation if it includes a IN",
                                                           get_columns_in_commons(other)));
    }

#if 0
    @Override
    protected boolean isSupportedBy(SecondaryIndex index)
    {
        return index.supportsOperator(Operator.IN);
    }
#endif
protected:
    virtual std::vector<std::vector<bytes_opt>> split_values(const query_options& options) const = 0;
};

/**
 * An IN restriction that has a set of terms for in values.
 * For example: "SELECT ... WHERE (a, b, c) IN ((1, 2, 3), (4, 5, 6))" or "WHERE (a, b, c) IN (?, ?)"
 */
class multi_column_restriction::IN_with_values final : public multi_column_restriction::IN {
private:
    std::vector<::shared_ptr<term>> _values;
public:
    IN_with_values(schema_ptr schema, std::vector<const column_definition*> defs, std::vector<::shared_ptr<term>> value)
        : multi_column_restriction::IN(schema, std::move(defs))
        , _values(std::move(value))
    { }

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override  {
        return abstract_restriction::term_uses_function(_values, ks_name, function_name);
    }

    virtual sstring to_string() const override  {
        return sprint("IN(%s)", std::to_string(_values));
    }

protected:
    virtual std::vector<std::vector<bytes_opt>> split_values(const query_options& options) const override {
        std::vector<std::vector<bytes_opt>> buffers(_values.size());
        std::transform(_values.begin(), _values.end(), buffers.begin(), [&] (const ::shared_ptr<term>& value) {
            auto term = static_pointer_cast<multi_item_terminal>(value->bind(options));
            return term->get_elements();
        });
        return buffers;
    }
};


/**
 * An IN restriction that uses a single marker for a set of IN values that are tuples.
 * For example: "SELECT ... WHERE (a, b, c) IN ?"
 */
class multi_column_restriction::IN_with_marker final : public multi_column_restriction::IN {
private:
    shared_ptr<abstract_marker> _marker;
public:
    IN_with_marker(schema_ptr schema, std::vector<const column_definition*> defs, shared_ptr<abstract_marker> marker)
        : IN(schema, std::move(defs)), _marker(marker) {
    }

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        return false;
    }

    virtual sstring to_string() const override {
        return "IN ?";
    }

protected:
    virtual std::vector<std::vector<bytes_opt>> split_values(const query_options& options) const override {
        auto in_marker = static_pointer_cast<tuples::in_marker>(_marker);
        auto in_value = static_pointer_cast<tuples::in_value>(in_marker->bind(options));
        statements::request_validations::check_not_null(in_value, "Invalid null value for IN restriction");
        return in_value->get_split_values();
    }
};

class multi_column_restriction::slice final : public multi_column_restriction {
private:
    term_slice _slice;

    slice(schema_ptr schema, std::vector<const column_definition*> defs, term_slice slice)
        : multi_column_restriction(schema, std::move(defs))
        , _slice(slice)
    { }
public:
    slice(schema_ptr schema, std::vector<const column_definition*> defs, statements::bound bound, bool inclusive, shared_ptr<term> term)
        : slice(schema, defs, term_slice::new_instance(bound, inclusive, term))
    { }

    virtual bool is_supported_by(const secondary_index::index& index) const override {
        for (auto* cdef : _column_defs) {
            if (_slice.is_supported_by(*cdef, index)) {
                return true;
            }
        }
        return false;
    }

    virtual bool is_slice() const override {
        return true;
    }

    virtual std::vector<clustering_key_prefix> values_as_keys(const query_options&) const override {
        throw exceptions::unsupported_operation_exception();
    }

    virtual std::vector<bytes_opt> bounds(statements::bound b, const query_options& options) const override {
        throw std::runtime_error(sprint("%s not implemented", __PRETTY_FUNCTION__));
#if 0
        return Composites.toByteBuffers(boundsAsComposites(b, options));
#endif
    }

    virtual std::vector<bounds_range_type> bounds_ranges(const query_options& options) const override {
        // FIXME: doesn't work properly with mixed CLUSTERING ORDER (CASSANDRA-7281)
        auto read_bound = [&] (statements::bound b) -> std::experimental::optional<bounds_range_type::bound> {
            if (!has_bound(b)) {
                return {};
            }
            auto vals = component_bounds(b, options);
            for (unsigned i = 0; i < vals.size(); i++) {
                statements::request_validations::check_not_null(vals[i], "Invalid null value in condition for column %s", _column_defs.at(i)->name_as_text());
            }
            auto prefix = clustering_key_prefix::from_optional_exploded(*_schema, vals);
            return bounds_range_type::bound(prefix, is_inclusive(b));
        };
        auto range = wrapping_range<clustering_key_prefix>(read_bound(statements::bound::START), read_bound(statements::bound::END));
        auto bounds = bound_view::from_range(range);
        if (bound_view::compare(*_schema)(bounds.second, bounds.first)) {
            return { };
        }
        return { bounds_range_type(std::move(range)) };
    }
#if 0
        @Override
        public void addIndexExpressionTo(List<IndexExpression> expressions,
                                         QueryOptions options) throws InvalidRequestException
        {
            throw invalidRequest("Slice restrictions are not supported on indexed columns which are part of a multi column relation");
        }

        @Override
        protected boolean isSupportedBy(SecondaryIndex index)
        {
            return slice.isSupportedBy(index);
        }

        private static Composite.EOC eocFor(Restriction r, Bound eocBound, Bound inclusiveBound)
        {
            if (eocBound.isStart())
                return r.isInclusive(inclusiveBound) ? Composite.EOC.NONE : Composite.EOC.END;

            return r.isInclusive(inclusiveBound) ? Composite.EOC.END : Composite.EOC.START;
        }
#endif
public:
    virtual bool has_bound(statements::bound b) const override {
        return _slice.has_bound(b);
    }

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        return (_slice.has_bound(statements::bound::START) && abstract_restriction::term_uses_function(_slice.bound(statements::bound::START), ks_name, function_name))
                || (_slice.has_bound(statements::bound::END) && abstract_restriction::term_uses_function(_slice.bound(statements::bound::END), ks_name, function_name));
    }

    virtual bool is_inclusive(statements::bound b) const override {
        return _slice.is_inclusive(b);
    }

    virtual void do_merge_with(::shared_ptr<primary_key_restrictions<clustering_key_prefix>> other) override {
        using namespace statements::request_validations;
        check_true(other->is_slice(),
                   "Column \"%s\" cannot be restricted by both an equality and an inequality relation",
                   get_columns_in_commons(other));
        auto other_slice = static_pointer_cast<slice>(other);

        check_false(has_bound(statements::bound::START) && other_slice->has_bound(statements::bound::START),
                    "More than one restriction was found for the start bound on %s",
                    get_columns_in_commons(other));
        check_false(has_bound(statements::bound::END) && other_slice->has_bound(statements::bound::END),
                    "More than one restriction was found for the end bound on %s",
                    get_columns_in_commons(other));

        if (_column_defs.size() < other_slice->_column_defs.size()) {
            _column_defs = other_slice->_column_defs;
        }
        _slice.merge(other_slice->_slice);
    }

    virtual sstring to_string() const override {
        return sstring("SLICE") + _slice.to_string();
    }

private:
    /**
     * Similar to bounds(), but returns one ByteBuffer per-component in the bound instead of a single
     * ByteBuffer to represent the entire bound.
     * @param b the bound type
     * @param options the query options
     * @return one ByteBuffer per-component in the bound
     * @throws InvalidRequestException if the components cannot be retrieved
     */
    std::vector<bytes_opt> component_bounds(statements::bound b, const query_options& options) const {
        auto value = static_pointer_cast<tuples::value>(_slice.bound(b)->bind(options));
        return value->get_elements();
    }
};

}

}
