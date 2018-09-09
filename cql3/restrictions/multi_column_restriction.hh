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
#include "cql3/restrictions/single_column_primary_key_restrictions.hh"

namespace cql3 {

namespace restrictions {

class multi_column_restriction : public primary_key_restrictions<clustering_key_prefix> {
private:
    bool _has_only_asc_columns;
    bool _has_only_desc_columns;
protected:
    schema_ptr _schema;
    std::vector<const column_definition*> _column_defs;
public:
    multi_column_restriction(schema_ptr schema, std::vector<const column_definition*>&& defs)
        : _schema(schema)
        , _column_defs(std::move(defs))
    {
        update_asc_desc_existence();
    }

    virtual bool is_multi_column() const override {
        return true;
    }

    virtual std::vector<const column_definition*> get_column_defs() const override {
        return _column_defs;
    }

    virtual std::vector<bytes_opt> values(const query_options& options) const override  {
        auto src = values_as_keys(options);
        std::vector<bytes_opt> res;
        for (const clustering_key_prefix& r : src) {
            for (const auto& component : r.components()) {
                res.emplace_back(component);
            }
        }
        return res;
    }

    virtual void merge_with(::shared_ptr<restriction> other) override {
        statements::request_validations::check_true(other->is_multi_column(),
            "Mixing single column relations and multi column relations on clustering columns is not allowed");
        auto as_pkr = static_pointer_cast<primary_key_restrictions<clustering_key_prefix>>(other);
        do_merge_with(as_pkr);
        update_asc_desc_existence();
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

    /**
     * @return true if the restriction contains at least one column of each
     * ordering, false otherwise.
     */
    bool is_mixed_order() const {
        return !is_desc_order() && !is_asc_order();
    }

    /**
     * @return true if all the restricted columns ordered in descending
     * order, false otherwise
     */
    bool is_desc_order() const {
        return _has_only_desc_columns;
    }

    /**
     * @return true if all the restricted columns ordered in ascending
     * order, false otherwise
     */
    bool is_asc_order() const {
        return _has_only_asc_columns;
    }

private:
    /**
     * Updates the _has_only_asc_columns and _has_only_desc_columns fields.
     */
    void update_asc_desc_existence() {
        std::size_t num_of_desc =
                std::count_if(_column_defs.begin(), _column_defs.end(),  [] (const column_definition* cd) { return cd->type->is_reversed(); });
        _has_only_asc_columns = num_of_desc == 0;
        _has_only_desc_columns = num_of_desc == _column_defs.size();
    }
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
    using restriction_shared_ptr = ::shared_ptr<primary_key_restrictions<clustering_key_prefix>>;
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
        if (!is_mixed_order()) {
            return bounds_ranges_unified_order(options);
        } else {
            return bounds_ranges_mixed_order(options);
        }
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

    std::vector<bytes_opt> read_bound_components(const query_options& options, statements::bound b) const {
        if (!has_bound(b)) {
            return {};
        }
        auto vals = component_bounds(b, options);
        for (unsigned i = 0; i < vals.size(); i++) {
            statements::request_validations::check_not_null(vals[i], "Invalid null value in condition for column %s", _column_defs.at(i)->name_as_text());
        }
        return vals;
    }

    /**
     * Retrieve the bounds for the case that all clustering columns have the same order.
     * Having the same order implies we can do a prefix search on the data.
     * @param options the query options
     * @return the vector of ranges for the restriction
     */
    std::vector<bounds_range_type> bounds_ranges_unified_order(const query_options& options) const {
        auto start_prefix = clustering_key_prefix::from_optional_exploded(*_schema, read_bound_components(options, statements::bound::START));
        auto start_bound = bounds_range_type::bound(std::move(start_prefix), is_inclusive(statements::bound::START));
        auto end_prefix = clustering_key_prefix::from_optional_exploded(*_schema, read_bound_components(options, statements::bound::END));
        auto end_bound = bounds_range_type::bound(std::move(end_prefix), is_inclusive(statements::bound::END));
        auto make_range = [&] () {
            if (is_asc_order()) {
                return bounds_range_type::make(start_bound, end_bound);
            } else {
                return bounds_range_type::make(end_bound, start_bound);
            }
        };
        auto range = make_range();
        auto bounds = bound_view::from_range(range);
        if (bound_view::compare(*_schema)(bounds.second, bounds.first)) {
            return { };
        }
        return { std::move(range) };
    }

    /**
     * Retrieve the bounds when clustering columns are mixed order
     * (contains ASC and DESC together).
     * Having mixed order implies that a prefix search can't take place,
     * instead, the bounds have to be broken down to separate prefix serchable
     * ranges such that their combination is equivalent to the original range.
     * @param options the query options
     * @return the vector of ranges for the restriction
     */
    std::vector<bounds_range_type> bounds_ranges_mixed_order(const query_options& options) const {
        std::vector<bounds_range_type> ret_ranges;
        auto mixed_order_restrictions = build_mixed_order_restriction_set(options);
        ret_ranges.reserve(mixed_order_restrictions.size());
        for (auto r : mixed_order_restrictions) {
            for (auto&& range : r->bounds_ranges(options)) {
                ret_ranges.emplace_back(std::move(range));
            }
        }
        return ret_ranges;
    }

    /**
     * The function returns the first real inequality component.
     * The first real inequality is the index of the first component in the
     * tuple that will turn into a slice single column restriction.
     * For example: (a, b, c) > (0, 1, 2) and (a, b, c) < (0, 1, 5) will be
     * broken into one single column restriction set of the form:
     * a = 0 and b = 1 and c > 2 and c < 5 , c is the first element that has
     * inequality so for this case the function will return 2.
     * @param start_components - the components of the starts tuple range.
     * @param end_components - the components of the end tuple range.
     * @return an empty value if not found and the index of the first index that
     * will yield inequality
     */
    std::optional<std::size_t> find_first_neq_component(std::vector<bytes_opt>& start_components,
                                                        std::vector<bytes_opt>& end_components) const {
        size_t common_components_count = std::min(start_components.size(), end_components.size());
        for (size_t i = 0; i < common_components_count ; i++) {
            if (start_components[i].value() != end_components[i].value()) {
                return i;
            }
        }

        size_t max_components_count = std::max(start_components.size(), end_components.size());
        if (common_components_count < max_components_count) {
            return common_components_count;
        } else {
            return std::nullopt;
        }
    }

    /**
     * Creates a single column restriction which is either slice or equality.
     * @param bound - if bound is empty this is an equality, if its either START or END ,
     *        this is the corresponding slice restriction.
     * @param inclusive - is the slice inclusive (ignored for equality).
     * @param column_pos - the column position to restrict
     * @param value - the value to restrict the colum with.
     * @return a shared pointer to the just created restriction.
     */
    ::shared_ptr<restriction> make_single_column_restriction(std::optional<cql3::statements::bound> bound, bool inclusive,
                                                             std::size_t column_pos,const bytes_opt& value) const {
        ::shared_ptr<cql3::term> term = ::make_shared<cql3::constants::value>(cql3::raw_value::make_value(value));
        if (!bound){
            return ::make_shared<cql3::restrictions::single_column_restriction::EQ>(*_column_defs[column_pos], term);
        } else {
            return ::make_shared<cql3::restrictions::single_column_restriction::slice>(*_column_defs[column_pos], bound.value(), inclusive, term);
        }
    }

    /**
     * A helper function to create a single column restrictions set from a tuple relation on
     * clustering keys.
     * i.e : (a,b,c) >= (0,1,2) will become:
     *      1.a > 0
     *      2. a = 0 and b > 1
     *      3. a = 0 and b = 1 and c >=2
     * @param bound - determines if the operator is '>' (START) or '<' (END)
     * @param bound_inclusive - determines if to append equality to the operator i.e: if > becomes >=
     * @param bound_values - the tuple values for the restriction
     * @param first_neq_component - the first component that will have inequality.
     *        for the example above, if this parameter is 1, only restrictions 2 and 3 will be created.
     *        this parameter helps to facilitate the nuances of breaking more complex relations, for example when
     *        there is in existence a second condition limiting the other side of the bound
     *        i.e:(a,b,c) >= (0,1,2)  and (a,b,c) < (5,6,7), this will require each bound to use the parameter.
     * @return the single column restriction set built according to the above parameters.
     */
    std::vector<restriction_shared_ptr> make_single_bound_restrictions(statements::bound bound, bool bound_inclusive,
                                                                       std::vector<bytes_opt>& bound_values,
                                                                       std::size_t first_neq_component) const{
        std::vector<restriction_shared_ptr> ret;
        std::size_t num_of_restrictions = bound_values.size() - first_neq_component;
        ret.reserve(num_of_restrictions);
        for (std::size_t i = 0;i < num_of_restrictions ; i++) {
            ret.emplace_back(::make_shared<cql3::restrictions::single_column_primary_key_restrictions<clustering_key>>(_schema, false));
            std::size_t neq_component_idx = first_neq_component + i;
            for (std::size_t j = 0;j < neq_component_idx; j++) {
                ret[i]->merge_with(make_single_column_restriction(std::nullopt, false, j, bound_values[j]));
            }
            bool inclusive = (i == (num_of_restrictions-1)) && bound_inclusive;
            ret[i]->merge_with(make_single_column_restriction(bound, inclusive, neq_component_idx, bound_values[neq_component_idx]));
        }
        return ret;
    }

    /**
     * Builds and returns a set of restrictions such that the union of their ranges (the restrictions OR-ed together)
     * is logically identical to this restriction, with the additional property that it can execute
     * correctly when the clustering columns are with "mixed order" - contains ASC and DESC orderings.
     * for more information: https://github.com/scylladb/scylla/issues/2050
     * @param options - the query options
     * @return set of restrictions which their ranges union is logically identical to this restriction.
     */
    std::vector<::shared_ptr<primary_key_restrictions<clustering_key_prefix>>>
    build_mixed_order_restriction_set(const query_options& options) const {
        std::vector<restriction_shared_ptr> ret;
        auto start_components = read_bound_components(options, statements::bound::START);
        auto end_components = read_bound_components(options, statements::bound::END);
        bool start_inclusive = is_inclusive(statements::bound::START);
        bool end_inclusive = is_inclusive(statements::bound::END);
        std::optional<std::size_t> first_neq_component = std::nullopt;

        // find the first index of the first component that is not equal between the tuples.
        if (start_components.empty() || end_components.empty()) {
            first_neq_component = 0;
        } else {
            auto tuple_mismatch = std::mismatch(start_components.begin(), start_components.end(),
                    end_components.begin(), end_components.end());
            if ((tuple_mismatch.first != start_components.end()) ||
                (tuple_mismatch.second != end_components.end())) {
                first_neq_component = std::distance(start_components.begin(), tuple_mismatch.first);
            }
        }

        // this is either a simple equality or a never fulfilled restriction
        if (!first_neq_component && start_inclusive && end_inclusive) {
            // This is a simple equality case
            shared_ptr<cql3::term> term = ::make_shared<cql3::tuples::value>(start_components);
            ret.emplace_back(::make_shared<cql3::restrictions::multi_column_restriction::EQ>(_schema, _column_defs, term));
            return ret;
        } else if (!first_neq_component) {
            // This is a contradiction case
            return {};
        } else if ((*first_neq_component == end_components.size() && !end_inclusive ) ||
                   (*first_neq_component == start_components.size() && !start_inclusive )) {
            // This is a case where one bound is a prefix of the other. If this prefix bound
            // is not inclusive the result will be an empty set.
            return {};
        }

        bool start_components_exists = (start_components.size() - first_neq_component.value()) > 0;
        bool end_components_exists = (end_components.size() - first_neq_component.value()) > 0;
        bool both_components_exists = start_components_exists && end_components_exists;
        if (start_components_exists) {
            auto restrictions =
                    make_single_bound_restrictions(statements::bound::START, start_inclusive, start_components, first_neq_component.value());
            for (auto&& r : restrictions) {
                ret.emplace_back(r);
            }
        }

        if (end_components_exists) {
            auto restrictions =
                    make_single_bound_restrictions(statements::bound::END, end_inclusive,
                            end_components, first_neq_component.value() + both_components_exists);
            for (auto&& r : restrictions) {
                ret.emplace_back(r);
            }
        }

        if (both_components_exists) {
            bool inclusive = end_inclusive && ((end_components.size() - first_neq_component.value()) == 1);
            ret[0]->merge_with(make_single_column_restriction(statements::bound::END, inclusive, first_neq_component.value(),
                    end_components[first_neq_component.value()]));
        }
        return ret;
    }
};

}

}
