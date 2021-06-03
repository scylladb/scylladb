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
 * Copyright (C) 2015-present ScyllaDB
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

#include "cql3/expr/expression.hh"
#include "cql3/relation.hh"
#include "cql3/term.hh"
#include "cql3/tuples.hh"

#include "cql3/restrictions/multi_column_restriction.hh"

namespace cql3 {

/**
 * A relation using the tuple notation, which typically affects multiple columns.
 * Examples:
 *  - SELECT ... WHERE (a, b, c) > (1, 'a', 10)
 *  - SELECT ... WHERE (a, b, c) IN ((1, 2, 3), (4, 5, 6))
 *  - SELECT ... WHERE (a, b) < ?
 *  - SELECT ... WHERE (a, b) IN ?
 */
class multi_column_relation final : public relation {
public:
    using mode = expr::comparison_order;
private:
    std::vector<shared_ptr<column_identifier::raw>> _entities;
    shared_ptr<term::multi_column_raw> _values_or_marker;
    std::vector<shared_ptr<term::multi_column_raw>> _in_values;
    shared_ptr<tuples::in_raw> _in_marker;
    mode _mode;
public:
    multi_column_relation(std::vector<shared_ptr<column_identifier::raw>> entities,
        expr::oper_t relation_type, shared_ptr<term::multi_column_raw> values_or_marker,
        std::vector<shared_ptr<term::multi_column_raw>> in_values, shared_ptr<tuples::in_raw> in_marker, mode m = mode::cql)
        : relation(relation_type)
        , _entities(std::move(entities))
        , _values_or_marker(std::move(values_or_marker))
        , _in_values(std::move(in_values))
        , _in_marker(std::move(in_marker))
        , _mode(m)
    { }

    static shared_ptr<multi_column_relation> create_multi_column_relation(
        std::vector<shared_ptr<column_identifier::raw>> entities, expr::oper_t relation_type,
        shared_ptr<term::multi_column_raw> values_or_marker, std::vector<shared_ptr<term::multi_column_raw>> in_values,
        shared_ptr<tuples::in_raw> in_marker, mode m = mode::cql) {
        return ::make_shared<multi_column_relation>(std::move(entities), relation_type, std::move(values_or_marker),
            std::move(in_values), std::move(in_marker), m);
    }

    /**
     * Creates a multi-column EQ, LT, LTE, GT, or GTE relation.
     * For example: "SELECT ... WHERE (a, b) > (0, 1)"
     * @param entities the columns on the LHS of the relation
     * @param relationType the relation operator
     * @param valuesOrMarker a Tuples.Literal instance or a Tuples.Raw marker
     * @return a new <code>MultiColumnRelation</code> instance
     */
    static shared_ptr<multi_column_relation> create_non_in_relation(std::vector<shared_ptr<column_identifier::raw>> entities,
                                                                    expr::oper_t relation_type, shared_ptr<term::multi_column_raw> values_or_marker) {
        assert(relation_type != expr::oper_t::IN);
        return create_multi_column_relation(std::move(entities), relation_type, std::move(values_or_marker), {}, {});
    }

    /**
     * Same as above, but sets the magic mode that causes us to treat the restrictions as "raw" clustering bounds
     */
    static shared_ptr<multi_column_relation> create_scylla_clustering_bound_non_in_relation(std::vector<shared_ptr<column_identifier::raw>> entities,
                                                                    expr::oper_t relation_type, shared_ptr<term::multi_column_raw> values_or_marker) {
        assert(relation_type != expr::oper_t::IN);
        return create_multi_column_relation(std::move(entities), relation_type, std::move(values_or_marker), {}, {}, mode::clustering);
    }

    /**
     * Creates a multi-column IN relation with a list of IN values or markers.
     * For example: "SELECT ... WHERE (a, b) IN ((0, 1), (2, 3))"
     * @param entities the columns on the LHS of the relation
     * @param inValues a list of Tuples.Literal instances or a Tuples.Raw markers
     * @return a new <code>MultiColumnRelation</code> instance
     */
    static shared_ptr<multi_column_relation> create_in_relation(std::vector<shared_ptr<column_identifier::raw>> entities,
                                                                std::vector<shared_ptr<tuples::literal>> in_values) {
        std::vector<shared_ptr<term::multi_column_raw>> values(in_values.size());
        std::copy(in_values.begin(), in_values.end(), values.begin());
        return create_multi_column_relation(std::move(entities), expr::oper_t::IN, {}, std::move(values), {});
    }

    static shared_ptr<multi_column_relation> create_in_relation(std::vector<shared_ptr<column_identifier::raw>> entities,
                                                                std::vector<shared_ptr<tuples::raw>> in_values) {
        std::vector<shared_ptr<term::multi_column_raw>> values(in_values.size());
        std::copy(in_values.begin(), in_values.end(), values.begin());
        return create_multi_column_relation(std::move(entities), expr::oper_t::IN, {}, std::move(values), {});
    }

    /**
     * Creates a multi-column IN relation with a marker for the IN values.
     * For example: "SELECT ... WHERE (a, b) IN ?"
     * @param entities the columns on the LHS of the relation
     * @param inMarker a single IN marker
     * @return a new <code>MultiColumnRelation</code> instance
     */
    static shared_ptr<multi_column_relation> create_single_marker_in_relation(std::vector<shared_ptr<column_identifier::raw>> entities,
                                                                              shared_ptr<tuples::in_raw> in_marker) {
        return create_multi_column_relation(std::move(entities), expr::oper_t::IN, {}, {}, std::move(in_marker));
    }

    const std::vector<shared_ptr<column_identifier::raw>>& get_entities() const {
        return _entities;
    }

private:
    /**
     * For non-IN relations, returns the Tuples.Literal or Tuples.Raw marker for a single tuple.
     * @return a Tuples.Literal for non-IN relations or Tuples.Raw marker for a single tuple.
     */
    shared_ptr<term::multi_column_raw> get_value() {
        return _relation_type == expr::oper_t::IN ? _in_marker : _values_or_marker;
    }
public:
    virtual bool is_multi_column() const override { return true; }

protected:
    virtual shared_ptr<restrictions::restriction> new_EQ_restriction(database& db, schema_ptr schema,
                                                                     variable_specifications& bound_names) override {
        auto rs = receivers(db, *schema);
        std::vector<lw_shared_ptr<column_specification>> col_specs(rs.size());
        std::transform(rs.begin(), rs.end(), col_specs.begin(), [] (auto cs) {
            return cs->column_specification;
        });
        auto t = to_term(col_specs, *get_value(), db, schema->ks_name(), bound_names);
        return ::make_shared<restrictions::multi_column_restriction::EQ>(schema, rs, t);
    }

    virtual shared_ptr<restrictions::restriction> new_IN_restriction(database& db, schema_ptr schema,
                                                                     variable_specifications& bound_names) override {
        auto rs = receivers(db, *schema);
        std::vector<lw_shared_ptr<column_specification>> col_specs(rs.size());
        std::transform(rs.begin(), rs.end(), col_specs.begin(), [] (auto cs) {
            return cs->column_specification;
        });
        if (_in_marker) {
            auto t = to_term(col_specs, *get_value(), db, schema->ks_name(), bound_names);
            auto as_abstract_marker = static_pointer_cast<abstract_marker>(t);
            return ::make_shared<restrictions::multi_column_restriction::IN_with_marker>(schema, rs, as_abstract_marker);
        } else {
            std::vector<::shared_ptr<term::raw>> raws(_in_values.size());
            std::copy(_in_values.begin(), _in_values.end(), raws.begin());
            auto ts = to_terms(col_specs, raws, db, schema->ks_name(), bound_names);
            // Convert a single-item IN restriction to an EQ restriction
            if (ts.size() == 1) {
                return ::make_shared<restrictions::multi_column_restriction::EQ>(schema, rs, std::move(ts[0]));
            }
            return ::make_shared<restrictions::multi_column_restriction::IN_with_values>(schema, rs, ts);
        }
    }

    virtual shared_ptr<restrictions::restriction> new_slice_restriction(database& db, schema_ptr schema,
                                                                        variable_specifications& bound_names,
                                                                        statements::bound bound, bool inclusive) override {
        auto rs = receivers(db, *schema);
        std::vector<lw_shared_ptr<column_specification>> col_specs(rs.size());
        std::transform(rs.begin(), rs.end(), col_specs.begin(), [] (auto cs) {
            return cs->column_specification;
        });
        auto t = to_term(col_specs, *get_value(), db, schema->ks_name(), bound_names);
        return ::make_shared<restrictions::multi_column_restriction::slice>(schema, rs, bound, inclusive, t, _mode);
    }

    virtual shared_ptr<restrictions::restriction> new_contains_restriction(database& db, schema_ptr schema,
                                                                           variable_specifications& bound_names, bool is_key) override {
        throw exceptions::invalid_request_exception(format("{} cannot be used for Multi-column relations", get_operator()));
    }

    virtual ::shared_ptr<restrictions::restriction> new_LIKE_restriction(
            database& db, schema_ptr schema, variable_specifications& bound_names) override {
        throw exceptions::invalid_request_exception("LIKE cannot be used for Multi-column relations");
    }

    virtual ::shared_ptr<relation> maybe_rename_identifier(const column_identifier::raw& from, column_identifier::raw to) override {
        auto new_entities = boost::copy_range<decltype(_entities)>(_entities | boost::adaptors::transformed([&] (auto&& entity) {
            return *entity == from ? ::make_shared<column_identifier::raw>(to) : entity;
        }));
        return create_multi_column_relation(std::move(new_entities), _relation_type, _values_or_marker, _in_values, _in_marker);
    }

    virtual shared_ptr<term> to_term(const std::vector<lw_shared_ptr<column_specification>>& receivers,
                                     const term::raw& raw, database& db, const sstring& keyspace,
                                     variable_specifications& bound_names) const override {
        const auto& as_multi_column_raw = dynamic_cast<const term::multi_column_raw&>(raw);
        auto t = as_multi_column_raw.prepare(db, keyspace, receivers);
        t->collect_marker_specification(bound_names);
        return t;
    }

    std::vector<const column_definition*> receivers(database& db, const schema& schema) {
        using namespace statements::request_validations;

        int previous_position = -1;
        std::vector<const column_definition*> names;
        for (auto&& raw : get_entities()) {
            const auto& def = to_column_definition(schema, *raw);
            check_true(def.is_clustering_key(), "Multi-column relations can only be applied to clustering columns but was applied to: %s", def.name_as_text());
            check_false(std::count(names.begin(), names.end(), &def), "Column \"%s\" appeared twice in a relation: %s", def.name_as_text(), to_string());

            // FIXME: the following restriction should be removed (CASSANDRA-8613)
            if (def.position() != unsigned(previous_position + 1)) {
                check_false(previous_position == -1, "Clustering columns may not be skipped in multi-column relations. "
                                                     "They should appear in the PRIMARY KEY order. Got %s", to_string());
                throw exceptions::invalid_request_exception(format("Clustering columns must appear in the PRIMARY KEY order in multi-column relations: {}", to_string()));
            }
            names.emplace_back(&def);
            previous_position = def.position();
        }
        return names;
    }

    virtual sstring to_string() const override {
        sstring str = tuples::tuple_to_string(_entities);
        if (is_IN()) {
            str += " IN ";
            str += !_in_marker ? "?" : tuples::tuple_to_string(_in_values);
            return str;
        }
        return format("{} {} {}", str, _relation_type, _values_or_marker->to_string());
    }
};

}
