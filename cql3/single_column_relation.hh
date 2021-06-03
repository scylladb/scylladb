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

#include <vector>
#include "cql3/restrictions/single_column_restriction.hh"
#include "statements/request_validations.hh"

#include <seastar/core/shared_ptr.hh>
#include "to_string.hh"

#include "cql3/relation.hh"
#include "cql3/column_identifier.hh"
#include "cql3/expr/expression.hh"
#include "cql3/term.hh"
#include "types/collection.hh"

namespace cql3 {

/**
 * Relations encapsulate the relationship between an entity of some kind, and
 * a value (term). For example, <key> > "start" or "colname1" = "somevalue".
 *
 */
class single_column_relation final : public relation {
private:
    ::shared_ptr<column_identifier::raw> _entity;
    ::shared_ptr<term::raw> _map_key;
    ::shared_ptr<term::raw> _value;
    std::vector<::shared_ptr<term::raw>> _in_values;
public:
    single_column_relation(::shared_ptr<column_identifier::raw> entity, ::shared_ptr<term::raw> map_key,
        expr::oper_t type, ::shared_ptr<term::raw> value, std::vector<::shared_ptr<term::raw>> in_values)
            : relation(type)
            , _entity(std::move(entity))
            , _map_key(std::move(map_key))
            , _value(std::move(value))
            , _in_values(std::move(in_values))
    { }

    /**
     * Creates a new relation.
     *
     * @param entity the kind of relation this is; what the term is being compared to.
     * @param map_key the key into the entity identifying the value the term is being compared to.
     * @param type the type that describes how this entity relates to the value.
     * @param value the value being compared.
     */
    single_column_relation(::shared_ptr<column_identifier::raw> entity, ::shared_ptr<term::raw> map_key,
        expr::oper_t type, ::shared_ptr<term::raw> value)
            : single_column_relation(std::move(entity), std::move(map_key), type, std::move(value), {})
    { }

    /**
     * Creates a new relation.
     *
     * @param entity the kind of relation this is; what the term is being compared to.
     * @param type the type that describes how this entity relates to the value.
     * @param value the value being compared.
     */
    single_column_relation(::shared_ptr<column_identifier::raw> entity, expr::oper_t type, ::shared_ptr<term::raw> value)
        : single_column_relation(std::move(entity), {}, type, std::move(value))
    { }

    static ::shared_ptr<single_column_relation> create_in_relation(::shared_ptr<column_identifier::raw> entity,
                                                                   std::vector<::shared_ptr<term::raw>> in_values) {
        return ::make_shared<single_column_relation>(std::move(entity), nullptr, expr::oper_t::IN, nullptr, std::move(in_values));
    }

    ::shared_ptr<column_identifier::raw> get_entity() {
        return _entity;
    }

    ::shared_ptr<term::raw> get_value() {
        return _value;
    }

protected:
    virtual ::shared_ptr<term> to_term(const std::vector<lw_shared_ptr<column_specification>>& receivers,
                          const term::raw& raw, database& db, const sstring& keyspace,
                          variable_specifications& bound_names) const override;

#if 0
    public SingleColumnRelation withNonStrictOperator()
    {
        switch (relationType)
        {
            case GT: return new SingleColumnRelation(entity, expr::oper_t.GTE, value);
            case LT: return new SingleColumnRelation(entity, expr::oper_t.LTE, value);
            default: return this;
        }
    }
#endif

    virtual sstring to_string() const override {
        auto entity_as_string = _entity->to_cql_string();
        if (_map_key) {
            entity_as_string = format("{}[{}]", std::move(entity_as_string), _map_key->to_string());
        }

        if (is_IN()) {
            return format("{} IN ({})", entity_as_string, join(", ", _in_values));
        }

        return format("{} {} {}", entity_as_string, _relation_type, _value->to_string());
    }

protected:
    virtual ::shared_ptr<restrictions::restriction> new_EQ_restriction(database& db, schema_ptr schema,
                                           variable_specifications& bound_names);

    virtual ::shared_ptr<restrictions::restriction> new_IN_restriction(database& db, schema_ptr schema,
                                           variable_specifications& bound_names) override;

    virtual ::shared_ptr<restrictions::restriction> new_slice_restriction(database& db, schema_ptr schema,
            variable_specifications& bound_names,
            statements::bound bound,
            bool inclusive) override {
        auto&& column_def = to_column_definition(*schema, *_entity);

        if (column_def.type->references_duration()) {
            using statements::request_validations::check_false;
            const auto& ty = *column_def.type;

            check_false(ty.is_collection(), "Slice restrictions are not supported on collections containing durations");
            check_false(ty.is_tuple(), "Slice restrictions are not supported on tuples containing durations");
            check_false(ty.is_user_type(), "Slice restrictions are not supported on UDTs containing durations");

            // We're a duration.
            throw exceptions::invalid_request_exception("Slice restrictions are not supported on duration columns");
        }

        auto term = to_term(to_receivers(*schema, column_def), *_value, db, schema->ks_name(), bound_names);
        auto r = ::make_shared<restrictions::single_column_restriction>(column_def);
        r->expression = expr::binary_operator{&column_def, _relation_type, std::move(term)};
        return r;
    }

    virtual shared_ptr<restrictions::restriction> new_contains_restriction(database& db, schema_ptr schema,
                                                 variable_specifications& bound_names,
                                                 bool is_key) override {
        auto&& column_def = to_column_definition(*schema, *_entity);
        auto term = to_term(to_receivers(*schema, column_def), *_value, db, schema->ks_name(), bound_names);
        auto r = ::make_shared<restrictions::single_column_restriction>(column_def);
        r->expression = expr::binary_operator{
                &column_def, is_key ? expr::oper_t::CONTAINS_KEY : expr::oper_t::CONTAINS, std::move(term)};
        return r;
    }

    virtual ::shared_ptr<restrictions::restriction> new_LIKE_restriction(
            database& db, schema_ptr schema, variable_specifications& bound_names) override;

    virtual ::shared_ptr<relation> maybe_rename_identifier(const column_identifier::raw& from, column_identifier::raw to) override {
        return *_entity == from
            ? ::make_shared<single_column_relation>(
                  ::make_shared<column_identifier::raw>(std::move(to)), _map_key, _relation_type, _value, _in_values)
            : static_pointer_cast<single_column_relation>(shared_from_this());
    }

private:
    /**
     * Returns the receivers for this relation.
     *
     * @param schema the Column Family meta data
     * @param column_def the column definition
     * @return the receivers for the specified relation.
     * @throws exceptions::invalid_request_exception if the relation is invalid
     */
    std::vector<lw_shared_ptr<column_specification>> to_receivers(const schema& schema, const column_definition& column_def) const;

    static lw_shared_ptr<column_specification> make_collection_receiver(lw_shared_ptr<column_specification> receiver, bool for_key) {
        return static_cast<const collection_type_impl*>(receiver->type.get())->make_collection_receiver(*receiver, for_key);
    }

    bool is_legal_relation_for_non_frozen_collection() const {
        return is_contains_key() || is_contains() || is_map_entry_equality();
    }

    bool is_map_entry_equality() const {
        return _map_key && is_EQ();
    }
};

};
