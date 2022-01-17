/*
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
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
    std::optional<expr::expression> _map_key;
    std::optional<expr::expression> _value;
    std::vector<expr::expression> _in_values;
public:
    single_column_relation(::shared_ptr<column_identifier::raw> entity, std::optional<expr::expression> map_key,
        expr::oper_t type, std::optional<expr::expression> value, std::vector<expr::expression> in_values)
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
    single_column_relation(::shared_ptr<column_identifier::raw> entity, std::optional<expr::expression> map_key,
        expr::oper_t type, std::optional<expr::expression> value)
            : single_column_relation(std::move(entity), std::move(map_key), type, std::move(value), {})
    { }

    /**
     * Creates a new relation.
     *
     * @param entity the kind of relation this is; what the term is being compared to.
     * @param type the type that describes how this entity relates to the value.
     * @param value the value being compared.
     */
    single_column_relation(::shared_ptr<column_identifier::raw> entity, expr::oper_t type, expr::expression value)
        : single_column_relation(std::move(entity), {}, type, std::move(value))
    { }

    static ::shared_ptr<single_column_relation> create_in_relation(::shared_ptr<column_identifier::raw> entity,
                                                                   std::vector<expr::expression> in_values) {
        return ::make_shared<single_column_relation>(std::move(entity), std::nullopt, expr::oper_t::IN, std::nullopt, std::move(in_values));
    }

    ::shared_ptr<column_identifier::raw> get_entity() {
        return _entity;
    }

    const std::optional<expr::expression>& get_value() {
        return _value;
    }

protected:
    virtual expr::expression to_expression(const std::vector<lw_shared_ptr<column_specification>>& receivers,
                                           const expr::expression& raw, data_dictionary::database db, const sstring& keyspace,
                                           prepare_context& ctx) const override;

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
            entity_as_string = format("{}[{}]", std::move(entity_as_string), *_map_key);
        }

        if (is_IN()) {
            return format("{} IN ({})", entity_as_string, join(", ", _in_values));
        }

        return format("{} {} {}", entity_as_string, _relation_type, *_value);
    }

protected:
    virtual ::shared_ptr<restrictions::restriction> new_EQ_restriction(data_dictionary::database db, schema_ptr schema,
                                           prepare_context& ctx) override;

    virtual ::shared_ptr<restrictions::restriction> new_IN_restriction(data_dictionary::database db, schema_ptr schema,
                                           prepare_context& ctx) override;

    virtual ::shared_ptr<restrictions::restriction> new_slice_restriction(data_dictionary::database db, schema_ptr schema,
            prepare_context& ctx,
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

        auto e = to_expression(to_receivers(*schema, column_def), *_value, db, schema->ks_name(), ctx);
        auto r = ::make_shared<restrictions::single_column_restriction>(column_def);
        r->expression = expr::binary_operator{expr::column_value{&column_def}, _relation_type, std::move(e)};
        return r;
    }

    virtual shared_ptr<restrictions::restriction> new_contains_restriction(data_dictionary::database db, schema_ptr schema,
                                                 prepare_context& ctx,
                                                 bool is_key) override {
        auto&& column_def = to_column_definition(*schema, *_entity);
        auto e = to_expression(to_receivers(*schema, column_def), *_value, db, schema->ks_name(), ctx);
        auto r = ::make_shared<restrictions::single_column_restriction>(column_def);
        r->expression = expr::binary_operator{
                expr::column_value{&column_def}, is_key ? expr::oper_t::CONTAINS_KEY : expr::oper_t::CONTAINS, std::move(e)};
        return r;
    }

    virtual ::shared_ptr<restrictions::restriction> new_LIKE_restriction(
            data_dictionary::database db, schema_ptr schema, prepare_context& ctx) override;

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
