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

#include "cql3/single_column_relation.hh"
#include "cql3/restrictions/single_column_restriction.hh"
#include "cql3/statements/request_validations.hh"
#include "cql3/cql3_type.hh"
#include "cql3/lists.hh"
#include "unimplemented.hh"
#include "types/map.hh"
#include "types/list.hh"

#include <seastar/util/defer.hh>

using namespace cql3::expr;

namespace cql3 {

expression
single_column_relation::to_expression(const std::vector<lw_shared_ptr<column_specification>>& receivers,
                                      const expr::expression& raw,
                                      data_dictionary::database db,
                                      const sstring& keyspace,
                                      prepare_context& ctx) const {
    // TODO: optimize vector away, accept single column_specification
    assert(receivers.size() == 1);
    auto expr = prepare_expression(raw, db, keyspace, receivers[0]);
    expr::fill_prepare_context(expr, ctx);
    return expr;
}

::shared_ptr<restrictions::restriction>
single_column_relation::new_EQ_restriction(data_dictionary::database db, schema_ptr schema, prepare_context& ctx) {
    const column_definition& column_def = to_column_definition(*schema, *_entity);
    auto reset_processing_pk_column = defer([&ctx] () noexcept { ctx.set_processing_pk_restrictions(false); });
    if (column_def.is_partition_key()) {
        ctx.set_processing_pk_restrictions(true);
    }
    if (!_map_key) {
        auto r = ::make_shared<restrictions::single_column_restriction>(column_def);
        auto e = to_expression(to_receivers(*schema, column_def), *_value, db, schema->ks_name(), ctx);
        r->expression = binary_operator{column_value{&column_def}, expr::oper_t::EQ, std::move(e)};
        return r;
    }
    auto&& receivers = to_receivers(*schema, column_def);
    auto&& entry_key = to_expression({receivers[0]}, *_map_key, db, schema->ks_name(), ctx);
    auto&& entry_value = to_expression({receivers[1]}, *_value, db, schema->ks_name(), ctx);
    auto r = make_shared<restrictions::single_column_restriction>(column_def);
    r->expression = binary_operator{
        column_value(&column_def, std::move(entry_key)), oper_t::EQ, std::move(entry_value)};
    return r;
}

::shared_ptr<restrictions::restriction>
single_column_relation::new_IN_restriction(data_dictionary::database db, schema_ptr schema, prepare_context& ctx) {
    using namespace restrictions;
    const column_definition& column_def = to_column_definition(*schema, *_entity);
    auto reset_processing_pk_column = defer([&ctx] () noexcept { ctx.set_processing_pk_restrictions(false); });
    if (column_def.is_partition_key()) {
        ctx.set_processing_pk_restrictions(true);
    }
    auto receivers = to_receivers(*schema, column_def);
    assert(_in_values.empty() || !_value);
    if (_value) {
        auto e = to_expression(receivers, *_value, db, schema->ks_name(), ctx);
        auto r = ::make_shared<single_column_restriction>(column_def);
        r->expression = binary_operator{column_value{&column_def}, expr::oper_t::IN, std::move(e)};
        return r;
    }
    auto expressions = to_expressions(receivers, _in_values, db, schema->ks_name(), ctx);
    // Convert a single-item IN restriction to an EQ restriction
    if (expressions.size() == 1) {
        auto r = ::make_shared<single_column_restriction>(column_def);
        r->expression = binary_operator{column_value{&column_def}, expr::oper_t::EQ, std::move(expressions[0])};
        return r;
    }
    auto r = ::make_shared<single_column_restriction>(column_def);
    collection_constructor list_value {
        .style = collection_constructor::style_type::list,
        .elements = std::move(expressions),
        .type = list_type_impl::get_instance(column_def.type, false),
    };
    r->expression = binary_operator{
            column_value{&column_def},
            expr::oper_t::IN,
            std::move(list_value)};
    return r;
}

::shared_ptr<restrictions::restriction>
single_column_relation::new_LIKE_restriction(
        data_dictionary::database db, schema_ptr schema, prepare_context& ctx) {
    const column_definition& column_def = to_column_definition(*schema, *_entity);
    if (!column_def.type->is_string()) {
        throw exceptions::invalid_request_exception(
                format("LIKE is allowed only on string types, which {} is not", column_def.name_as_text()));
    }
    auto e = to_expression(to_receivers(*schema, column_def), *_value, db, schema->ks_name(), ctx);
    auto r = ::make_shared<restrictions::single_column_restriction>(column_def);
    r->expression = binary_operator{column_value{&column_def}, expr::oper_t::LIKE, std::move(e)};
    return r;
}

std::vector<lw_shared_ptr<column_specification>>
single_column_relation::to_receivers(const schema& schema, const column_definition& column_def) const
{
    using namespace statements::request_validations;
    auto receiver = column_def.column_specification;

    if (schema.is_dense() && column_def.is_regular()) {
        throw exceptions::invalid_request_exception(format("Predicates on the non-primary-key column ({}) of a COMPACT table are not yet supported", column_def.name_as_text()));
    }

    if (is_contains() && !receiver->type->is_collection()) {
        throw exceptions::invalid_request_exception(format("Cannot use CONTAINS on non-collection column \"{}\"", receiver->name));
    }

    if (is_contains_key()) {
        if (!dynamic_cast<const map_type_impl*>(receiver->type.get())) {
            throw exceptions::invalid_request_exception(format("Cannot use CONTAINS KEY on non-map column {}", receiver->name));
        }
    }

    if (_map_key) {
        check_false(dynamic_cast<const list_type_impl*>(receiver->type.get()), "Indexes on list entries ({}[index] = value) are not currently supported.", receiver->name);
        check_true(dynamic_cast<const map_type_impl*>(receiver->type.get()), "Column {} cannot be used as a map", receiver->name);
        check_true(receiver->type->is_multi_cell(), "Map-entry equality predicates on frozen map column {} are not supported", receiver->name);
        check_true(is_EQ(), "Only EQ relations are supported on map entries");
    }

    if (receiver->type->is_collection()) {
        // We don't support relations against entire collections (unless they're frozen), like "numbers = {1, 2, 3}"
        check_false(receiver->type->is_multi_cell() && !is_legal_relation_for_non_frozen_collection(),
                   "Collection column '{}' ({}) cannot be restricted by a '{}' relation",
                   receiver->name,
                   receiver->type->as_cql3_type(),
                   get_operator());

        if (is_contains_key() || is_contains()) {
            receiver = make_collection_receiver(receiver, is_contains_key());
        } else if (receiver->type->is_multi_cell() && _map_key && is_EQ()) {
            return {
                make_collection_receiver(receiver, true),
                make_collection_receiver(receiver, false),
            };
        }
    }

    return {std::move(receiver)};
}

}
