/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "cql3/column_condition.hh"
#include "statements/request_validations.hh"
#include "unimplemented.hh"
#include "lists.hh"
#include "maps.hh"
#include <boost/range/algorithm_ext/push_back.hpp>
#include "types/map.hh"
#include "types/list.hh"
#include "utils/like_matcher.hh"
#include "expr/expression.hh"

namespace {

void validate_operation_on_durations(const abstract_type& type, cql3::expr::oper_t op) {
    using cql3::statements::request_validations::check_false;

    if (is_slice(op) && type.references_duration()) {
        check_false(type.is_collection(), "Slice conditions are not supported on collections containing durations");
        check_false(type.is_tuple(), "Slice conditions are not supported on tuples containing durations");
        check_false(type.is_user_type(), "Slice conditions are not supported on UDTs containing durations");

        // We're a duration.
        throw exceptions::invalid_request_exception(format("Slice conditions are not supported on durations"));
    }
}

} // end of anonymous namespace

namespace cql3 {

static
expr::expression
build_condition(const column_definition& column, std::optional<expr::expression> collection_element,
        std::optional<expr::expression> value, std::vector<expr::expression> in_values,
        expr::oper_t op) {
    using namespace expr;

    auto lhs = expression(column_value{&column});
    if (collection_element) {
        auto lhs_type = type_of(lhs);
        auto col_type = dynamic_pointer_cast<const collection_type_impl>(lhs_type);
        assert(col_type);
        auto element_type = col_type->value_comparator();
        assert(element_type);
        lhs = subscript{std::move(lhs), std::move(*collection_element), std::move(element_type)};
    }
    if (op == oper_t::IN && !value.has_value()) {
        auto rhs = collection_constructor{collection_constructor::style_type::list, std::move(in_values), list_type_impl::get_instance(type_of(lhs), false)};
        return binary_operator(std::move(lhs), op, std::move(rhs));
    } else {
        assert(value.has_value());
        assert(in_values.empty());
        return binary_operator(std::move(lhs), op, std::move(*value));
    }
}

static
expr::expression
update_for_lwt_null_equality_rules(const expr::expression& e) {
    using namespace expr;

    return search_and_replace(e, [] (const expression& e) -> expression {
        if (auto* binop = as_if<binary_operator>(&e)) {
            auto new_binop = *binop;
            new_binop.null_handling = expr::null_handling_style::lwt_nulls;
            return new_binop;
        }
        return e;
    });
}

column_condition::column_condition(const column_definition& column, std::optional<expr::expression> collection_element,
    std::optional<expr::expression> value, std::vector<expr::expression> in_values,
    expr::oper_t op)
        : _expr(build_condition(column, std::move(collection_element), std::move(value), std::move(in_values), op))
{
    // If a collection is multi-cell and not frozen, it is returned as a map even if the
    // underlying data type is "set" or "list". This is controlled by
    // partition_slice::collections_as_maps enum, which is set when preparing a read command
    // object. Representing a list as a map<timeuuid, listval> is necessary to identify the list field
    // being updated, e.g. in case of UPDATE t SET list[3] = null WHERE a = 1 IF list[3]
    // = 'key'
    //
    // We adjust for it by reinterpreting the returned value as a list, since the map
    // representation is not needed here.
    _expr = expr::adjust_for_collection_as_maps(_expr);

    _expr = expr::optimize_like(_expr);

    _expr = update_for_lwt_null_equality_rules(_expr);
}

void column_condition::collect_marker_specificaton(prepare_context& ctx) {
    expr::fill_prepare_context(_expr, ctx);
}

bool column_condition::applies_to(const expr::evaluation_inputs& inputs) const {
    static auto true_value = raw_value::make_value(data_value(true).serialize());
    return expr::evaluate(_expr, inputs) == true_value;
}

lw_shared_ptr<column_condition>
column_condition::raw::prepare(data_dictionary::database db, const sstring& keyspace, const schema& schema) const {
    auto id = _lhs->prepare_column_identifier(schema);
    const column_definition* def = get_column_definition(schema, *id);
    if (!def) {
        throw exceptions::invalid_request_exception(format("Unknown identifier {}", *id));
    }

    if (def->is_primary_key()) {
        throw exceptions::invalid_request_exception(format("PRIMARY KEY column '{}' cannot have IF conditions", *id));
    }

    auto& receiver = *def;

    if (receiver.type->is_counter()) {
        throw exceptions::invalid_request_exception("Conditions on counters are not supported");
    }
    std::optional<expr::expression> collection_element_expression;
    lw_shared_ptr<column_specification> value_spec = receiver.column_specification;

    if (_collection_element) {
        if (!receiver.type->is_collection()) {
            throw exceptions::invalid_request_exception(format("Invalid element access syntax for non-collection column {}",
                        receiver.name_as_text()));
        }
        // Pass  a correct type specification to the collection_element->prepare(), so that it can
        // later be used to validate the parameter type is compatible with receiver type.
        lw_shared_ptr<column_specification> element_spec;
        auto ctype = static_cast<const collection_type_impl*>(receiver.type.get());
        const column_specification& recv_column_spec = *receiver.column_specification;
        if (ctype->get_kind() == abstract_type::kind::list) {
            element_spec = lists::index_spec_of(recv_column_spec);
            value_spec = lists::value_spec_of(recv_column_spec);
        } else if (ctype->get_kind() == abstract_type::kind::map) {
            element_spec = maps::key_spec_of(recv_column_spec);
            value_spec = maps::value_spec_of(recv_column_spec);
        } else if (ctype->get_kind() == abstract_type::kind::set) {
            throw exceptions::invalid_request_exception(format("Invalid element access syntax for set column {}",
                        receiver.name_as_text()));
        } else {
            throw exceptions::invalid_request_exception(
                    format("Unsupported collection type {} in a condition with element access", ctype->cql3_type_name()));
        }
        collection_element_expression = prepare_expression(*_collection_element, db, keyspace, nullptr, element_spec);
    }

    if (is_compare(_op)) {
        validate_operation_on_durations(*receiver.type, _op);
        return column_condition::condition(receiver, std::move(collection_element_expression),
                prepare_expression(*_value, db, keyspace, nullptr, value_spec), _op);
    }

    if (_op == expr::oper_t::LIKE) {
            // Pass through rhs value, matcher object built on execution (or optimized in constructor)
            // TODO: caller should validate parametrized LIKE pattern
            return column_condition::condition(receiver, std::move(collection_element_expression),
                    prepare_expression(*_value, db, keyspace, nullptr, value_spec), _op);
    }

    if (_op != expr::oper_t::IN) {
        throw exceptions::invalid_request_exception(format("Unsupported operator type {} in a condition ", _op));
    }

    if (_in_marker) {
        assert(_in_values.empty());
        auto in_name = ::make_shared<column_identifier>(format("in({})", value_spec->name->text()), true);
        lw_shared_ptr<column_specification> in_list_receiver = make_lw_shared<column_specification>(value_spec->ks_name, value_spec->cf_name, in_name, list_type_impl::get_instance(value_spec->type, false));
        expr::expression multi_item_term = prepare_expression(*_in_marker, db, keyspace, nullptr, in_list_receiver);
        return column_condition::in_condition(receiver, collection_element_expression, std::move(multi_item_term), {});
    }
    // Both _in_values and in _in_marker can be missing in case of empty IN list: "a IN ()"
    std::vector<expr::expression> terms;
    terms.reserve(_in_values.size());
    for (auto&& value : _in_values) {
        terms.push_back(prepare_expression(value, db, keyspace, nullptr, value_spec));
    }
    return column_condition::in_condition(receiver, std::move(collection_element_expression),
                                          std::nullopt, std::move(terms));
}

} // end of namespace cql3
