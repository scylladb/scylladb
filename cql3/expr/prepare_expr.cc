/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "expression.hh"
#include "expr-utils.hh"
#include "evaluate.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/castas_fcts.hh"
#include "cql3/functions/scalar_function.hh"
#include "cql3/column_identifier.hh"
#include "cql3/lists.hh"
#include "cql3/maps.hh"
#include "cql3/sets.hh"
#include "cql3/user_types.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "types/map.hh"
#include "types/user.hh"
#include "exceptions/unrecognized_entity_exception.hh"
#include "utils/like_matcher.hh"

#include <boost/range/algorithm/count.hpp>

namespace cql3::expr {

static const column_value resolve_column(const unresolved_identifier& col_ident, const schema& schema);

static assignment_testable::test_result expression_test_assignment(const data_type& expr_type,
                                                                   const column_specification& receiver);


static
lw_shared_ptr<column_specification>
column_specification_of(const expression& e) {
    return visit(overloaded_functor{
        [] (const column_value& cv) {
            return cv.col->column_specification;
        },
        [&] (const ExpressionElement auto& other) {
            auto type = type_of(e);
            if (!type) {
                throw exceptions::invalid_request_exception(fmt::format("cannot infer type of {}", e));
            }
            // Fake out a column_identifier
            //
            // FIXME: come up with something better
            // This works for now because the we only call this when preparing
            // a subscript, and the grammar only allows column_values to be subscripted.
            // So we never end up in this branch. In case we do, we'll see the internal
            // representation of the expression, rather than what the user typed in.
            //
            // The correct fix is to augment expressions with a source_location member so
            // we can just point at the line and column (and quote the text) of the expression
            // we're naming. As an example, if we allow
            //
            //    WHERE {'a': 3, 'b': 5}[19.5] = 3
            //
            // then the column_identifier should be "key type of {'a': 3, 'b': 5}" - it
            // doesn't identify a column but some subexpression that we're using to infer the
            // type of the "19.5" (and failing).
            auto col_id = ::make_shared<column_identifier>(fmt::format("{}", e), true);
            return make_lw_shared<column_specification>("", "", std::move(col_id), std::move(type));
        }
    }, e);
}

static
lw_shared_ptr<column_specification>
usertype_field_spec_of(const column_specification& column, size_t field) {
    auto&& ut = static_pointer_cast<const user_type_impl>(column.type);
    auto&& name = ut->field_name(field);
    auto&& sname = sstring(reinterpret_cast<const char*>(name.data()), name.size());
    return make_lw_shared<column_specification>(
                                   column.ks_name,
                                   column.cf_name,
                                   ::make_shared<column_identifier>(column.name->to_string() + "." + sname, true),
                                   ut->field_type(field));
}

static
void
usertype_constructor_validate_assignable_to(const usertype_constructor& u, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    if (!receiver.type->is_user_type()) {
        throw exceptions::invalid_request_exception(format("Invalid user type literal for {} of type {}", *receiver.name, receiver.type->as_cql3_type()));
    }

    auto ut = static_pointer_cast<const user_type_impl>(receiver.type);
    for (size_t i = 0; i < ut->size(); i++) {
        column_identifier field(to_bytes(ut->field_name(i)), utf8_type);
        if (!u.elements.contains(field)) {
            continue;
        }
        const expression& value = u.elements.at(field);
        auto&& field_spec = usertype_field_spec_of(receiver, i);
        if (!assignment_testable::is_assignable(test_assignment(value, db, keyspace, schema_opt, *field_spec))) {
            throw exceptions::invalid_request_exception(format("Invalid user type literal for {}: field {} is not of type {}", *receiver.name, field, field_spec->type->as_cql3_type()));
        }
    }
}

static
assignment_testable::test_result
usertype_constructor_test_assignment(const usertype_constructor& u, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    try {
        usertype_constructor_validate_assignable_to(u, db, keyspace, schema_opt, receiver);
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    } catch (exceptions::invalid_request_exception& e) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
}

static
std::optional<expression>
usertype_constructor_prepare_expression(const usertype_constructor& u, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver) {
    if (!receiver) {
        return std::nullopt; // cannot infer type from {field: value}
    }
    usertype_constructor_validate_assignable_to(u, db, keyspace, schema_opt, *receiver);
    auto&& ut = static_pointer_cast<const user_type_impl>(receiver->type);
    bool all_terminal = true;

    usertype_constructor::elements_map_type prepared_elements;
    size_t found_values = 0;
    for (size_t i = 0; i < ut->size(); ++i) {
        auto&& field = column_identifier(to_bytes(ut->field_name(i)), utf8_type);
        auto iraw = u.elements.find(field);
        expression raw = expr::make_untyped_null();
        if (iraw != u.elements.end()) {
            raw = iraw->second;
            ++found_values;
        }
        expression value = prepare_expression(raw, db, keyspace, schema_opt, usertype_field_spec_of(*receiver, i));

        if (!is<constant>(value)) {
            all_terminal = false;
        }

        prepared_elements.emplace(std::move(field), std::move(value));
    }
    if (found_values != u.elements.size()) {
        // We had some field that are not part of the type
        for (auto&& id_val : u.elements) {
            auto&& id = id_val.first;
            if (!boost::range::count(ut->field_names(), id.bytes_)) {
                throw exceptions::invalid_request_exception(format("Unknown field '{}' in value of user defined type {}", id, ut->get_name_as_string()));
            }
        }
    }

    usertype_constructor value {
        .elements = std::move(prepared_elements),
        .type = ut
    };

    if (all_terminal) {
        return constant(evaluate(value, query_options::DEFAULT), value.type);
    } else {
        return value;
    }
}

extern logging::logger expr_logger;

static
lw_shared_ptr<column_specification>
map_key_spec_of(const column_specification& column) {
    return make_lw_shared<column_specification>(column.ks_name, column.cf_name,
                ::make_shared<column_identifier>(format("key({})", *column.name), true),
                dynamic_cast<const map_type_impl&>(column.type->without_reversed()).get_keys_type());
}

static
lw_shared_ptr<column_specification>
list_key_spec_of(const column_specification& column) {
    return make_lw_shared<column_specification>(column.ks_name, column.cf_name,
                ::make_shared<column_identifier>(format("index({})", *column.name), true),
                int32_type);
}

static
lw_shared_ptr<column_specification>
map_value_spec_of(const column_specification& column) {
    return make_lw_shared<column_specification>(column.ks_name, column.cf_name,
                ::make_shared<column_identifier>(format("value({})", *column.name), true),
                 dynamic_cast<const map_type_impl&>(column.type->without_reversed()).get_values_type());
}

static
void
map_validate_assignable_to(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    if (!receiver.type->without_reversed().is_map()) {
        throw exceptions::invalid_request_exception(format("Invalid map literal for {} of type {}", *receiver.name, receiver.type->as_cql3_type()));
    }
    auto&& key_spec = map_key_spec_of(receiver);
    auto&& value_spec = map_value_spec_of(receiver);
    for (auto&& entry : c.elements) {
        auto& entry_tuple = expr::as<tuple_constructor>(entry);
        if (entry_tuple.elements.size() != 2) {
            on_internal_error(expr_logger, "map element is not a tuple of arity 2");
        }
        if (!is_assignable(test_assignment(entry_tuple.elements[0], db, keyspace, schema_opt, *key_spec))) {
            throw exceptions::invalid_request_exception(format("Invalid map literal for {}: key {} is not of type {}", *receiver.name, entry_tuple.elements[0], key_spec->type->as_cql3_type()));
        }
        if (!is_assignable(test_assignment(entry_tuple.elements[1], db, keyspace, schema_opt, *value_spec))) {
            throw exceptions::invalid_request_exception(format("Invalid map literal for {}: value {} is not of type {}", *receiver.name, entry_tuple.elements[1], value_spec->type->as_cql3_type()));
        }
    }
}

static
assignment_testable::test_result
map_test_assignment(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    if (!dynamic_pointer_cast<const map_type_impl>(receiver.type)) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
    // If there is no elements, we can't say it's an exact match (an empty map if fundamentally polymorphic).
    if (c.elements.empty()) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }
    auto key_spec = maps::key_spec_of(receiver);
    auto value_spec = maps::value_spec_of(receiver);
    // It's an exact match if all are exact match, but is not assignable as soon as any is non assignable.
    auto res = assignment_testable::test_result::EXACT_MATCH;
    for (auto entry : c.elements) {
        auto& entry_tuple = expr::as<tuple_constructor>(entry);
        if (entry_tuple.elements.size() != 2) {
            on_internal_error(expr_logger, "map element is not a tuple of arity 2");
        }
        auto t1 = test_assignment(entry_tuple.elements[0], db, keyspace, schema_opt, *key_spec);
        auto t2 = test_assignment(entry_tuple.elements[1], db, keyspace, schema_opt, *value_spec);
        if (t1 == assignment_testable::test_result::NOT_ASSIGNABLE || t2 == assignment_testable::test_result::NOT_ASSIGNABLE)
            return assignment_testable::test_result::NOT_ASSIGNABLE;
        if (t1 != assignment_testable::test_result::EXACT_MATCH || t2 != assignment_testable::test_result::EXACT_MATCH)
            res = assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }
    return res;
}

static
std::optional<expression>
map_prepare_expression(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver) {
    if (!receiver) {
        // TODO: It is possible to infer the type of a map from the types of the key/value pairs
        return std::nullopt;
    }
    map_validate_assignable_to(c, db, keyspace, schema_opt, *receiver);

    auto key_spec = maps::key_spec_of(*receiver);
    auto value_spec = maps::value_spec_of(*receiver);
    const map_type_impl* map_type = dynamic_cast<const map_type_impl*>(&receiver->type->without_reversed());
    if (map_type == nullptr) {
        on_internal_error(expr_logger,
                          format("map_prepare_expression bad non-map receiver type: {}", receiver->type->name()));
    }
    data_type map_element_tuple_type = tuple_type_impl::get_instance({map_type->get_keys_type(), map_type->get_values_type()});

    // In Cassandra, an empty (unfrozen) map/set/list is equivalent to the column being null. In
    // other words a non-frozen collection only exists if it has elements.  Return nullptr right
    // away to simplify predicate evaluation.  See also
    // https://issues.apache.org/jira/browse/CASSANDRA-5141
    if (map_type->is_multi_cell() && c.elements.empty()) {
        return constant::make_null(receiver->type);
    }

    std::vector<expression> values;
    values.reserve(c.elements.size());
    bool all_terminal = true;
    for (auto&& entry : c.elements) {
        auto& entry_tuple = expr::as<tuple_constructor>(entry);
        if (entry_tuple.elements.size() != 2) {
            on_internal_error(expr_logger, "map element is not a tuple of arity 2");
        }
        expression k = prepare_expression(entry_tuple.elements[0], db, keyspace, schema_opt, key_spec);
        expression v = prepare_expression(entry_tuple.elements[1], db, keyspace, schema_opt, value_spec);

        // Check if one of values contains a nonpure function
        if (!is<constant>(k) || !is<constant>(v)) {
            all_terminal = false;
        }

        values.emplace_back(tuple_constructor {
            .elements = {std::move(k), std::move(v)},
            .type = map_element_tuple_type
        });
    }

    collection_constructor map_value {
        .style = collection_constructor::style_type::map,
        .elements = std::move(values),
        .type = receiver->type
    };
    if (all_terminal) {
        return constant(evaluate(map_value, query_options::DEFAULT), map_value.type);
    } else {
        return map_value;
    }
}

static
lw_shared_ptr<column_specification>
set_value_spec_of(const column_specification& column) {
    return make_lw_shared<column_specification>(column.ks_name, column.cf_name,
            ::make_shared<column_identifier>(format("value({})", *column.name), true),
            dynamic_cast<const set_type_impl&>(column.type->without_reversed()).get_elements_type());
}

static
void
set_validate_assignable_to(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    if (!receiver.type->without_reversed().is_set()) {
        // We've parsed empty maps as a set literal to break the ambiguity so
        // handle that case now
        if (dynamic_pointer_cast<const map_type_impl>(receiver.type) && c.elements.empty()) {
            return;
        }

        throw exceptions::invalid_request_exception(format("Invalid set literal for {} of type {}", *receiver.name, receiver.type->as_cql3_type()));
    }

    auto&& value_spec = set_value_spec_of(receiver);
    for (auto& e: c.elements) {
        if (!is_assignable(test_assignment(e, db, keyspace, schema_opt, *value_spec))) {
            throw exceptions::invalid_request_exception(format("Invalid set literal for {}: value {} is not of type {}", *receiver.name, e, value_spec->type->as_cql3_type()));
        }
    }
}

static
assignment_testable::test_result
set_test_assignment(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    if (!receiver.type->without_reversed().is_set()) {
        // We've parsed empty maps as a set literal to break the ambiguity so handle that case now
        if (dynamic_pointer_cast<const map_type_impl>(receiver.type) && c.elements.empty()) {
            return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
        }

        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }

    // If there is no elements, we can't say it's an exact match (an empty set if fundamentally polymorphic).
    if (c.elements.empty()) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }

    auto&& value_spec = set_value_spec_of(receiver);
    return test_assignment_all(c.elements, db, keyspace, schema_opt, *value_spec);
}

static
std::optional<expression>
set_prepare_expression(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver) {
    if (!receiver) {
        // TODO: It is possible to infer the type of a set from the types of the values
        return std::nullopt;
    }
    set_validate_assignable_to(c, db, keyspace, schema_opt, *receiver);

    if (c.elements.empty()) {

        // In Cassandra, an empty (unfrozen) map/set/list is equivalent to the column being null. In
        // other words a non-frozen collection only exists if it has elements.  Return nullptr right
        // away to simplify predicate evaluation.  See also
        // https://issues.apache.org/jira/browse/CASSANDRA-5141
        if (receiver->type->is_multi_cell()) {
            return constant::make_null(receiver->type);
        }
        // We've parsed empty maps as a set literal to break the ambiguity so
        // handle that case now. This branch works for frozen sets/maps only.
        const map_type_impl* maybe_map_type = dynamic_cast<const map_type_impl*>(receiver->type.get());
        if (maybe_map_type != nullptr) {
            collection_constructor map_value {
                .style = collection_constructor::style_type::map,
                .elements = {},
                .type = receiver->type
            };
            return constant(expr::evaluate(map_value, query_options::DEFAULT), map_value.type);
        }
    }

    auto value_spec = set_value_spec_of(*receiver);
    std::vector<expression> values;
    values.reserve(c.elements.size());
    bool all_terminal = true;
    for (auto& e : c.elements)
    {
        expression elem = prepare_expression(e, db, keyspace, schema_opt, value_spec);

        if (!is<constant>(elem)) {
            all_terminal = false;
        }

        values.push_back(std::move(elem));
    }

    collection_constructor value {
        .style = collection_constructor::style_type::set,
        .elements = std::move(values),
        .type = receiver->type
    };
    
    if (all_terminal) {
        return constant(evaluate(value, query_options::DEFAULT), value.type);
    } else {
        return value;
    }
}

static
lw_shared_ptr<column_specification>
list_value_spec_of(const column_specification& column) {
    return make_lw_shared<column_specification>(column.ks_name, column.cf_name,
            ::make_shared<column_identifier>(format("value({})", *column.name), true),
                dynamic_cast<const list_type_impl&>(column.type->without_reversed()).get_elements_type());
}

static
void
list_validate_assignable_to(const collection_constructor& c, data_dictionary::database db, const sstring keyspace, const schema* schema_opt, const column_specification& receiver) {
    if (!receiver.type->without_reversed().is_list()) {
        throw exceptions::invalid_request_exception(format("Invalid list literal for {} of type {}",
                *receiver.name, receiver.type->as_cql3_type()));
    }
    auto&& value_spec = list_value_spec_of(receiver);
    for (auto& e : c.elements) {
        if (!is_assignable(test_assignment(e, db, keyspace, schema_opt, *value_spec))) {
            throw exceptions::invalid_request_exception(format("Invalid list literal for {}: value {} is not of type {}",
                    *receiver.name, e, value_spec->type->as_cql3_type()));
        }
    }
}

static
assignment_testable::test_result
list_test_assignment(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    if (!dynamic_pointer_cast<const list_type_impl>(receiver.type)) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }

    // If there is no elements, we can't say it's an exact match (an empty list if fundamentally polymorphic).
    if (c.elements.empty()) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }

    auto&& value_spec = list_value_spec_of(receiver);
    return test_assignment_all(c.elements, db, keyspace, schema_opt, *value_spec);
}


static
std::optional<expression>
list_prepare_expression(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver) {
    if (!receiver) {
        // TODO: It is possible to infer the type of a list from the types of the key/value pairs
        return std::nullopt;
    }
    list_validate_assignable_to(c, db, keyspace, schema_opt, *receiver);

    // In Cassandra, an empty (unfrozen) map/set/list is equivalent to the column being null. In
    // other words a non-frozen collection only exists if it has elements. Return nullptr right
    // away to simplify predicate evaluation. See also
    // https://issues.apache.org/jira/browse/CASSANDRA-5141
    if (receiver->type->is_multi_cell() &&  c.elements.empty()) {
        return constant::make_null(receiver->type);
    }

    auto&& value_spec = list_value_spec_of(*receiver);
    std::vector<expression> values;
    values.reserve(c.elements.size());
    bool all_terminal = true;
    for (auto& e : c.elements) {
        expression elem = prepare_expression(e, db, keyspace, schema_opt, value_spec);

        if (!is<constant>(elem)) {
            all_terminal = false;
        }
        values.push_back(std::move(elem));
    }
    collection_constructor value {
        .style = collection_constructor::style_type::list,
        .elements = std::move(values),
        .type = receiver->type
    };
    if (all_terminal) {
        return constant(evaluate(value, query_options::DEFAULT), value.type);
    } else {
        return value;
    }
}

static
lw_shared_ptr<column_specification>
component_spec_of(const column_specification& column, size_t component) {
    return make_lw_shared<column_specification>(
            column.ks_name,
            column.cf_name,
            ::make_shared<column_identifier>(format("{}[{:d}]", column.name, component), true),
            static_pointer_cast<const tuple_type_impl>(column.type->underlying_type())->type(component));
}

static
void
tuple_constructor_validate_assignable_to(const tuple_constructor& tc, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    auto tt = dynamic_pointer_cast<const tuple_type_impl>(receiver.type->underlying_type());
    if (!tt) {
        throw exceptions::invalid_request_exception(format("Invalid tuple type literal for {} of type {}", *receiver.name, receiver.type->as_cql3_type()));
    }
    for (size_t i = 0; i < tc.elements.size(); ++i) {
        if (i >= tt->size()) {
            throw exceptions::invalid_request_exception(format("Invalid tuple literal for {}: too many elements. Type {} expects {:d} but got {:d}",
                                                            *receiver.name, tt->as_cql3_type(), tt->size(), tc.elements.size()));
        }

        auto&& value = tc.elements[i];
        auto&& spec = component_spec_of(receiver, i);
        if (!assignment_testable::is_assignable(test_assignment(value, db, keyspace, schema_opt, *spec))) {
            throw exceptions::invalid_request_exception(format("Invalid tuple literal for {}: component {:d} is not of type {}", *receiver.name, i, spec->type->as_cql3_type()));
        }
    }
}

static
assignment_testable::test_result
tuple_constructor_test_assignment(const tuple_constructor& tc, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    try {
        tuple_constructor_validate_assignable_to(tc, db, keyspace, schema_opt, receiver);
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    } catch (exceptions::invalid_request_exception& e) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
}

static
std::optional<expression>
tuple_constructor_prepare_nontuple(const tuple_constructor& tc, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver) {
    if (receiver) {
        tuple_constructor_validate_assignable_to(tc, db, keyspace, schema_opt, *receiver);
    }
    std::vector<expression> values;
    bool all_terminal = true;
    for (size_t i = 0; i < tc.elements.size(); ++i) {
        lw_shared_ptr<column_specification> component_receiver;
        if (receiver) {
            component_receiver = component_spec_of(*receiver, i);
        }
        std::optional<expression> value_opt = try_prepare_expression(tc.elements[i], db, keyspace, schema_opt, component_receiver);
        if (!value_opt) {
            return std::nullopt;
        }
        auto& value = *value_opt;
        if (!is<constant>(value)) {
            all_terminal = false;
        }
        values.push_back(std::move(value));
    }
    data_type type;
    if (receiver) {
        type = receiver->type;
    } else {
        type = tuple_type_impl::get_instance(boost::copy_range<std::vector<data_type>>(
                values
                | boost::adaptors::transformed(type_of)));
    }
    tuple_constructor value {
        .elements  = std::move(values),
        .type = std::move(type),
    };
    if (all_terminal) {
        return constant(evaluate(value, query_options::DEFAULT), value.type);
    } else {
        return value;
    }
}

}

template <> struct fmt::formatter<cql3::expr::untyped_constant::type_class> : fmt::formatter<string_view> {
    auto format(cql3::expr::untyped_constant::type_class t, fmt::format_context& ctx) const {
        using enum cql3::expr::untyped_constant::type_class;
        std::string_view name;
        switch (t) {
            case string:   name = "STRING"; break;
            case integer:  name = "INTEGER"; break;
            case uuid:     name = "UUID"; break;
            case floating_point:    name = "FLOAT"; break;
            case boolean:  name = "BOOLEAN"; break;
            case hex:      name = "HEX"; break;
            case duration: name = "DURATION"; break;
            case null:     name = "NULL"; break;
        }
        return fmt::format_to(ctx.out(), "{}", name);
    }
};

namespace cql3::expr {

static
bytes
untyped_constant_parsed_value(const untyped_constant uc, data_type validator)
{
    try {
        if (uc.partial_type == untyped_constant::type_class::hex && validator == bytes_type) {
            auto v = static_cast<sstring_view>(uc.raw_text);
            v.remove_prefix(2);
            return validator->from_string(v);
        }
        if (validator->is_counter()) {
            return long_type->from_string(uc.raw_text);
        }
        return validator->from_string(uc.raw_text);
    } catch (const marshal_exception& e) {
        throw exceptions::invalid_request_exception(e.what());
    }
}

static
assignment_testable::test_result
untyped_constant_test_assignment(const untyped_constant& uc, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver)
{
    bool uc_is_null = uc.partial_type == untyped_constant::type_class::null;
    auto receiver_type = receiver.type->as_cql3_type();
    if ((receiver_type.is_collection() || receiver_type.is_user_type()) && !uc_is_null) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
    if (!receiver_type.is_native()) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }
    auto kind = receiver_type.get_kind();
    switch (uc.partial_type) {
        case untyped_constant::type_class::string:
            if (cql3_type::kind_enum_set::frozen<
                    cql3_type::kind::ASCII,
                    cql3_type::kind::TEXT,
                    cql3_type::kind::INET,
                    cql3_type::kind::TIMESTAMP,
                    cql3_type::kind::DATE,
                    cql3_type::kind::TIME>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case untyped_constant::type_class::integer:
            if (cql3_type::kind_enum_set::frozen<
                    cql3_type::kind::BIGINT,
                    cql3_type::kind::COUNTER,
                    cql3_type::kind::DECIMAL,
                    cql3_type::kind::DOUBLE,
                    cql3_type::kind::FLOAT,
                    cql3_type::kind::INT,
                    cql3_type::kind::SMALLINT,
                    cql3_type::kind::TIMESTAMP,
                    cql3_type::kind::DATE,
                    cql3_type::kind::TINYINT,
                    cql3_type::kind::VARINT>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case untyped_constant::type_class::uuid:
            if (cql3_type::kind_enum_set::frozen<
                    cql3_type::kind::UUID,
                    cql3_type::kind::TIMEUUID>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case untyped_constant::type_class::floating_point:
            if (cql3_type::kind_enum_set::frozen<
                    cql3_type::kind::DECIMAL,
                    cql3_type::kind::DOUBLE,
                    cql3_type::kind::FLOAT>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case untyped_constant::type_class::boolean:
            if (kind == cql3_type::kind_enum_set::prepare<cql3_type::kind::BOOLEAN>()) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case untyped_constant::type_class::hex:
            if (kind == cql3_type::kind_enum_set::prepare<cql3_type::kind::BLOB>()) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case untyped_constant::type_class::duration:
            if (kind == cql3_type::kind_enum_set::prepare<cql3_type::kind::DURATION>()) {
                return assignment_testable::test_result::EXACT_MATCH;
            }
            break;
        case untyped_constant::type_class::null:
            return receiver.type->is_counter()
                ? assignment_testable::test_result::NOT_ASSIGNABLE
                : assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }
    return assignment_testable::test_result::NOT_ASSIGNABLE;
}

static
std::optional<expression>
untyped_constant_prepare_expression(const untyped_constant& uc, data_dictionary::database db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver)
{
    if (!receiver) {
        // TODO: It is possible to infer the type of a constant by looking at the value and selecting the smallest fit
        return std::nullopt;
    }
    if (!is_assignable(untyped_constant_test_assignment(uc, db, keyspace, *receiver))) {
      if (uc.partial_type != untyped_constant::type_class::null) {
        throw exceptions::invalid_request_exception(format("Invalid {} constant ({}) for \"{}\" of type {}",
            uc.partial_type, uc.raw_text, *receiver->name, receiver->type->as_cql3_type().to_string()));
      } else {
        throw exceptions::invalid_request_exception("Invalid null value for counter increment/decrement");
      }
    }

    if (uc.partial_type == untyped_constant::type_class::null) {
        return constant::make_null(receiver->type);
    }

    raw_value raw_val = cql3::raw_value::make_value(untyped_constant_parsed_value(uc, receiver->type));
    return constant(std::move(raw_val), receiver->type);
}

static
assignment_testable::test_result
bind_variable_test_assignment(const bind_variable& bv, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver) {
    return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
}

static
std::optional<bind_variable>
bind_variable_prepare_expression(const bind_variable& bv, data_dictionary::database db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver)
{   
    if (!receiver) {
        return std::nullopt;
    }

    return bind_variable {
        .bind_index = bv.bind_index,
        .receiver = receiver
    };
}

static data_type cast_get_prepared_type(const cast& c, data_dictionary::database db, const sstring& keyspace) {
    data_type cast_type = std::visit(overloaded_functor {
        [&](const shared_ptr<cql3_type::raw>& raw_type) { return raw_type->prepare(db, keyspace).get_type(); },
        [](const data_type& prepared_type) {return prepared_type;}
    }, c.type);

    return cast_type;
}

static
lw_shared_ptr<column_specification>
casted_spec_of(const cast& c, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver) {
    data_type cast_type = cast_get_prepared_type(c, db, keyspace);

    sstring display_name = format("({}){:user}", cast_type->cql3_type_name(), c.arg);

    return make_lw_shared<column_specification>(receiver.ks_name, receiver.cf_name,
            ::make_shared<column_identifier>(display_name, true), cast_type);
}

static
assignment_testable::test_result
cast_test_assignment(const cast& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    try {
        data_type casted_type = cast_get_prepared_type(c, db, keyspace);
        if (receiver.type == casted_type) {
            return assignment_testable::test_result::EXACT_MATCH;
        } else if (receiver.type->is_value_compatible_with(*casted_type)) {
            return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
        } else {
            return assignment_testable::test_result::NOT_ASSIGNABLE;
        }
    } catch (exceptions::invalid_request_exception& e) {
        abort();
    }
}

// expr::cast shows up when the user uses a C-style cast with destination type in parenthesis.
// For example: `(int)1242` or `(blob)(int)1337`.
// Currently such casts are very limited. Casting using this syntax is only allowed when the
// binary representation doesn't change during the cast. This means that it's legal to cast
// an int to blob (the bytes don't change), but it's illegal to cast an int to bigint (4 bytes -> 8 bytes).
// This limitation simplifies things - we can just reinterpret the value as the destination type.
static
std::optional<expression>
c_cast_prepare_expression(const cast& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver) {
    data_type cast_type = cast_get_prepared_type(c, db, keyspace);

    if (!receiver) {
        sstring receiver_name = format("cast({}){:user}", cast_type->cql3_type_name(), c.arg);
        receiver = make_lw_shared<column_specification>(
            keyspace, "unknown_cf", ::make_shared<column_identifier>(receiver_name, true), cast_type);
    }

    // casted_spec_of creates a receiver with type equal to c.type
    lw_shared_ptr<column_specification> cast_type_receiver = casted_spec_of(c, db, keyspace, *receiver);

    // First check if the casted expression can be assigned(converted) to the type specified in the cast.
    // test_assignment uses is_value_compatible_with to check if the binary representation is compatible
    // between the two types.
    if (!is_assignable(test_assignment(c.arg, db, keyspace, schema_opt, *cast_type_receiver))) {
        throw exceptions::invalid_request_exception(format("Cannot cast value {:user} to type {}", c.arg, cast_type->as_cql3_type()));
    }

    // Then check if a value of type c.type can be assigned(converted) to the receiver type.
    // cast_test_assignment also uses is_value_compatible_with to check binary representation compatibility.
    if (!is_assignable(cast_test_assignment(c, db, keyspace, schema_opt, *receiver))) {
        throw exceptions::invalid_request_exception(format("Cannot assign value {:user} to {} of type {}", c, receiver->name, receiver->type->as_cql3_type()));
    }

    // Now we know that c.arg is compatible with c.type, and c.type is compatible with receiver->type.
    // This means that the cast is correct - we can take the binary representation of c.arg value and
    // reinterpret it as a value of type receiver->type without any problems.

    // Prepare the argument using cast_type_receiver.
    // Using this receiver makes it possible to write things like: (blob)(int)1234
    // Using the original receiver wouldn't work in such cases - it would complain
    // that untyped_constant(1234) isn't a valid blob constant.
    return cast{
        .style = cast::cast_style::c,
        .arg = prepare_expression(c.arg, db, keyspace, schema_opt, cast_type_receiver),
        .type = receiver->type,
    };
}

static
std::optional<expression>
sql_cast_prepare_expression(const cast& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver) {
    data_type cast_type = cast_get_prepared_type(c, db, keyspace);

    if (!receiver) {
        sstring receiver_name = format("cast({}){:user}", cast_type->cql3_type_name(), c.arg);
        receiver = make_lw_shared<column_specification>(
            keyspace, "unknown_cf", ::make_shared<column_identifier>(receiver_name, true), cast_type);
    }

    auto prepared_arg = try_prepare_expression(c.arg, db, keyspace, schema_opt, nullptr);
    if (!prepared_arg) {
        throw exceptions::invalid_request_exception(fmt::format("Could not infer type of cast argument {}", c.arg));
    }

    // cast to the same type should be omitted
    if (cast_type == type_of(*prepared_arg)) {
        return prepared_arg;
    }

    // This will throw if a cast is impossible
    auto fun = functions::get_castas_fctn_as_cql3_function(cast_type, type_of(*prepared_arg));

    // We implement the cast to a function_call.
    return function_call{
        .func = std::move(fun),
        .args = std::vector({*prepared_arg}),
    };
}

std::optional<expression>
cast_prepare_expression(const cast& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver) {
    switch (c.style) {
    case cast::cast_style::c:
        return c_cast_prepare_expression(c, db, keyspace, schema_opt, std::move(receiver));
    case cast::cast_style::sql:
        return sql_cast_prepare_expression(c, db, keyspace, schema_opt, std::move(receiver));
    }
    on_internal_error(expr_logger, "Illegal cast style");
}

std::optional<expression>
field_selection_prepare_expression(const field_selection& fs, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver) {
    // We can't infer the type of the user defined type from the field being selected
    auto prepared_structure = try_prepare_expression(fs.structure, db, keyspace, schema_opt, nullptr);
    if (!prepared_structure) {
        throw exceptions::invalid_request_exception(fmt::format("Cannot infer type of {}", fs.structure));
    }
    auto type = type_of(*prepared_structure);
    if (!type->underlying_type()->is_user_type()) {
        throw exceptions::invalid_request_exception(
                format("Invalid field selection: {} of type {} is not a user type", fs.structure, type->as_cql3_type()));
    }

    auto ut = static_pointer_cast<const user_type_impl>(type->underlying_type());
    // FIXME: this check is artificial: prepare() below requires a schema even though one isn't
    // necessary for to prepare a field name. Luckily we'll always have a schema here, since there's
    // no way to get a user-defined type value other than by reading it from a table.
    if (!schema_opt) {
        throw exceptions::invalid_request_exception(fmt::format("Unable to prepare {} without schema", *fs.field));
    }
    auto prepared_field = fs.field->prepare(*schema_opt);
    auto idx = ut->idx_of_field(prepared_field->bytes_);
    if (!idx) {
        throw exceptions::invalid_request_exception(format("{} of type {} has no field {}",
                                                           fs.structure, ut->as_cql3_type(), fs.field));
    }
    return field_selection{
        .structure = std::move(*prepared_structure),
        .field = fs.field,  
        .field_idx = *idx,
        .type = ut->type(*idx),
    };
}

assignment_testable::test_result
field_selection_test_assignment(const field_selection& fs, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    // We can't infer the type of the user defined type from the field being selected
    auto prepared_structure = try_prepare_expression(fs.structure, db, keyspace, schema_opt, nullptr);
    if (!prepared_structure) {
        throw exceptions::invalid_request_exception(fmt::format("Cannot infer type of {}", fs.structure));
    }
    auto type = type_of(*prepared_structure);
    if (!type->underlying_type()->is_user_type()) {
        throw exceptions::invalid_request_exception(
                format("Invalid field selection: {} of type {} is not a user type", fs.structure, type->as_cql3_type()));
    }
    auto ut = static_pointer_cast<const user_type_impl>(type->underlying_type());
    // FIXME: this check is artificial: prepare() below requires a schema even though one isn't
    // necessary for to prepare a field name. Luckily we'll always have a schema here, since there's
    // no way to get a user-defined type value other than by reading it from a table.
    if (!schema_opt) {
        throw exceptions::invalid_request_exception(fmt::format("Unable to prepare {} without schema", *fs.field));
    }
    auto prepared_field = fs.field->prepare(*schema_opt);
    auto idx = ut->idx_of_field(prepared_field->bytes_);
    if (!idx) {
        throw exceptions::invalid_request_exception(format("{} of type {} has no field {}",
                                                           fs.structure, ut->as_cql3_type(), fs.field));
    }
    auto field_type = ut->type(*idx);
    return expression_test_assignment(field_type, receiver);
}

static
std::vector<::shared_ptr<assignment_testable>>
prepare_function_args_for_type_inference(std::span<const expression> args, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt) {
    // Prepare the arguments that can be prepared without a receiver.
    // Prepared expressions have a known type, which helps with finding the right function.
    std::vector<shared_ptr<assignment_testable>> partially_prepared_args;
    for (const expression& argument : args) {
        std::optional<expression> prepared_arg_opt = try_prepare_expression(argument, db, keyspace, schema_opt, nullptr);
        auto type = prepared_arg_opt ? std::optional(type_of(*prepared_arg_opt)) : std::nullopt;
        auto expr = prepared_arg_opt ? std::move(*prepared_arg_opt) : argument;
        partially_prepared_args.emplace_back(as_assignment_testable(std::move(expr), std::move(type)));
    }
    return partially_prepared_args;
}

std::optional<expression>
prepare_function_call(const expr::function_call& fc, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver) {
    // Try to extract a column family name from the available information.
    // Most functions can be prepared without information about the column family, usually just the keyspace is enough.
    // One exception is the token() function - in order to prepare system.token() we have to know the partition key of the table,
    // which can only be known when the column family is known.
    // In cases when someone calls prepare_function_call on a token() function without a known column_family, an exception is thrown by functions::get.
    std::optional<std::string_view> cf_name;
    if (schema_opt != nullptr) {
        cf_name = std::string_view(schema_opt->cf_name());
    } else if (receiver.get() != nullptr) {
        cf_name = receiver->cf_name;
    }

    // Prepare the arguments that can be prepared without a receiver.
    // Prepared expressions have a known type, which helps with finding the right function.
    auto partially_prepared_args = prepare_function_args_for_type_inference(fc.args, db, keyspace, schema_opt);

    auto&& fun = std::visit(overloaded_functor{
        [] (const shared_ptr<functions::function>& func) {
            return func;
        },
        [&] (const functions::function_name& name) {
            auto fun = functions::instance().get(db, keyspace, name, partially_prepared_args, keyspace, cf_name, receiver.get());
            if (!fun) {
                throw exceptions::invalid_request_exception(format("Unknown function {} called", name));
            }
            return fun;
        },
    }, fc.func);

    // Functions.get() will complain if no function "name" type check with the provided arguments.
    // We still have to validate that the return type matches however
    if (receiver && !receiver->type->is_value_compatible_with(*fun->return_type())) {
        throw exceptions::invalid_request_exception(format("Type error: cannot assign result of function {} (type {}) to {} (type {})",
                                                    fun->name(), fun->return_type()->as_cql3_type(),
                                                    receiver->name, receiver->type->as_cql3_type()));
    }

    if (fun->arg_types().size() != fc.args.size()) {
        throw exceptions::invalid_request_exception(format("Incorrect number of arguments specified for function {} (expected {:d}, found {:d})",
                                                    fun->name(), fun->arg_types().size(), fc.args.size()));
    }

    std::vector<expr::expression> parameters;
    parameters.reserve(partially_prepared_args.size());
    bool all_terminal = true;
    for (size_t i = 0; i < partially_prepared_args.size(); ++i) {
        expr::expression e = prepare_expression(fc.args[i], db, keyspace, schema_opt,
                                                functions::instance().make_arg_spec(keyspace, cf_name, *fun, i));
        if (!expr::is<expr::constant>(e)) {
            all_terminal = false;
        }
        parameters.push_back(std::move(e));
    }

    // If all parameters are terminal and the function is pure and scalar, we can
    // evaluate it now, otherwise we'd have to wait execution time
    expr::function_call fun_call {
        .func = fun,
        .args = std::move(parameters),
        .lwt_cache_id = fc.lwt_cache_id
    };
    if (all_terminal && fun->is_pure() && !fun->is_aggregate() && !fun->requires_thread()) {
        return constant(expr::evaluate(fun_call, query_options::DEFAULT), fun->return_type());
    } else {
        return fun_call;
    }
}

assignment_testable::test_result
test_assignment_function_call(const cql3::expr::function_call& fc, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    // Note: Functions.get() will return null if the function doesn't exist, or throw is no function matching
    // the arguments can be found. We may get one of those if an undefined/wrong function is used as argument
    // of another, existing, function. In that case, we return true here because we'll throw a proper exception
    // later with a more helpful error message that if we were to return false here.
    try {
        auto&& fun = std::visit(overloaded_functor{
            [&] (const functions::function_name& name) {
                auto args = prepare_function_args_for_type_inference(fc.args, db, keyspace, schema_opt);
                return functions::instance().get(db, keyspace, name, args, receiver.ks_name, receiver.cf_name, &receiver);
            },
            [] (const shared_ptr<functions::function>& func) {
                return func;
            },
        }, fc.func);
        if (fun && receiver.type == fun->return_type()) {
            return assignment_testable::test_result::EXACT_MATCH;
        } else if (!fun || receiver.type->is_value_compatible_with(*fun->return_type())) {
            return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
        } else {
            return assignment_testable::test_result::NOT_ASSIGNABLE;
        }
    } catch (exceptions::invalid_request_exception& e) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }
}

static assignment_testable::test_result expression_test_assignment(const data_type& expr_type,
                                                                   const column_specification& receiver) {
    if (receiver.type->underlying_type() == expr_type->underlying_type() || (receiver.type == long_type && expr_type->is_counter())) {
        return assignment_testable::test_result::EXACT_MATCH;
    } else if (receiver.type->is_value_compatible_with(*expr_type)) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    } else {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
}

std::optional<expression> prepare_conjunction(const conjunction& conj,
                                              data_dictionary::database db,
                                              const sstring& keyspace,
                                              const schema* schema_opt,
                                              lw_shared_ptr<column_specification> receiver) {
    if (receiver.get() != nullptr && receiver->type->without_reversed().get_kind() != abstract_type::kind::boolean) {
        throw exceptions::invalid_request_exception(
            format("AND conjunction produces a boolean value, which doesn't match the type: {} of {}",
                   receiver->type->name(), receiver->name->text()));
    }

    lw_shared_ptr<column_specification> child_receiver;
    if (receiver.get() != nullptr) {
        ::shared_ptr<column_identifier> child_receiver_name =
            ::make_shared<column_identifier>(format("AND_element({})", receiver->name->text()), true);
        child_receiver = make_lw_shared<column_specification>(receiver->ks_name, receiver->cf_name,
                                                              std::move(child_receiver_name), boolean_type);
    } else {
        ::shared_ptr<column_identifier> child_receiver_name =
            ::make_shared<column_identifier>("AND_element(unknown)", true);
        sstring cf_name = schema_opt ? schema_opt->cf_name() : "unknown_cf";
        child_receiver = make_lw_shared<column_specification>(keyspace, std::move(cf_name),
                                                              std::move(child_receiver_name), boolean_type);
    }

    std::vector<expression> prepared_children;

    bool all_terminal = true;
    for (const expression& child : conj.children) {
        std::optional<expression> prepared_child =
            try_prepare_expression(child, db, keyspace, schema_opt, child_receiver);
        if (!prepared_child.has_value()) {
            throw exceptions::invalid_request_exception(fmt::format("Could not infer type of {}", child));
        }
        if (!is<constant>(*prepared_child)) {
            all_terminal = false;
        }
        prepared_children.push_back(std::move(*prepared_child));
    }

    conjunction result = conjunction{std::move(prepared_children)};
    if (all_terminal) {
        return constant(evaluate(result, evaluation_inputs{}), boolean_type);
    }
    return result;
}

static
std::optional<expression>
prepare_column_mutation_attribute(
        const column_mutation_attribute& cma,
        data_dictionary::database db,
        const sstring& keyspace,
        const schema* schema_opt,
        lw_shared_ptr<column_specification> receiver) {
    auto result_type = expr::column_mutation_attribute_type(cma);
    if (receiver.get() != nullptr && receiver->type->without_reversed().get_kind() != result_type->get_kind()) {
        throw exceptions::invalid_request_exception(
            format("A {} produces a {} value, which doesn't match the type: {} of {}",
                    cma.kind, result_type->name(),
                    receiver->type->name(), receiver->name->text()));
    }
    auto column = prepare_expression(cma.column, db, keyspace, schema_opt, nullptr);
    auto cval = expr::as_if<column_value>(&column);
    if (!cval) {
        throw exceptions::invalid_request_exception(fmt::format("{} expects a column, but {} is a general expression", cma.kind, column));
    }
    if (!cval->col->is_atomic()) {
        throw exceptions::invalid_request_exception(fmt::format("{} expects an atomic column, but {} is a non-frozen collection", cma.kind, column));
    }
    if (cval->col->is_primary_key()) {
        throw exceptions::invalid_request_exception(fmt::format("{} is not legal on partition key component {}", cma.kind, column));
    }
    return column_mutation_attribute{
        .kind = cma.kind,
        .column = std::move(column),
    };
}

std::optional<expression>
try_prepare_expression(const expression& expr, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver) {
    return expr::visit(overloaded_functor{
        [&] (const constant& value) -> std::optional<expression> {
            if (receiver && !is_assignable(expression_test_assignment(value.type, *receiver))) {
                throw exceptions::invalid_request_exception(
                    format("cannot assign a constant {:user} of type {} to receiver {} of type {}", value,
                           value.type->as_cql3_type(), receiver->name, receiver->type->as_cql3_type()));
            }

            constant result = value;
            if (receiver) {
                // The receiver might have a different type from the constant, but this is allowed if the types are compatible.
                // In such case the type is implicitly converted to receiver type.
                result.type = receiver->type;
            }
            return result;
        },
        [&] (const binary_operator& binop) -> std::optional<expression> {
            if (receiver.get() != nullptr && &receiver->type->without_reversed() != boolean_type.get()) {
                throw exceptions::invalid_request_exception(
                    format("binary operator produces a boolean value, which doesn't match the type: {} of {}",
                           receiver->type->name(), receiver->name->text()));
            }

            binary_operator result = prepare_binary_operator(binop, db, *schema_opt);

            // A binary operator where both sides of the equation are known can be evaluated to a boolean value.
            // This only applies to operators in the CQL order, operations in the clustering order should only be
            // of form (clustering_column1, colustering_column2) < SCYLLA_CLUSTERING_BOUND(1, 2).
            if (is<constant>(result.lhs) && is<constant>(result.rhs) && result.order == comparison_order::cql) {
                return constant(evaluate(result, query_options::DEFAULT), boolean_type);
            }
            return result;
        },
        [&] (const conjunction& conj) -> std::optional<expression> {
            return prepare_conjunction(conj, db, keyspace, schema_opt, receiver);
        },
        [] (const column_value& cv) -> std::optional<expression> {
            return cv;
        },
        [&] (const subscript& sub) -> std::optional<expression> {
            if (!schema_opt) {
                throw exceptions::invalid_request_exception("cannot process subscript operation without schema");
            }
            auto& schema = *schema_opt;

            auto sub_col_opt = try_prepare_expression(sub.val, db, keyspace, schema_opt, receiver);
            if (!sub_col_opt) {
                return std::nullopt;
            }
            auto& sub_col = *sub_col_opt;
            const abstract_type& sub_col_type = type_of(sub_col)->without_reversed();

            auto col_spec = column_specification_of(sub_col);
            lw_shared_ptr<column_specification> subscript_column_spec;
            if (sub_col_type.is_map()) {
                subscript_column_spec = map_key_spec_of(*col_spec);
            } else if (sub_col_type.is_list()) {
                subscript_column_spec = list_key_spec_of(*col_spec);
            } else {
                throw exceptions::invalid_request_exception(format("Column {} is not a map/list, cannot be subscripted", col_spec->name->text()));
            }

            return subscript {
                .val = sub_col,
                .sub = prepare_expression(sub.sub, db, schema.ks_name(), &schema, std::move(subscript_column_spec)),
                .type = static_cast<const collection_type_impl&>(sub_col_type).value_comparator(),
            };
        },
        [&] (const unresolved_identifier& unin) -> std::optional<expression> {
            if (!schema_opt) {
                throw exceptions::invalid_request_exception(fmt::format("Cannot resolve column {} without schema", unin.ident->to_cql_string()));
            }
            return resolve_column(unin, *schema_opt);
        },
        [&] (const column_mutation_attribute& cma) -> std::optional<expression> {
            return prepare_column_mutation_attribute(cma, db, keyspace, schema_opt, std::move(receiver));
        },
        [&] (const function_call& fc) -> std::optional<expression> {
            return prepare_function_call(fc, db, keyspace, schema_opt, std::move(receiver));
        },
        [&] (const cast& c) -> std::optional<expression> {
            return cast_prepare_expression(c, db, keyspace, schema_opt, receiver);
        },
        [&] (const field_selection& fs) -> std::optional<expression> {
            return field_selection_prepare_expression(fs, db, keyspace, schema_opt, receiver);
        },
        [&] (const bind_variable& bv) -> std::optional<expression> {
            return bind_variable_prepare_expression(bv, db, keyspace, receiver);
        },
        [&] (const untyped_constant& uc) -> std::optional<expression> {
            return untyped_constant_prepare_expression(uc, db, keyspace, receiver);
        },
        [&] (const tuple_constructor& tc) -> std::optional<expression> {
            return tuple_constructor_prepare_nontuple(tc, db, keyspace, schema_opt, receiver);
        },
        [&] (const collection_constructor& c) -> std::optional<expression> {
            switch (c.style) {
            case collection_constructor::style_type::list: return list_prepare_expression(c, db, keyspace, schema_opt, receiver);
            case collection_constructor::style_type::set: return set_prepare_expression(c, db, keyspace, schema_opt, receiver);
            case collection_constructor::style_type::map: return map_prepare_expression(c, db, keyspace, schema_opt, receiver);
            }
            on_internal_error(expr_logger, fmt::format("unexpected collection_constructor style {}", static_cast<unsigned>(c.style)));
        },
        [&] (const usertype_constructor& uc) -> std::optional<expression> {
            return usertype_constructor_prepare_expression(uc, db, keyspace, schema_opt, receiver);
        },
        [&] (const temporary& t) -> std::optional<expression> {
            on_internal_error(expr_logger, "temporary found during prepare, should have been introduced post-prepare");
        },
    }, expr);
}

static
assignment_testable::test_result
unresolved_identifier_test_assignment(const unresolved_identifier& ui, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    auto prepared = prepare_expression(ui, db, keyspace, schema_opt, make_lw_shared<column_specification>(receiver));
    return test_assignment(prepared, db, keyspace, schema_opt, receiver);
}

static
assignment_testable::test_result
column_mutation_attribute_test_assignment(const column_mutation_attribute& cma, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    auto type = column_mutation_attribute_type(cma);
    return expression_test_assignment(std::move(type), std::move(receiver));
}

assignment_testable::test_result
test_assignment(const expression& expr, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    using test_result = assignment_testable::test_result;
    return expr::visit(overloaded_functor{
        [&] (const constant& value) -> test_result {
            return expression_test_assignment(value.type, receiver);
        },
        [&] (const binary_operator&) -> test_result {
            on_internal_error(expr_logger, "binary_operators are not yet reachable via test_assignment()");
        },
        [&] (const conjunction&) -> test_result {
            on_internal_error(expr_logger, "conjunctions are not yet reachable via test_assignment()");
        },
        [&] (const column_value& col_val) -> test_result {
            return expression_test_assignment(col_val.col->type, receiver);
        },
        [&] (const subscript&) -> test_result {
            on_internal_error(expr_logger, "subscripts are not yet reachable via test_assignment()");
        },
        [&] (const unresolved_identifier& ui) -> test_result {
            return unresolved_identifier_test_assignment(ui, db, keyspace, schema_opt, receiver);
        },
        [&] (const column_mutation_attribute& cma) -> test_result {
            return column_mutation_attribute_test_assignment(cma, db, keyspace, schema_opt, receiver);
        },
        [&] (const function_call& fc) -> test_result {
            return test_assignment_function_call(fc, db, keyspace, schema_opt, receiver);
        },
        [&] (const cast& c) -> test_result {
            return cast_test_assignment(c, db, keyspace, schema_opt, receiver);
        },
        [&] (const field_selection& fs) -> test_result {
            return field_selection_test_assignment(fs, db, keyspace, schema_opt, receiver);
        },
        [&] (const bind_variable& bv) -> test_result {
            return bind_variable_test_assignment(bv, db, keyspace, receiver);
        },
        [&] (const untyped_constant& uc) -> test_result {
            return untyped_constant_test_assignment(uc, db, keyspace, receiver);
        },
        [&] (const tuple_constructor& tc) -> test_result {
            return tuple_constructor_test_assignment(tc, db, keyspace, schema_opt, receiver);
        },
        [&] (const collection_constructor& c) -> test_result {
            switch (c.style) {
            case collection_constructor::style_type::list: return list_test_assignment(c, db, keyspace, schema_opt, receiver);
            case collection_constructor::style_type::set: return set_test_assignment(c, db, keyspace, schema_opt, receiver);
            case collection_constructor::style_type::map: return map_test_assignment(c, db, keyspace, schema_opt, receiver);
            }
            on_internal_error(expr_logger, fmt::format("unexpected collection_constructor style {}", static_cast<unsigned>(c.style)));
        },
        [&] (const usertype_constructor& uc) -> test_result {
            return usertype_constructor_test_assignment(uc, db, keyspace, schema_opt, receiver);
        },
        [&] (const temporary& t) -> test_result {
            on_internal_error(expr_logger, "temporary found in test_assignment, should have been introduced post-prepare");
        },
    }, expr);
}

expression
prepare_expression(const expression& expr, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver) {
    auto e_opt = try_prepare_expression(expr, db, keyspace, schema_opt, std::move(receiver));
    if (!e_opt) {
        throw exceptions::invalid_request_exception(fmt::format("Could not infer type of {}", expr));
    }
    return std::move(*e_opt);
}

assignment_testable::test_result
test_assignment_all(const std::vector<expression>& to_test, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    using test_result = assignment_testable::test_result;
    test_result res = test_result::EXACT_MATCH;
    for (auto&& e : to_test) {
        test_result t = test_assignment(e, db, keyspace, schema_opt, receiver);
        if (t == test_result::NOT_ASSIGNABLE) {
            return test_result::NOT_ASSIGNABLE;
        }
        if (t == test_result::WEAKLY_ASSIGNABLE) {
            res = test_result::WEAKLY_ASSIGNABLE;
        }
    }
    return res;
}

class assignment_testable_expression : public assignment_testable {
    expression _e;
    std::optional<data_type> _type_opt;
public:
    explicit assignment_testable_expression(expression e, std::optional<data_type> type_opt) : _e(std::move(e)), _type_opt(std::move(type_opt)) {}
    virtual test_result test_assignment(data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) const override {
        return expr::test_assignment(_e, db, keyspace, schema_opt, receiver);
    }
    virtual sstring assignment_testable_source_context() const override {
        return fmt::format("{}", _e);
    }
    virtual std::optional<data_type> assignment_testable_type_opt() const override {
        return _type_opt;
    }
};

::shared_ptr<assignment_testable> as_assignment_testable(expression e, std::optional<data_type> type_opt) {
    return ::make_shared<assignment_testable_expression>(std::move(e), std::move(type_opt));
}

// Finds column_defintion for given column name in the schema.
static const column_value resolve_column(const unresolved_identifier& col_ident, const schema& schema) {
    ::shared_ptr<column_identifier> id = col_ident.ident->prepare_column_identifier(schema);
    const column_definition* def = get_column_definition(schema, *id);
    if (!def || def->is_hidden_from_cql()) {
        throw exceptions::unrecognized_entity_exception(*id);
    }
    return column_value(def);
}

// Finds the type of a prepared LHS of binary_operator and creates a receiver with it.
static lw_shared_ptr<column_specification> get_lhs_receiver(const expression& prepared_lhs, const schema& schema) {
    return expr::visit(overloaded_functor{
        [](const column_value& col_val) -> lw_shared_ptr<column_specification> {
            return col_val.col->column_specification;
        },
        [](const subscript& col_val) -> lw_shared_ptr<column_specification> {
            const column_value& sub_col = get_subscripted_column(col_val);
            if (sub_col.col->type->is_map()) {
                return map_value_spec_of(*sub_col.col->column_specification);
            } else {
                return list_value_spec_of(*sub_col.col->column_specification);
            }
        },
        [&](const tuple_constructor& tup) -> lw_shared_ptr<column_specification> {
            std::ostringstream tuple_name;
            tuple_name << "(";
            std::vector<data_type> tuple_types;
            tuple_types.reserve(tup.elements.size());

            for (std::size_t i = 0; i < tup.elements.size(); i++) {
                lw_shared_ptr<column_specification> elem_receiver = get_lhs_receiver(tup.elements[i], schema);
                tuple_name << elem_receiver->name->text();
                if (i+1 != tup.elements.size()) {
                    tuple_name << ",";
                }

                tuple_types.push_back(elem_receiver->type->underlying_type());
            }

            tuple_name << ")";

            shared_ptr<column_identifier> identifier = ::make_shared<column_identifier>(tuple_name.str(), true);
            data_type tuple_type = tuple_type_impl::get_instance(tuple_types);
            return make_lw_shared<column_specification>(schema.ks_name(), schema.cf_name(), std::move(identifier), std::move(tuple_type));
        },
        [&](const function_call& fun_call) -> lw_shared_ptr<column_specification> {
            // In case of an expression like `token(p1, p2, p3) = ?` the receiver name should be "partition key token".
            // This is required for compatibality with the java driver, it breaks with a receiver name like "token(p1, p2, p3)".
            if (is_partition_token_for_schema(fun_call, schema)) {
                return make_lw_shared<column_specification>(
                    schema.ks_name(),
                    schema.cf_name(),
                    ::make_shared<column_identifier>("partition key token", true),
                    long_type);
            }

            data_type return_type = std::visit(
                    overloaded_functor{
                        [](const shared_ptr<db::functions::function>& fun) -> data_type { return fun->return_type(); },
                        [&](const functions::function_name&) -> data_type {
                            on_internal_error(expr_logger,
                                              format("get_lhs_receiver: unprepared function call {:debug}", fun_call));
                        }},
                    fun_call.func);

            return make_lw_shared<column_specification>(
                schema.ks_name(), schema.cf_name(),
                ::make_shared<column_identifier>(format("{:user}", fun_call), true),
                return_type);
        },
        [](const auto& other) -> lw_shared_ptr<column_specification> {
            on_internal_error(expr_logger, format("get_lhs_receiver: unexpected expression: {}", other));
        },
    }, prepared_lhs);
}

// Given type of LHS and the operation finds the expected type of RHS.
// The type will be the same as LHS for simple operations like =, but it will be different for more complex ones like IN or CONTAINS.
static lw_shared_ptr<column_specification> get_rhs_receiver(lw_shared_ptr<column_specification>& lhs_receiver, oper_t oper) {
    const data_type lhs_type = lhs_receiver->type->underlying_type();

    if (oper == oper_t::IN) {
        data_type rhs_receiver_type = list_type_impl::get_instance(std::move(lhs_type), false);
        auto in_name = ::make_shared<column_identifier>(format("in({})", lhs_receiver->name->text()), true);
        return make_lw_shared<column_specification>(lhs_receiver->ks_name,
                                                    lhs_receiver->cf_name,
                                                    in_name,
                                                    std::move(rhs_receiver_type));
    } else if (oper == oper_t::CONTAINS) {
        if (lhs_type->is_list()) {
            return list_value_spec_of(*lhs_receiver);
        } else if (lhs_type->is_set()) {
            return set_value_spec_of(*lhs_receiver);
        } else if (lhs_type->is_map()) {
            return map_value_spec_of(*lhs_receiver);
        } else {
            throw exceptions::invalid_request_exception(format("Cannot use CONTAINS on non-collection column \"{}\"",
                                                                     lhs_receiver->name));
        }
    } else if (oper == oper_t::CONTAINS_KEY) {
        if (lhs_type->is_map()) {
            return map_key_spec_of(*lhs_receiver);
        } else {
            throw exceptions::invalid_request_exception(format("Cannot use CONTAINS KEY on non-map column {}",
                                                               lhs_receiver->name));
        }
    } else if (oper == oper_t::LIKE) {
        if (!lhs_type->is_string()) {
            throw exceptions::invalid_request_exception(
                format("LIKE is allowed only on string types, which {} is not", lhs_receiver->name->text()));
        }
        return lhs_receiver;
    } else {
        return lhs_receiver;
    }
}

class like_constant_function : public cql3::functions::scalar_function {
    functions::function_name _name;
    like_matcher _matcher;
    std::vector<data_type> _lhs_types;
public:
    like_constant_function(data_type arg_type, bytes_view pattern)
            : _name("system", fmt::format("like({})",
                    std::string_view(reinterpret_cast<const char*>(pattern.data()), pattern.size())))
            , _matcher(pattern) {
        _lhs_types.push_back(std::move(arg_type));
    }

    virtual const functions::function_name& name() const override {
        return _name;
    }

    virtual const std::vector<data_type>& arg_types() const override {
        return _lhs_types;
    }

    virtual const data_type& return_type() const override {
        return boolean_type;
    }

    virtual bool is_pure() const override {
        return true;
    }

    virtual bool is_native() const override {
        return true;
    }

    virtual bool requires_thread() const override {
        return false;
    }

    virtual bool is_aggregate() const override {
        return false;
    }

    virtual void print(std::ostream& os) const override {
        os << "LIKE(compiled)";
    }

    virtual sstring column_name(const std::vector<sstring>& column_names) const override {
        return "LIKE";
    }

    virtual bytes_opt execute(std::span<const bytes_opt> parameters) override {
        auto& str_opt = parameters[0];
        if (!str_opt) {
            return std::nullopt;
        }
        bool match_result = _matcher(*str_opt);
        return data_value(match_result).serialize();
    }
};

expression
optimize_like(const expression& e) {
    // Check for LIKE with constant pattern; replace with anonymous 
    // function that contains the compiled regex.
    return search_and_replace(e, [] (const expression& subexpression) -> std::optional<expression> {
        if (auto* binop = as_if<binary_operator>(&subexpression)) {
            if (binop->op == oper_t::LIKE) {
                if (auto* rhs = as_if<constant>(&binop->rhs)) {
                    if ((type_of(*rhs) == utf8_type || type_of(*rhs) == ascii_type) && !rhs->is_null()) {
                        auto pattern = to_bytes(rhs->value.view());
                        auto func = ::make_shared<like_constant_function>(type_of(binop->lhs), pattern);
                        auto args = std::vector<expression>();
                        args.push_back(binop->lhs);
                        return function_call{std::move(func), std::move(args)};
                    }
                }
            }
        }
        return std::nullopt;
    });
}

binary_operator prepare_binary_operator(binary_operator binop, data_dictionary::database db, const schema& table_schema) {
    std::optional<expression> prepared_lhs_opt = try_prepare_expression(binop.lhs, db, table_schema.ks_name(), &table_schema, {});
    if (!prepared_lhs_opt) {
        throw exceptions::invalid_request_exception(fmt::format("Could not infer type of {}", binop.lhs));
    }
    auto& prepared_lhs = *prepared_lhs_opt;
    lw_shared_ptr<column_specification> lhs_receiver = get_lhs_receiver(prepared_lhs, table_schema);

    if (type_of(prepared_lhs)->references_duration() && is_slice(binop.op)) {
        throw exceptions::invalid_request_exception(fmt::format("Duration type is unordered for {}", lhs_receiver->name));
    }

    lw_shared_ptr<column_specification> rhs_receiver = get_rhs_receiver(lhs_receiver, binop.op);
    expression prepared_rhs = prepare_expression(binop.rhs, db, table_schema.ks_name(), &table_schema, rhs_receiver);

    // IS NOT NULL requires an additional check that the RHS is NULL.
    // Otherwise things like `int_col IS NOT 123` would be allowed - the types match, but the value is wrong.
    if (binop.op == oper_t::IS_NOT) {
        bool rhs_is_null = is<constant>(prepared_rhs) && as<constant>(prepared_rhs).is_null();

        if (!rhs_is_null) {
            throw exceptions::invalid_request_exception(format(
                "IS NOT NULL is the only expression that is allowed when using IS NOT. Invalid binary operator: {:user}",
                binop));
        }
    }

    return binary_operator(std::move(prepared_lhs), binop.op, std::move(prepared_rhs), binop.order);
}

}

namespace cql3 {

lw_shared_ptr<column_specification>
lists::value_spec_of(const column_specification& column) {
    return cql3::expr::list_value_spec_of(column);
}

lw_shared_ptr<column_specification>
maps::key_spec_of(const column_specification& column) {
    return cql3::expr::map_key_spec_of(column);
}

lw_shared_ptr<column_specification>
maps::value_spec_of(const column_specification& column) {
    return cql3::expr::map_value_spec_of(column);
}

lw_shared_ptr<column_specification>
sets::value_spec_of(const column_specification& column) {
    return cql3::expr::set_value_spec_of(column);
}

lw_shared_ptr<column_specification>
user_types::field_spec_of(const column_specification& column, size_t field) {
    return cql3::expr::usertype_field_spec_of(column, field);
}

}
