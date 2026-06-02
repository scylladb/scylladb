/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "build_mode.hh"
#include "expression.hh"
#include "expr-utils.hh"
#include "evaluate.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/aggregate_fcts.hh"
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
#include "types/vector.hh"
#include "types/user.hh"
#include "exceptions/unrecognized_entity_exception.hh"
#include "utils/like_matcher.hh"
#include "utils/chunked_string.hh"
#include "utils/hash.hh"

#include <ranges>

namespace cql3::expr {

static const column_value resolve_column(const unresolved_identifier& col_ident, const schema& schema);

static assignment_testable::test_result expression_test_assignment(const data_type& expr_type,
                                                                   const column_specification& receiver);

static std::optional<data_type> try_widen(const data_type& a, const data_type& b);
static expression coerce_to(expression e, const data_type& target, data_dictionary::database db, const sstring& keyspace);

static bool is_widenable_to(const data_type& from, const data_type& to);

// Memoization that is active only for the duration of a single top-level prepare.
//
// Resolving an unresolved nested function call against a candidate parameter type
// is recursive. When a multi-overload function has an argument that is itself an
// unresolved hole (e.g. an ambiguous nested call), overload resolution probes that
// hole once per candidate parameter type, at every nesting level - which is
// exponential in the nesting depth. Caching the result of such a probe for a given
// (call, receiver type) collapses that back to linear.
//
// A prepare_memo is created on the stack by the public prepare entry points and
// threaded by reference through the recursive prepare; it is freed when that entry
// point returns, so nothing is retained between independent prepare calls. Being a
// parameter rather than a global, its lifetime is exactly the prepare it belongs to,
// with no dependence on preparation staying synchronous.
//
// keyspace/schema/cf are constant within a prepare, so the key is just (call, receiver type).
struct call_probe_key {
    function_call call;
    data_type receiver_type;
    bool operator==(const call_probe_key&) const = default;
};

// Shallow hash: (qualified name, arity, receiver type) only - args aren't hashed; operator== separates them.
struct call_probe_key_hash {
    size_t operator()(const call_probe_key& k) const {
        const auto fn = std::visit(overloaded_functor{
            [] (const functions::function_name& name) { return name; },
            [] (const shared_ptr<functions::function>& f) { return f->name(); },
        }, k.call.func);
        size_t h = std::hash<functions::function_name>{}(fn);
        h = utils::hash_combine(h, k.call.args.size());
        h = utils::hash_combine(h, std::hash<sstring>{}(k.receiver_type->name()));
        return h;
    }
};

struct prepare_memo {
    // Test-only: when false, probes are neither looked up nor stored, so a test can
    // observe the un-memoized (exponential) probing cost for comparison. The constructor
    // seeds it from the test switch (always true in release).
    bool enabled = true;
    std::unordered_map<call_probe_key, assignment_testable::test_result, call_probe_key_hash> test_assignment_function_call;

    prepare_memo();
};

// Memo-threaded overloads of the public entry points. The public (memo-less) functions
// create a prepare_memo on the stack and delegate here; the recursive prepare passes the
// same memo by reference, so it is never global.
static std::optional<expression> try_prepare_expression(const expression& expr, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver, bool infer_default, prepare_memo& memo);
static assignment_testable::test_result test_assignment(const expression& expr, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo);
static expression prepare_expression(const expression& expr, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver, prepare_memo& memo);
static assignment_testable::test_result test_assignment_all(const std::vector<expression>& to_test, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo);
static ::shared_ptr<assignment_testable> as_assignment_testable(expression e, std::optional<data_type> type_opt, prepare_memo& memo);

struct inferred_elements {
    data_type element_type;
    std::vector<expression> prepared;
};

template <typename Project>
static std::optional<inferred_elements>
prepare_and_infer_collection_elements(std::span<const expression> elements,
        data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, Project&& project, prepare_memo& memo) {
    std::optional<data_type> result;
    std::vector<expression> prepared;
    prepared.reserve(elements.size());
    for (const expression& e : elements) {
        std::optional<expression> p = try_prepare_expression(project(e), db, keyspace, schema_opt, nullptr, /*infer_default=*/true, memo);
        if (!p) {
            return std::nullopt;
        }
        data_type t = type_of(*p);
        if (!result) {
            result = t;
        } else if (**result != *t) {
            auto widened = try_widen(*result, t);
            if (!widened) {
                return std::nullopt;
            }
            result = std::move(widened);
        }
        prepared.push_back(std::move(*p));
    }
    if (!result) {
        return std::nullopt;
    }
    return inferred_elements{std::move(*result), std::move(prepared)};
}

// Build a collection literal from elements that were already prepared (by
// prepare_and_infer_collection_elements), coercing each to the collection's element type
// rather than preparing it again. Folds to a terminal constant when every element is.
static expression
build_collection_from_prepared(collection_constructor::style_type style, data_type collection_type,
        data_type element_type, std::vector<expression> prepared,
        data_dictionary::database db, const sstring& keyspace) {
    std::vector<expression> values;
    values.reserve(prepared.size());
    bool all_terminal = true;
    for (auto& p : prepared) {
        expression elem = coerce_to(std::move(p), element_type, db, keyspace);
        if (!is<constant>(elem)) {
            all_terminal = false;
        }
        values.push_back(std::move(elem));
    }
    collection_constructor value {
        .style = style,
        .elements = std::move(values),
        .type = std::move(collection_type),
    };
    if (all_terminal) {
        return constant(evaluate(value, query_options::DEFAULT), value.type);
    }
    return value;
}


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
usertype_constructor_validate_assignable_to(const usertype_constructor& u, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo) {
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
        if (!assignment_testable::is_assignable(test_assignment(value, db, keyspace, schema_opt, *field_spec, memo))) {
            throw exceptions::invalid_request_exception(format("Invalid user type literal for {}: field {} is not of type {}", *receiver.name, field, field_spec->type->as_cql3_type()));
        }
    }
}

static
assignment_testable::test_result
usertype_constructor_test_assignment(const usertype_constructor& u, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo) {
    try {
        usertype_constructor_validate_assignable_to(u, db, keyspace, schema_opt, receiver, memo);
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    } catch (exceptions::invalid_request_exception& e) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
}

static
std::optional<expression>
usertype_constructor_prepare_expression(const usertype_constructor& u, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver, prepare_memo& memo) {
    if (!receiver) {
        return std::nullopt; // cannot infer type from {field: value}
    }
    usertype_constructor_validate_assignable_to(u, db, keyspace, schema_opt, *receiver, memo);
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
        expression value = prepare_expression(raw, db, keyspace, schema_opt, usertype_field_spec_of(*receiver, i), memo);

        if (!is<constant>(value)) {
            all_terminal = false;
        }

        prepared_elements.emplace(std::move(field), std::move(value));
    }
    if (found_values != u.elements.size()) {
        // We had some field that are not part of the type
        for (auto&& id_val : u.elements) {
            auto&& id = id_val.first;
            if (!std::ranges::contains(ut->field_names(), id.bytes_)) {
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
map_validate_assignable_to(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo) {
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
        if (!is_assignable(test_assignment(entry_tuple.elements[0], db, keyspace, schema_opt, *key_spec, memo))) {
            throw exceptions::invalid_request_exception(format("Invalid map literal for {}: key {} is not of type {}", *receiver.name, entry_tuple.elements[0], key_spec->type->as_cql3_type()));
        }
        if (!is_assignable(test_assignment(entry_tuple.elements[1], db, keyspace, schema_opt, *value_spec, memo))) {
            throw exceptions::invalid_request_exception(format("Invalid map literal for {}: value {} is not of type {}", *receiver.name, entry_tuple.elements[1], value_spec->type->as_cql3_type()));
        }
    }
}

static
assignment_testable::test_result
map_test_assignment(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo) {
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
        auto t1 = test_assignment(entry_tuple.elements[0], db, keyspace, schema_opt, *key_spec, memo);
        auto t2 = test_assignment(entry_tuple.elements[1], db, keyspace, schema_opt, *value_spec, memo);
        if (t1 == assignment_testable::test_result::NOT_ASSIGNABLE || t2 == assignment_testable::test_result::NOT_ASSIGNABLE)
            return assignment_testable::test_result::NOT_ASSIGNABLE;
        if (t1 != assignment_testable::test_result::EXACT_MATCH || t2 != assignment_testable::test_result::EXACT_MATCH)
            res = assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }
    return res;
}

static
std::optional<expression>
map_prepare_expression(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver, bool infer_default, prepare_memo& memo) {
    if (!receiver) {
        if (!infer_default) {
            return std::nullopt;
        }
        auto key_of = [] (const expression& e) -> const expression& { return expr::as<tuple_constructor>(e).elements[0]; };
        auto value_of = [] (const expression& e) -> const expression& { return expr::as<tuple_constructor>(e).elements[1]; };
        auto keys = prepare_and_infer_collection_elements(c.elements, db, keyspace, schema_opt, key_of, memo);
        auto values = prepare_and_infer_collection_elements(c.elements, db, keyspace, schema_opt, value_of, memo);
        if (!keys || !values) {
            return std::nullopt;
        }
        data_type map_type = map_type_impl::get_instance(keys->element_type, values->element_type, false);
        data_type entry_tuple_type = tuple_type_impl::get_instance({keys->element_type, values->element_type});
        std::vector<expression> entries;
        entries.reserve(c.elements.size());
        bool all_terminal = true;
        for (size_t i = 0; i < c.elements.size(); ++i) {
            expression k = coerce_to(std::move(keys->prepared[i]), keys->element_type, db, keyspace);
            expression v = coerce_to(std::move(values->prepared[i]), values->element_type, db, keyspace);
            if (!is<constant>(k) || !is<constant>(v)) {
                all_terminal = false;
            }
            entries.emplace_back(tuple_constructor {
                .elements = {std::move(k), std::move(v)},
                .type = entry_tuple_type,
            });
        }
        collection_constructor map_value {
            .style = collection_constructor::style_type::map,
            .elements = std::move(entries),
            .type = map_type,
        };
        if (all_terminal) {
            return constant(evaluate(map_value, query_options::DEFAULT), map_value.type);
        }
        return map_value;
    }
    map_validate_assignable_to(c, db, keyspace, schema_opt, *receiver, memo);

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
        expression k = prepare_expression(entry_tuple.elements[0], db, keyspace, schema_opt, key_spec, memo);
        expression v = prepare_expression(entry_tuple.elements[1], db, keyspace, schema_opt, value_spec, memo);

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
set_validate_assignable_to(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo) {
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
        if (!is_assignable(test_assignment(e, db, keyspace, schema_opt, *value_spec, memo))) {
            throw exceptions::invalid_request_exception(format("Invalid set literal for {}: value {} is not of type {}", *receiver.name, e, value_spec->type->as_cql3_type()));
        }
    }
}

static
assignment_testable::test_result
set_test_assignment(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo) {
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
    return test_assignment_all(c.elements, db, keyspace, schema_opt, *value_spec, memo);
}

static
std::optional<expression>
set_prepare_expression(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver, bool infer_default, prepare_memo& memo) {
    if (!receiver) {
        if (!infer_default) {
            return std::nullopt;
        }
        auto identity = [] (const expression& e) -> const expression& { return e; };
        auto inferred = prepare_and_infer_collection_elements(c.elements, db, keyspace, schema_opt, identity, memo);
        if (!inferred) {
            return std::nullopt;
        }
        return build_collection_from_prepared(collection_constructor::style_type::set,
                set_type_impl::get_instance(inferred->element_type, false),
                inferred->element_type, std::move(inferred->prepared), db, keyspace);
    }
    set_validate_assignable_to(c, db, keyspace, schema_opt, *receiver, memo);

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
        expression elem = prepare_expression(e, db, keyspace, schema_opt, value_spec, memo);

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
list_validate_assignable_to(const collection_constructor& c, data_dictionary::database db, const sstring keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo) {
    if (!receiver.type->without_reversed().is_list()) {
        throw exceptions::invalid_request_exception(format("Invalid list literal for {} of type {}",
                *receiver.name, receiver.type->as_cql3_type()));
    }
    auto&& value_spec = list_value_spec_of(receiver);
    for (auto& e : c.elements) {
        if (!is_assignable(test_assignment(e, db, keyspace, schema_opt, *value_spec, memo))) {
            throw exceptions::invalid_request_exception(format("Invalid list literal for {}: value {} is not of type {}",
                    *receiver.name, e, value_spec->type->as_cql3_type()));
        }
    }
}

static
assignment_testable::test_result
list_test_assignment(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo) {
    // If there is no elements, we can't say it's an exact match (an empty list if fundamentally polymorphic).
    if (c.elements.empty()) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }

    auto&& value_spec = list_value_spec_of(receiver);
    return test_assignment_all(c.elements, db, keyspace, schema_opt, *value_spec, memo);
}


static
std::optional<expression>
list_prepare_expression(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver, prepare_memo& memo) {
    list_validate_assignable_to(c, db, keyspace, schema_opt, *receiver, memo);

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
        expression elem = prepare_expression(e, db, keyspace, schema_opt, value_spec, memo);

        if (!is<constant>(elem)) {
            all_terminal = false;
        }
        values.push_back(std::move(elem));
    }
    collection_constructor value {
        .style = collection_constructor::style_type::list_or_vector,
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
vector_value_spec_of(const column_specification& column) {
    return make_lw_shared<column_specification>(column.ks_name, column.cf_name,
            ::make_shared<column_identifier>(format("value({})", *column.name), true),
            dynamic_cast<const vector_type_impl&>(column.type->without_reversed()).get_elements_type());
}

static
void
vector_validate_assignable_to(const collection_constructor& c, data_dictionary::database db, const sstring keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo) {
    auto vt = dynamic_pointer_cast<const vector_type_impl>(receiver.type->underlying_type());
    if (!vt) {
        throw exceptions::invalid_request_exception(format("Invalid vector type literal for {} of type {}", *receiver.name, receiver.type->as_cql3_type()));
    }

    vector_dimension_t expected_size = vt->get_dimension();
    if (expected_size == 0) {
        throw exceptions::invalid_request_exception(format("Invalid vector type literal for {}: type {} expects at least one element",
                                                            *receiver.name, receiver.type->as_cql3_type()));
    }
    size_t received_size = c.elements.size();
    if (expected_size != received_size) {
        throw exceptions::invalid_request_exception(format("Invalid vector literal for {}: type {} expects {:d} elements but got {:d}",
                                                            *receiver.name, receiver.type->as_cql3_type(), expected_size, received_size));
    }

    auto&& value_spec = vector_value_spec_of(receiver);
    for (auto& e : c.elements) {
        if (!is_assignable(test_assignment(e, db, keyspace, schema_opt, *value_spec, memo))) {
            throw exceptions::invalid_request_exception(format("Invalid vector literal for {}: value {} is not of type {}",
                    *receiver.name, e, value_spec->type->as_cql3_type()));
        }
    }
}

static
assignment_testable::test_result
vector_test_assignment(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo) {
    // If there is no elements, we can't say it's an exact match (an empty vector if fundamentally polymorphic).
    if (c.elements.empty()) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }

    auto&& value_spec = vector_value_spec_of(receiver);
    return test_assignment_all(c.elements, db, keyspace, schema_opt, *value_spec, memo);
}

static
std::optional<expression>
vector_prepare_expression(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver, prepare_memo& memo) {
    vector_validate_assignable_to(c, db, keyspace, schema_opt, *receiver, memo);

    auto&& value_spec = vector_value_spec_of(*receiver);
    std::vector<expression> values;
    values.reserve(c.elements.size());
    bool all_terminal = true;
    for (auto& e : c.elements) {
        expression elem = prepare_expression(e, db, keyspace, schema_opt, value_spec, memo);

        if (!is<constant>(elem)) {
            all_terminal = false;
        }
        values.push_back(std::move(elem));
    }

    // Here we convert from list_or_vector to vector style, as we know that we received a vector type.
    collection_constructor value {
        .style = collection_constructor::style_type::vector,
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
assignment_testable::test_result
list_or_vector_test_assignment(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo) {
    if (dynamic_pointer_cast<const vector_type_impl>(receiver.type)) {
        return vector_test_assignment(c, db, keyspace, schema_opt, receiver, memo);
    } else if (dynamic_pointer_cast<const list_type_impl>(receiver.type)) {
        return list_test_assignment(c, db, keyspace, schema_opt, receiver, memo);
    } else {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
}

static
std::optional<expression>
list_or_vector_prepare_expression(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver, bool infer_default, prepare_memo& memo) {
    if (!receiver) {
        if (!infer_default) {
            return std::nullopt;
        }
        // With no receiver we cannot know a vector's dimension, so a bare list/vector
        // literal is inferred as a list, built directly from the elements prepared
        // during inference rather than preparing them a second time.
        auto identity = [] (const expression& e) -> const expression& { return e; };
        auto inferred = prepare_and_infer_collection_elements(c.elements, db, keyspace, schema_opt, identity, memo);
        if (!inferred) {
            return std::nullopt;
        }
        return build_collection_from_prepared(collection_constructor::style_type::list_or_vector,
                list_type_impl::get_instance(inferred->element_type, false),
                inferred->element_type, std::move(inferred->prepared), db, keyspace);
    }

    // We do not check if the receiver is a list because it is checked later in the list_prepare_expression.
    if (receiver->type->is_vector()) {
        return vector_prepare_expression(c, db, keyspace, schema_opt, receiver, memo);
    } else {
        return list_prepare_expression(c, db, keyspace, schema_opt, receiver, memo);
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
tuple_constructor_validate_assignable_to(const tuple_constructor& tc, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo) {
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
        if (!assignment_testable::is_assignable(test_assignment(value, db, keyspace, schema_opt, *spec, memo))) {
            throw exceptions::invalid_request_exception(format("Invalid tuple literal for {}: component {:d} is not of type {}", *receiver.name, i, spec->type->as_cql3_type()));
        }
    }
}

static
assignment_testable::test_result
tuple_constructor_test_assignment(const tuple_constructor& tc, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo) {
    try {
        tuple_constructor_validate_assignable_to(tc, db, keyspace, schema_opt, receiver, memo);
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    } catch (exceptions::invalid_request_exception& e) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
}

static
std::optional<expression>
tuple_constructor_prepare_nontuple(const tuple_constructor& tc, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver, bool infer_default, prepare_memo& memo) {
    if (receiver) {
        tuple_constructor_validate_assignable_to(tc, db, keyspace, schema_opt, *receiver, memo);
    }
    std::vector<expression> values;
    bool all_terminal = true;
    for (size_t i = 0; i < tc.elements.size(); ++i) {
        lw_shared_ptr<column_specification> component_receiver;
        if (receiver) {
            component_receiver = component_spec_of(*receiver, i);
        }
        std::optional<expression> value_opt = try_prepare_expression(tc.elements[i], db, keyspace, schema_opt, component_receiver, infer_default && !receiver, memo);
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
        type = tuple_type_impl::get_instance(
                values
                | std::views::transform(type_of)
                | std::ranges::to<std::vector>());
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
managed_bytes
untyped_constant_parsed_value(const untyped_constant& uc, data_type validator)
{
    try {
        if (uc.partial_type == untyped_constant::type_class::hex && validator == bytes_type) {
            auto v = utils::chunked_string_view(uc.raw_text);
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
    if ((receiver_type.is_collection() || receiver_type.is_user_type() || receiver_type.is_vector()) && !uc_is_null) {
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
std::optional<data_type>
default_type_for_constant(const untyped_constant& uc) {
    switch (uc.partial_type) {
        case untyped_constant::type_class::string:          return utf8_type;
        case untyped_constant::type_class::boolean:         return boolean_type;
        case untyped_constant::type_class::floating_point:  return double_type;
        case untyped_constant::type_class::duration:        return duration_type;
        case untyped_constant::type_class::uuid:            return uuid_type;
        case untyped_constant::type_class::hex:             return bytes_type;
        case untyped_constant::type_class::null:            return std::nullopt;
        case untyped_constant::type_class::integer:
            try {
                int32_type->from_string(uc.raw_text);
                return int32_type;
            } catch (const marshal_exception&) {}
            try {
                long_type->from_string(uc.raw_text);
                return long_type;
            } catch (const marshal_exception&) {}
            return varint_type;
    }
    on_internal_error(expr_logger, format("unexpected untyped_constant type_class: {}", static_cast<int>(uc.partial_type)));
}

static
std::optional<expression>
untyped_constant_prepare_expression(const untyped_constant& uc, data_dictionary::database db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver, bool infer_default)
{
    if (!receiver) {
        if (infer_default) {
            if (auto default_type = default_type_for_constant(uc)) {
                raw_value raw_val = cql3::raw_value::make_value(untyped_constant_parsed_value(uc, *default_type));
                return constant(std::move(raw_val), std::move(*default_type));
            }
        }
        return std::nullopt;
    }
    if (!is_assignable(untyped_constant_test_assignment(uc, db, keyspace, *receiver))) {
      if (uc.partial_type != untyped_constant::type_class::null) {
        throw exceptions::invalid_request_exception(format("Invalid {} constant ({}) for \"{}\" of type {}",
            uc.partial_type, uc.raw_text.linearize(), *receiver->name, receiver->type->as_cql3_type().to_string()));
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
        } else if (receiver.type->is_value_compatible_with(*casted_type)
                   || is_widenable_to(casted_type, receiver.type)) {
            return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
        } else {
            return assignment_testable::test_result::NOT_ASSIGNABLE;
        }
    } catch (exceptions::invalid_request_exception& e) {
        throwing_assert(0 && "cast_test_assignment exception");
    }
}

// expr::cast shows up when the user uses a C-style cast with destination type in parenthesis.
// For example: `(int)1242` or `(blob)(int)1337`.
// A C-style cast represents its argument's value as the destination type when that is
// lossless: a byte-compatible reinterpret (e.g. int -> blob, the bytes don't change) or a
// numeric widening (e.g. int -> bigint, converted like CAST).
static
std::optional<expression>
c_cast_prepare_expression(const cast& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver, prepare_memo& memo) {
    data_type cast_type = cast_get_prepared_type(c, db, keyspace);

    if (!receiver) {
        sstring receiver_name = format("cast({}){:user}", cast_type->cql3_type_name(), c.arg);
        receiver = make_lw_shared<column_specification>(
            keyspace, "unknown_cf", ::make_shared<column_identifier>(receiver_name, true), cast_type);
    }

    // casted_spec_of creates a receiver with type equal to c.type
    lw_shared_ptr<column_specification> cast_type_receiver = casted_spec_of(c, db, keyspace, *receiver);

    // First check if the casted expression can be assigned(converted) to the type specified in the cast.
    // test_assignment accepts a value-compatible binary representation or a lossless widening to c.type.
    if (!is_assignable(test_assignment(c.arg, db, keyspace, schema_opt, *cast_type_receiver, memo))) {
        throw exceptions::invalid_request_exception(format("Cannot cast value {:user} to type {}", c.arg, cast_type->as_cql3_type()));
    }

    // Prepare the argument using cast_type_receiver.
    // Using this receiver makes it possible to write things like: (blob)(int)1234
    // Using the original receiver wouldn't work in such cases - it would complain
    // that untyped_constant(1234) isn't a valid blob constant.
    expression prepared_arg = prepare_expression(c.arg, db, keyspace, schema_opt, cast_type_receiver, memo);

    // Then check if a value of type c.type can be assigned(converted) to the receiver type.
    // cast_test_assignment accepts a value-compatible representation or a lossless widening to it.
    if (!is_assignable(cast_test_assignment(c, db, keyspace, schema_opt, *receiver))) {
        throw exceptions::invalid_request_exception(format("Cannot assign value {:user} to {} of type {}", c, receiver->name, receiver->type->as_cql3_type()));
    }

    // Now we know c.arg is compatible with c.type, and c.type with receiver->type - by a binary
    // reinterpret or a lossless widening. The cast node carries c.type; if receiver->type is wider,
    // the consuming prepare_expression coerces the result to it.
    return cast{
        .style = cast::cast_style::c,
        .arg = std::move(prepared_arg),
        .type = cast_type,
    };
}

static
std::optional<expression>
sql_cast_prepare_expression(const cast& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver, prepare_memo& memo) {
    data_type cast_type = cast_get_prepared_type(c, db, keyspace);

    if (!receiver) {
        sstring receiver_name = format("cast({}){:user}", cast_type->cql3_type_name(), c.arg);
        receiver = make_lw_shared<column_specification>(
            keyspace, "unknown_cf", ::make_shared<column_identifier>(receiver_name, true), cast_type);
    }

    auto prepared_arg = prepare_expression(c.arg, db, keyspace, schema_opt, nullptr, memo);

    // cast to the same type should be omitted
    if (cast_type == type_of(prepared_arg)) {
        return prepared_arg;
    }

    // This will throw if a cast is impossible
    auto fun = functions::get_castas_fctn_as_cql3_function(cast_type, type_of(prepared_arg));

    // We implement the cast to a function_call.
    return function_call{
        .func = std::move(fun),
        .args = std::vector({std::move(prepared_arg)}),
    };
}

std::optional<expression>
cast_prepare_expression(const cast& c, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver, prepare_memo& memo) {
    switch (c.style) {
    case cast::cast_style::c:
        return c_cast_prepare_expression(c, db, keyspace, schema_opt, std::move(receiver), memo);
    case cast::cast_style::sql:
        return sql_cast_prepare_expression(c, db, keyspace, schema_opt, std::move(receiver), memo);
    }
    on_internal_error(expr_logger, "Illegal cast style");
}

std::optional<expression>
field_selection_prepare_expression(const field_selection& fs, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver, prepare_memo& memo) {
    // We can't infer the type of the user defined type from the field being selected
    auto prepared_structure = try_prepare_expression(fs.structure, db, keyspace, schema_opt, nullptr, /*infer_default=*/false, memo);
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
field_selection_test_assignment(const field_selection& fs, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo) {
    // We can't infer the type of the user defined type from the field being selected
    auto prepared_structure = try_prepare_expression(fs.structure, db, keyspace, schema_opt, nullptr, /*infer_default=*/false, memo);
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

// Widen two types within a shared lossless numeric chain, returning the wider one
// (nullopt if they share none). Chains: byte < short < int32 < int64 < varint, and
// float < double; cross-chain (e.g. int32 -> double) is not widenable.
static
std::optional<data_type>
try_widen(const data_type& a, const data_type& b) {
    using kind = abstract_type::kind;
    static constexpr kind integer_chain[] = {
        kind::byte, kind::short_kind, kind::int32, kind::long_kind, kind::varint
    };
    static constexpr kind float_chain[] = {
        kind::float_kind, kind::double_kind
    };
    auto rank_in = [] (const kind chain[], size_t len, const data_type& t) -> std::optional<size_t> {
        for (size_t i = 0; i < len; ++i) {
            if (chain[i] == t->get_kind()) {
                return i;
            }
        }
        return std::nullopt;
    };
    auto kind_to_type = [] (kind k) -> data_type {
        switch (k) {
            case kind::byte: return byte_type;
            case kind::short_kind: return short_type;
            case kind::int32: return int32_type;
            case kind::long_kind: return long_type;
            case kind::varint: return varint_type;
            case kind::float_kind: return float_type;
            case kind::double_kind: return double_type;
            default: on_internal_error(expr_logger, format("unexpected kind in numeric widening: {}", static_cast<int>(k)));
        }
    };
    auto ra = rank_in(integer_chain, std::size(integer_chain), a);
    auto rb = rank_in(integer_chain, std::size(integer_chain), b);
    if (ra && rb) {
        return kind_to_type(integer_chain[std::max(*ra, *rb)]);
    }
    ra = rank_in(float_chain, std::size(float_chain), a);
    rb = rank_in(float_chain, std::size(float_chain), b);
    if (ra && rb) {
        return kind_to_type(float_chain[std::max(*ra, *rb)]);
    }
    return std::nullopt;
}

static
bool is_widenable_to(const data_type& from, const data_type& to) {
    auto widened = try_widen(from, to);
    return widened && **widened == *to;
}

// Give `e` target's representation. Precondition: `e` is assignable to `target`.
// Value-compatible types share bytes, so only a constant's label is updated; a
// width-changing widening (e.g. int -> bigint) needs a real castas conversion,
// folded immediately for a terminal constant.
static
expression
coerce_to(expression e, const data_type& target, data_dictionary::database db, const sstring& keyspace) {
    const data_type src = type_of(e);
    if (src == target) {
        return e;
    }
    // Only a width-changing widening needs conversion; every other assignable pair
    // (value-compatible, widening to varint, counter -> bigint, reversed) is byte-safe.
    if (is_widenable_to(src, target) && !target->is_value_compatible_with(*src)) {
        const bool terminal = expr::is<constant>(e);
        function_call conversion {
            .func = functions::get_castas_fctn_as_cql3_function(target, src),
            .args = {std::move(e)},
        };
        if (terminal) {
            return constant(expr::evaluate(conversion, query_options::DEFAULT), target);
        }
        return conversion;
    }
    if (auto* c = expr::as_if<constant>(&e)) {
        c->type = target;
    }
    return e;
}

// One function argument, partially processed for overload resolution.
//   testable - fed to functions::get() to choose the overload.
//   prepared - the argument, if it could be prepared with no receiver. Reusing it
//              (instead of preparing again for each candidate) keeps nested calls linear.
struct partially_prepared_arg {
    ::shared_ptr<assignment_testable> testable;
    std::optional<expression> prepared;
};

static
std::vector<partially_prepared_arg>
prepare_function_args_for_type_inference(std::span<const expression> args, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, prepare_memo& memo) {
    // Prepare the arguments that can be prepared without a receiver.
    // Prepared expressions have a known type, which helps with finding the right function.
    std::vector<partially_prepared_arg> partially_prepared_args;
    partially_prepared_args.reserve(args.size());
    for (const expression& argument : args) {
        // A nullopt means the argument can't be resolved without a receiver (e.g. a nested
        // call that is ambiguous until the parameter type is known) - it stays a hole, to be
        // probed against each candidate parameter type and re-prepared once one is chosen.
        std::optional<expression> prepared_arg_opt = try_prepare_expression(argument, db, keyspace, schema_opt, nullptr, /*infer_default=*/false, memo);
        std::optional<data_type> type;
        if (prepared_arg_opt) {
            type = type_of(*prepared_arg_opt);
        } else {
            // Hole: keep the raw expression as the testable (so overload resolution
            // still fit-checks untyped constants against narrow parameter types), but
            // attach a default-type hint for functions that need to know the argument
            // type up front, such as count() and toJson(). The hint is derived from the
            // same default inference used elsewhere; the defaulted form itself is not
            // reused as the prepared argument, so it does not pin a literal to its
            // default type before the real parameter type is known.
            if (auto defaulted = try_prepare_expression(argument, db, keyspace, schema_opt, nullptr, /*infer_default=*/true, memo)) {
                type = type_of(*defaulted);
            }
        }
        expression testable_expr = prepared_arg_opt ? *prepared_arg_opt : argument;
        partially_prepared_args.push_back(partially_prepared_arg{
            .testable = as_assignment_testable(std::move(testable_expr), std::move(type), memo),
            .prepared = std::move(prepared_arg_opt),
        });
    }
    return partially_prepared_args;
}

// Counts entries to prepare_function_call and test_assignment_function_call. Used by
// tests to guard against re-introducing exponential preparation of nested function calls.
static thread_local uint64_t g_prepare_function_call_count = 0;

uint64_t prepare_function_call_count() {
    return g_prepare_function_call_count;
}

void reset_prepare_function_call_count() {
    g_prepare_function_call_count = 0;
}

static thread_local uint64_t g_test_assignment_function_call_count = 0;

uint64_t test_assignment_function_call_count() {
    return g_test_assignment_function_call_count;
}

void reset_test_assignment_function_call_count() {
    g_test_assignment_function_call_count = 0;
}

// Test-only switch to disable memoization, so tests can observe the un-memoized
// (exponential) probing cost for comparison. Read once when a top-level prepare entry
// point creates its prepare_memo; a stale value across a yield only affects whether a
// cache is used, never correctness.
static thread_local bool g_prepare_memo_enabled = true;

void set_prepare_memo_enabled(bool enabled) {
    g_prepare_memo_enabled = enabled;
}

prepare_memo::prepare_memo() : enabled(g_prepare_memo_enabled) {}

std::optional<expression>
prepare_function_call(const expr::function_call& fc, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver, prepare_memo& memo) {
    ++g_prepare_function_call_count;
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
    auto partially_prepared_args = prepare_function_args_for_type_inference(fc.args, db, keyspace, schema_opt, memo);

    auto fun_opt = std::visit(overloaded_functor{
        [] (const shared_ptr<functions::function>& func) -> std::optional<shared_ptr<functions::function>> {
            return func;
        },
        [&] (const functions::function_name& name) -> std::optional<shared_ptr<functions::function>> {
            auto resolution = functions::instance().try_get(db, keyspace, name,
                    partially_prepared_args
                            | std::views::transform(&partially_prepared_arg::testable)
                            | std::ranges::to<std::vector>(),
                    keyspace, cf_name, receiver.get());
            // try_get yields the function (null if the name isn't declared) or the resolution
            // error. During type inference (no receiver) any non-resolution is an expected hole;
            // with a receiver it is surfaced.
            if (resolution.has_value() && resolution.value()) {
                return std::move(resolution).value();
            }
            if (!receiver) {
                return std::nullopt;
            }
            if (resolution.has_error()) {
                resolution.error().throw_me();
            }
            throw exceptions::invalid_request_exception(format("Unknown function {} called", name));
        },
    }, fc.func);
    if (!fun_opt) {
        return std::nullopt;
    }
    auto fun = std::move(*fun_opt);

    // Functions.get() will complain if no function "name" type check with the provided arguments.
    // Still validate the return type: accepted if value-compatible with the receiver or
    // widenable to it (the caller then wraps the call via coerce_to).
    if (receiver && !receiver->type->is_value_compatible_with(*fun->return_type())
            && !is_widenable_to(fun->return_type(), receiver->type)) {
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
        auto arg_spec = functions::instance().make_arg_spec(keyspace, cf_name, *fun, i);
        // Both paths must yield arg_spec->type: prepare_expression tail-coerces; the
        // reuse path coerces explicitly.
        expr::expression e = [&] () -> expr::expression {
            if (!partially_prepared_args[i].prepared) {
                return prepare_expression(fc.args[i], db, keyspace, schema_opt, arg_spec, memo);
            } else {
                expr::expression prepared = std::move(*partially_prepared_args[i].prepared);
                return coerce_to(std::move(prepared), arg_spec->type, db, keyspace);
            }
        }();
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
test_assignment_function_call(const cql3::expr::function_call& fc, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo) {
    std::optional<call_probe_key> memo_key;
    if (memo.enabled) {
        memo_key = call_probe_key{fc, receiver.type};
        auto it = memo.test_assignment_function_call.find(*memo_key);
        if (it != memo.test_assignment_function_call.end()) {
            return it->second;
        }
    }

    ++g_test_assignment_function_call_count;

    // Note: Functions.get() will return null if the function doesn't exist, or throw is no function matching
    // the arguments can be found. We may get one of those if an undefined/wrong function is used as argument
    // of another, existing, function. In that case, we return true here because we'll throw a proper exception
    // later with a more helpful error message that if we were to return false here.
    auto result = [&] () -> assignment_testable::test_result {
        try {
            auto&& fun = std::visit(overloaded_functor{
                [&] (const functions::function_name& name) {
                    auto args = prepare_function_args_for_type_inference(fc.args, db, keyspace, schema_opt, memo);
                    return functions::instance().get(db, keyspace, name,
                            args
                                    | std::views::transform(&partially_prepared_arg::testable)
                                    | std::ranges::to<std::vector>(),
                            receiver.ks_name, receiver.cf_name, &receiver);
                },
                [] (const shared_ptr<functions::function>& func) {
                    return func;
                },
            }, fc.func);
            if (fun && receiver.type == fun->return_type()) {
                return assignment_testable::test_result::EXACT_MATCH;
            } else if (!fun || receiver.type->is_value_compatible_with(*fun->return_type())
                            || is_widenable_to(fun->return_type(), receiver.type)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            } else {
                return assignment_testable::test_result::NOT_ASSIGNABLE;
            }
        } catch (exceptions::invalid_request_exception& e) {
            return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
        }
    }();

    if (memo_key) {
        memo.test_assignment_function_call.emplace(std::move(*memo_key), result);
    }
    return result;
}

static assignment_testable::test_result expression_test_assignment(const data_type& expr_type,
                                                                   const column_specification& receiver) {
    if (receiver.type->underlying_type() == expr_type->underlying_type() || (receiver.type == long_type && expr_type->is_counter())) {
        return assignment_testable::test_result::EXACT_MATCH;
    } else if (receiver.type->is_value_compatible_with(*expr_type)) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    } else if (is_widenable_to(expr_type, receiver.type)) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    } else {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
}

std::optional<expression> prepare_conjunction(const conjunction& conj,
                                              data_dictionary::database db,
                                              const sstring& keyspace,
                                              const schema* schema_opt,
                                              lw_shared_ptr<column_specification> receiver,
                                              prepare_memo& memo) {
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
            try_prepare_expression(child, db, keyspace, schema_opt, child_receiver, /*infer_default=*/false, memo);
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
        lw_shared_ptr<column_specification> receiver,
        prepare_memo& memo) {
    auto result_type = expr::column_mutation_attribute_type(cma);
    if (receiver.get() != nullptr && receiver->type->without_reversed().get_kind() != result_type->get_kind()) {
        throw exceptions::invalid_request_exception(
            format("A {} produces a {} value, which doesn't match the type: {} of {}",
                    cma.kind, result_type->name(),
                    receiver->type->name(), receiver->name->text()));
    }
    auto column = prepare_expression(cma.column, db, keyspace, schema_opt, nullptr, memo);
    // Helper for the subscript and field-selection cases below: validates that
    // inner_expr is a column, not a primary key column, that its type satisfies
    // type_allowed, and that the cluster feature flag is on.
    auto validate_and_return =
            [&](const expression& inner_expr, std::string_view context,
                auto type_allowed, std::string_view type_allowed_str) -> std::optional<expression> {
        auto inner_cval = expr::as_if<column_value>(&inner_expr);
        if (!inner_cval) {
            throw exceptions::invalid_request_exception(fmt::format("{} on a {} expects a column, got {}", cma.kind, context, inner_expr));
        }
        if (inner_cval->col->is_primary_key()) {
            throw exceptions::invalid_request_exception(fmt::format("{} is not legal on primary key component {}", cma.kind, inner_cval->col->name_as_text()));
        }
        if (!type_allowed(inner_cval->col->type)) {
            throw exceptions::invalid_request_exception(fmt::format("{} on a {} is only valid for {}", cma.kind, context, type_allowed_str));
        }
        if (!db.features().writetime_ttl_individual_element) {
            throw exceptions::invalid_request_exception(fmt::format(
                "{} on a {} is not supported until all nodes in the cluster are upgraded", cma.kind, context));
        }
        return column_mutation_attribute{.kind = cma.kind, .column = std::move(column)};
    };
    // Handle WRITETIME(m[key]) / TTL(m[key]) - a subscript into a non-frozen map or set column
    if (auto sub = expr::as_if<subscript>(&column)) {
        return validate_and_return(sub->val, "subscript",
            [](const data_type& t) { return (t->is_map() || t->is_set()) && t->is_multi_cell(); },
            "non-frozen map or set columns");
    }
    // Handle WRITETIME(x.field) / TTL(x.field) - a field selection into a non-frozen UDT column
    if (auto fs = expr::as_if<field_selection>(&column)) {
        return validate_and_return(fs->structure, "field selection",
            [](const data_type& t) { return t->is_user_type() && t->is_multi_cell(); },
            "non-frozen UDT columns");
    }
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
try_prepare_expression(const expression& expr, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver, bool infer_default) {
    prepare_memo memo;
    return try_prepare_expression(expr, db, keyspace, schema_opt, std::move(receiver), infer_default, memo);
}

static std::optional<expression>
try_prepare_expression(const expression& expr, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver, bool infer_default, prepare_memo& memo) {
    return expr::visit(overloaded_functor{
        [&] (const constant& value) -> std::optional<expression> {
            if (receiver && !is_assignable(expression_test_assignment(value.type, *receiver))) {
                throw exceptions::invalid_request_exception(
                    format("cannot assign a constant {:user} of type {} to receiver {} of type {}", value,
                           value.type->as_cql3_type(), receiver->name, receiver->type->as_cql3_type()));
            }

            if (receiver) {
                return coerce_to(value, receiver->type, db, keyspace);
            }
            return value;
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
        [&] (const unary_operator& uo) -> std::optional<expression> {
            // Prepare the operand; unary_operator preserves its operand's type.
            auto prepared_operand = try_prepare_expression(uo.operand, db, keyspace, schema_opt, receiver);
            if (!prepared_operand) {
                return std::nullopt;
            }
            unary_operator result{uo.op, std::move(*prepared_operand)};
            // Constant folding: if the operand is fully known, evaluate now.
            if (is<constant>(result.operand)) {
                return constant(evaluate(result, query_options::DEFAULT), type_of(result.operand));
            }
            return result;
        },
        [&] (const conjunction& conj) -> std::optional<expression> {
            return prepare_conjunction(conj, db, keyspace, schema_opt, receiver, memo);
        },
        [] (const column_value& cv) -> std::optional<expression> {
            return cv;
        },
        [&] (const subscript& sub) -> std::optional<expression> {
            if (!schema_opt) {
                throw exceptions::invalid_request_exception("cannot process subscript operation without schema");
            }
            auto& schema = *schema_opt;

            auto sub_col_opt = try_prepare_expression(sub.val, db, keyspace, schema_opt, receiver, /*infer_default=*/false, memo);
            if (!sub_col_opt) {
                return std::nullopt;
            }
            auto& sub_col = *sub_col_opt;
            const abstract_type& sub_col_type = type_of(sub_col)->without_reversed();

            auto col_spec = column_specification_of(sub_col);
            lw_shared_ptr<column_specification> subscript_column_spec;
            data_type value_cmp;
            if (sub_col_type.is_map()) {
                subscript_column_spec = map_key_spec_of(*col_spec);
                value_cmp = static_cast<const collection_type_impl&>(sub_col_type).value_comparator();
            } else if (sub_col_type.is_set()) {
                subscript_column_spec = set_value_spec_of(*col_spec);
                value_cmp = static_cast<const collection_type_impl&>(sub_col_type).name_comparator();
            } else if (sub_col_type.is_list()) {
                subscript_column_spec = list_key_spec_of(*col_spec);
                value_cmp = static_cast<const collection_type_impl&>(sub_col_type).value_comparator();
            } else {
                throw exceptions::invalid_request_exception(format("Column {} is not a map/set/list, cannot be subscripted", col_spec->name->text()));
            }

            return subscript {
                .val = sub_col,
                .sub = prepare_expression(sub.sub, db, schema.ks_name(), &schema, std::move(subscript_column_spec), memo),
                .type = value_cmp,
            };
        },
        [&] (const unresolved_identifier& unin) -> std::optional<expression> {
            if (!schema_opt) {
                throw exceptions::invalid_request_exception(fmt::format("Cannot resolve column {} without schema", unin.ident->to_cql_string()));
            }
            return resolve_column(unin, *schema_opt);
        },
        [&] (const column_mutation_attribute& cma) -> std::optional<expression> {
            return prepare_column_mutation_attribute(cma, db, keyspace, schema_opt, std::move(receiver), memo);
        },
        [&] (const function_call& fc) -> std::optional<expression> {
            return prepare_function_call(fc, db, keyspace, schema_opt, std::move(receiver), memo);
        },
        [&] (const cast& c) -> std::optional<expression> {
            return cast_prepare_expression(c, db, keyspace, schema_opt, receiver, memo);
        },
        [&] (const field_selection& fs) -> std::optional<expression> {
            return field_selection_prepare_expression(fs, db, keyspace, schema_opt, receiver, memo);
        },
        [&] (const bind_variable& bv) -> std::optional<expression> {
            return bind_variable_prepare_expression(bv, db, keyspace, receiver);
        },
        [&] (const untyped_constant& uc) -> std::optional<expression> {
            return untyped_constant_prepare_expression(uc, db, keyspace, receiver, infer_default);
        },
        [&] (const tuple_constructor& tc) -> std::optional<expression> {
            return tuple_constructor_prepare_nontuple(tc, db, keyspace, schema_opt, receiver, infer_default, memo);
        },
        [&] (const collection_constructor& c) -> std::optional<expression> {
            switch (c.style) {
            case collection_constructor::style_type::list_or_vector: return list_or_vector_prepare_expression(c, db, keyspace, schema_opt, receiver, infer_default, memo);
            case collection_constructor::style_type::set: return set_prepare_expression(c, db, keyspace, schema_opt, receiver, infer_default, memo);
            case collection_constructor::style_type::map: return map_prepare_expression(c, db, keyspace, schema_opt, receiver, infer_default, memo);
            case collection_constructor::style_type::vector:
                on_internal_error(expr_logger, "vector style type found during prepare, should have been introduced post-prepare");
            }
            on_internal_error(expr_logger, fmt::format("unexpected collection_constructor style {}", static_cast<unsigned>(c.style)));
        },
        [&] (const usertype_constructor& uc) -> std::optional<expression> {
            return usertype_constructor_prepare_expression(uc, db, keyspace, schema_opt, receiver, memo);
        },
        [&] (const temporary& t) -> std::optional<expression> {
            on_internal_error(expr_logger, "temporary found during prepare, should have been introduced post-prepare");
        },
    }, expr);
}

static
assignment_testable::test_result
unresolved_identifier_test_assignment(const unresolved_identifier& ui, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo) {
    auto prepared = prepare_expression(ui, db, keyspace, schema_opt, make_lw_shared<column_specification>(receiver), memo);
    return test_assignment(prepared, db, keyspace, schema_opt, receiver, memo);
}

static
assignment_testable::test_result
column_mutation_attribute_test_assignment(const column_mutation_attribute& cma, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    auto type = column_mutation_attribute_type(cma);
    return expression_test_assignment(std::move(type), std::move(receiver));
}

assignment_testable::test_result
test_assignment(const expression& expr, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    prepare_memo memo;
    return test_assignment(expr, db, keyspace, schema_opt, receiver, memo);
}

static assignment_testable::test_result
test_assignment(const expression& expr, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo) {
    using test_result = assignment_testable::test_result;
    return expr::visit(overloaded_functor{
        [&] (const constant& value) -> test_result {
            return expression_test_assignment(value.type, receiver);
        },
        [&] (const binary_operator&) -> test_result {
            on_internal_error(expr_logger, "binary_operators are not yet reachable via test_assignment()");
        },
        [&] (const unary_operator&) -> test_result {
            on_internal_error(expr_logger, "unary_operators are not yet reachable via test_assignment()");
        },
        [&] (const conjunction&) -> test_result {
            on_internal_error(expr_logger, "conjunctions are not yet reachable via test_assignment()");
        },
        [&] (const column_value& col_val) -> test_result {
            return expression_test_assignment(col_val.col->type, receiver);
        },
        [&] (const subscript&) -> test_result {
            // not implemented. issue #22075
            return assignment_testable::test_result::NOT_ASSIGNABLE;
        },
        [&] (const unresolved_identifier& ui) -> test_result {
            return unresolved_identifier_test_assignment(ui, db, keyspace, schema_opt, receiver, memo);
        },
        [&] (const column_mutation_attribute& cma) -> test_result {
            return column_mutation_attribute_test_assignment(cma, db, keyspace, schema_opt, receiver);
        },
        [&] (const function_call& fc) -> test_result {
            return test_assignment_function_call(fc, db, keyspace, schema_opt, receiver, memo);
        },
        [&] (const cast& c) -> test_result {
            return cast_test_assignment(c, db, keyspace, schema_opt, receiver);
        },
        [&] (const field_selection& fs) -> test_result {
            return field_selection_test_assignment(fs, db, keyspace, schema_opt, receiver, memo);
        },
        [&] (const bind_variable& bv) -> test_result {
            return bind_variable_test_assignment(bv, db, keyspace, receiver);
        },
        [&] (const untyped_constant& uc) -> test_result {
            return untyped_constant_test_assignment(uc, db, keyspace, receiver);
        },
        [&] (const tuple_constructor& tc) -> test_result {
            return tuple_constructor_test_assignment(tc, db, keyspace, schema_opt, receiver, memo);
        },
        [&] (const collection_constructor& c) -> test_result {
            switch (c.style) {
            case collection_constructor::style_type::list_or_vector: return list_or_vector_test_assignment(c, db, keyspace, schema_opt, receiver, memo);
            case collection_constructor::style_type::set: return set_test_assignment(c, db, keyspace, schema_opt, receiver, memo);
            case collection_constructor::style_type::map: return map_test_assignment(c, db, keyspace, schema_opt, receiver, memo);
            case collection_constructor::style_type::vector:
                on_internal_error(expr_logger, "vector style type found in test_assignment, should have been introduced post-prepare");
            }
            on_internal_error(expr_logger, fmt::format("unexpected collection_constructor style {}", static_cast<unsigned>(c.style)));
        },
        [&] (const usertype_constructor& uc) -> test_result {
            return usertype_constructor_test_assignment(uc, db, keyspace, schema_opt, receiver, memo);
        },
        [&] (const temporary& t) -> test_result {
            on_internal_error(expr_logger, "temporary found in test_assignment, should have been introduced post-prepare");
        },
    }, expr);
}

template <cql3_type::kind... Kinds>
assignment_testable::vector_test_result
test_assignment_any_size_float_vector(const expression& expr) {
    using test_result = assignment_testable::vector_test_result;
    const test_result NOT_ASSIGNABLE = {assignment_testable::test_result::NOT_ASSIGNABLE, std::nullopt};
    const test_result WEAKLY_ASSIGNABLE = {assignment_testable::test_result::WEAKLY_ASSIGNABLE, std::nullopt};
    auto is_float_or_bind = [] (const expression& e) {
        return expr::visit(overloaded_functor{
            [] (const bind_variable&) {
                return true;
            },
            [] (const untyped_constant& uc) {
                return uc.partial_type == untyped_constant::type_class::floating_point
                    || uc.partial_type == untyped_constant::type_class::integer;
            },
            [] (const constant& value) {
                auto kind = value.type->as_cql3_type().get_kind();
                return cql3_type::kind_enum_set::frozen<Kinds...>().contains(kind);
            },
            [] (const auto&) {
                return false;
            },
        }, e);
    };
    auto validate_assignment = [&] (const data_type& dt) -> test_result {
         auto vt = dynamic_pointer_cast<const vector_type_impl>(dt->underlying_type());
            if (!vt) {
                return NOT_ASSIGNABLE;
            }
            auto elem_kind = vt->get_elements_type()->as_cql3_type().get_kind();
            if (cql3_type::kind_enum_set::frozen<Kinds...>().contains(elem_kind)) {
                return {assignment_testable::test_result::WEAKLY_ASSIGNABLE, vt->get_dimension()};
            }
            return NOT_ASSIGNABLE;
    };
    return expr::visit(overloaded_functor{
        [&] (const constant& value) -> test_result {
            return validate_assignment(value.type);
        },
        [&] (const binary_operator&) -> test_result {
            return NOT_ASSIGNABLE;
        },
        [&] (const unary_operator&) -> test_result {
            return NOT_ASSIGNABLE;
        },
        [&] (const conjunction&) -> test_result {
            return NOT_ASSIGNABLE;
        },
        [&] (const column_value& col_val) -> test_result {
            return validate_assignment(col_val.col->type);
        },
        [&] (const subscript&) -> test_result {
            return NOT_ASSIGNABLE;
        },
        [&] (const unresolved_identifier& ui) -> test_result {
            return NOT_ASSIGNABLE;
        },
        [&] (const column_mutation_attribute& cma) -> test_result {
            return NOT_ASSIGNABLE;
        },
        [&] (const function_call& fc) -> test_result {
            return NOT_ASSIGNABLE;
        },
        [&] (const cast& c) -> test_result {
            return NOT_ASSIGNABLE;
        },
        [&] (const field_selection& fs) -> test_result {
            return NOT_ASSIGNABLE;
        },
        [&] (const bind_variable& bv) -> test_result {
            return WEAKLY_ASSIGNABLE;
        },
        [&] (const untyped_constant& uc) -> test_result {
            return uc.partial_type == untyped_constant::type_class::null
                ? WEAKLY_ASSIGNABLE
                : NOT_ASSIGNABLE;
        },
        [&] (const tuple_constructor& tc) -> test_result {
            return NOT_ASSIGNABLE;
        },
        [&] (const collection_constructor& c) -> test_result {
            switch (c.style) {
            case collection_constructor::style_type::list_or_vector: {
                if(std::ranges::all_of(c.elements, is_float_or_bind)) {
                    return {assignment_testable::test_result::WEAKLY_ASSIGNABLE, c.elements.size()};
                }
                return NOT_ASSIGNABLE;
            }
            case collection_constructor::style_type::set: return NOT_ASSIGNABLE;
            case collection_constructor::style_type::map: return NOT_ASSIGNABLE;
            case collection_constructor::style_type::vector:
                on_internal_error(expr_logger, "vector style type found in test_assignment, should have been introduced post-prepare");
            }
            on_internal_error(expr_logger, fmt::format("unexpected collection_constructor style {}", static_cast<unsigned>(c.style)));
        },
        [&] (const usertype_constructor& uc) -> test_result {
            return NOT_ASSIGNABLE;
        },
        [&] (const temporary& t) -> test_result {
            return NOT_ASSIGNABLE;
        },
    }, expr);
}

assignment_testable::vector_test_result
test_assignment_any_size_float_vector(const expression& expr) {
    return test_assignment_any_size_float_vector<cql3_type::kind::FLOAT, cql3_type::kind::DOUBLE>(expr);
}

expression
prepare_expression(const expression& expr, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver) {
    prepare_memo memo;
    return prepare_expression(expr, db, keyspace, schema_opt, std::move(receiver), memo);
}

static expression
prepare_expression(const expression& expr, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, lw_shared_ptr<column_specification> receiver, prepare_memo& memo) {
    // Pass 1: contextual typing. Expressions without a type (untyped constants,
    // collection literals) yield nullopt when no receiver constrains them.
    auto e_opt = try_prepare_expression(expr, db, keyspace, schema_opt, receiver, /*infer_default=*/false, memo);
    // Pass 2: default-type inference. The same prepare is retried with infer_default
    // enabled, so untyped constants and collection literals fall back to a default
    // type at the point where no receiver is available.
    if (!e_opt) {
        e_opt = try_prepare_expression(expr, db, keyspace, schema_opt, receiver, /*infer_default=*/true, memo);
    }
    if (!e_opt) {
        throw exceptions::invalid_request_exception(fmt::format("Could not infer type of {}", expr));
    }
    expression result = std::move(*e_opt);
    if (receiver) {
        result = coerce_to(std::move(result), receiver->type, db, keyspace);
    }
    return result;
}

assignment_testable::test_result
test_assignment_all(const std::vector<expression>& to_test, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) {
    prepare_memo memo;
    return test_assignment_all(to_test, db, keyspace, schema_opt, receiver, memo);
}

static assignment_testable::test_result
test_assignment_all(const std::vector<expression>& to_test, data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver, prepare_memo& memo) {
    using test_result = assignment_testable::test_result;
    test_result res = test_result::EXACT_MATCH;
    for (auto&& e : to_test) {
        test_result t = test_assignment(e, db, keyspace, schema_opt, receiver, memo);
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
    prepare_memo* _memo;
public:
    explicit assignment_testable_expression(expression e, std::optional<data_type> type_opt, prepare_memo* memo) : _e(std::move(e)), _type_opt(std::move(type_opt)), _memo(memo) {}
    virtual test_result test_assignment(data_dictionary::database db, const sstring& keyspace, const schema* schema_opt, const column_specification& receiver) const override {
        if (_memo) {
            return expr::test_assignment(_e, db, keyspace, schema_opt, receiver, *_memo);
        }
        return expr::test_assignment(_e, db, keyspace, schema_opt, receiver);
    }
    virtual vector_test_result test_assignment_any_size_float_vector() const override {
        return expr::test_assignment_any_size_float_vector(_e);
    }
    virtual sstring assignment_testable_source_context() const override {
        return fmt::format("{}", _e);
    }
    virtual std::optional<data_type> assignment_testable_type_opt() const override {
        return _type_opt;
    }
};

::shared_ptr<assignment_testable> as_assignment_testable(expression e, std::optional<data_type> type_opt) {
    return ::make_shared<assignment_testable_expression>(std::move(e), std::move(type_opt), nullptr);
}

static ::shared_ptr<assignment_testable> as_assignment_testable(expression e, std::optional<data_type> type_opt, prepare_memo& memo) {
    return ::make_shared<assignment_testable_expression>(std::move(e), std::move(type_opt), &memo);
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
            } else if (sub_col.col->type->is_set()) {
                return set_value_spec_of(*sub_col.col->column_specification);
            } else {
                return list_value_spec_of(*sub_col.col->column_specification);
            }
        },
        [&](const field_selection& fs) -> lw_shared_ptr<column_specification> {
            return make_lw_shared<column_specification>(
                schema.ks_name(), schema.cf_name(),
                ::make_shared<column_identifier>(fs.field->text(), true),
                fs.type);
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

    if (oper == oper_t::IN || oper == oper_t::NOT_IN) {
        data_type rhs_receiver_type = list_type_impl::get_instance(std::move(lhs_type), false);
        auto in_name = ::make_shared<column_identifier>(format("{}({})", oper, lhs_receiver->name->text()), true);
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
