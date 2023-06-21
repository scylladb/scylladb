/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "selectable.hh"
#include "selectable_with_field_selection.hh"
#include "field_selector.hh"
#include "writetime_or_ttl.hh"
#include "selector_factories.hh"
#include "simple_selector.hh"
#include "cql3/query_options.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/castas_fcts.hh"
#include "cql3/functions/aggregate_fcts.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/expr-utils.hh"
#include "abstract_function_selector.hh"
#include "writetime_or_ttl_selector.hh"

namespace cql3 {

namespace selection {

seastar::logger slogger("cql3_selection");

class selectable_column : public selectable {
    column_identifier _ci;
public:
    explicit selectable_column(column_identifier ci) : _ci(std::move(ci)) {}
    virtual ::shared_ptr<selector::factory> new_selector_factory(data_dictionary::database db, schema_ptr schema,
        std::vector<const column_definition*>& defs) override;
    virtual sstring to_string() const override {
        return _ci.to_string();
    }
};

::shared_ptr<selector::factory>
selectable_column::new_selector_factory(data_dictionary::database db, schema_ptr schema, std::vector<const column_definition*>& defs) {
    auto def = get_column_definition(*schema, _ci);
    if (!def) {
        throw exceptions::invalid_request_exception(format("Undefined name {} in selection clause", _ci.text()));
    }
    // Do not allow explicitly selecting hidden columns. We also skip them on
    // "SELECT *" (see selection::wildcard()).
    if (def->is_hidden_from_cql()) {
        throw exceptions::invalid_request_exception(format("Undefined name {} in selection clause", _ci.text()));
    }
    return simple_selector::new_factory(def->name_as_text(), add_and_get_index(*def, defs), def->type);
}

shared_ptr<selector::factory>
selectable::writetime_or_ttl::new_selector_factory(data_dictionary::database db, schema_ptr s, std::vector<const column_definition*>& defs) {
    auto&& def = s->get_column_definition(_id->name());
    if (!def || def->is_hidden_from_cql()) {
        throw exceptions::invalid_request_exception(format("Undefined name {} in selection clause", _id));
    }
    if (def->is_primary_key()) {
        throw exceptions::invalid_request_exception(
                format("Cannot use selection function {} on PRIMARY KEY part {}",
                              _is_writetime ? "writeTime" : "ttl",
                              def->name_as_text()));
    }
    if (def->type->is_multi_cell()) {
        throw exceptions::invalid_request_exception(format("Cannot use selection function {} on non-frozen collections",
                                                        _is_writetime ? "writeTime" : "ttl"));
    }

    return writetime_or_ttl_selector::new_factory(def->name_as_text(), add_and_get_index(*def, defs), _is_writetime);
}

sstring
selectable::writetime_or_ttl::to_string() const {
    return format("{}({})", _is_writetime ? "writetime" : "ttl", _id->to_string());
}

expr::expression
make_count_rows_function_expression() {
    return expr::function_call{
            cql3::functions::function_name::native_function(cql3::functions::aggregate_fcts::COUNT_ROWS_FUNCTION_NAME),
                    std::vector<cql3::expr::expression>()};
}

shared_ptr<selector::factory>
selectable::with_anonymous_function::new_selector_factory(data_dictionary::database db, schema_ptr s, std::vector<const column_definition*>& defs) {
    auto&& factories = selector_factories::create_factories_and_collect_column_definitions(_args, db, s, defs);
    return abstract_function_selector::new_factory(_function, std::move(factories));
}

sstring
selectable::with_anonymous_function::to_string() const {
    return format("{}({})", _function->name().name, fmt::join(_args, ", "));
}

shared_ptr<selector::factory>
selectable::with_field_selection::new_selector_factory(data_dictionary::database db, schema_ptr s, std::vector<const column_definition*>& defs) {
    auto&& factory = _selected->new_selector_factory(db, s, defs);
    auto&& type = factory->new_instance()->get_type();
    if (!type->underlying_type()->is_user_type()) {
        throw exceptions::invalid_request_exception(
                format("Invalid field selection: {} of type {} is not a user type", _selected->to_string(), type->as_cql3_type()));
    }

    auto ut = static_pointer_cast<const user_type_impl>(type->underlying_type());

    return field_selector::new_factory(std::move(ut), _field_idx, std::move(factory));
}

sstring
selectable::with_field_selection::to_string() const {
    return format("{}.{}", _selected->to_string(), _field->to_string());
}

static
shared_ptr<selectable>
do_prepare_selectable(const schema& s, const expr::expression& selectable_expr) {
    return expr::visit(overloaded_functor{
        [&] (const expr::constant&) -> shared_ptr<selectable> {
            on_internal_error(slogger, "no way to express SELECT constant in the grammar yet");
        },
        [&] (const expr::conjunction& conj) -> shared_ptr<selectable> {
            on_internal_error(slogger, "no way to express 'SELECT a AND b' in the grammar yet");
        },
        [&] (const expr::binary_operator& conj) -> shared_ptr<selectable> {
            on_internal_error(slogger, "no way to express 'SELECT a binop b' in the grammar yet");
        },
        [&] (const expr::column_value& column) -> shared_ptr<selectable> {
            return ::make_shared<selectable_column>(column_identifier(column.col->name(), column.col->name_as_text()));
        },
        [&] (const expr::subscript& sub) -> shared_ptr<selectable> {
            on_internal_error(slogger, "no way to express 'SELECT a[b]' in the grammar yet");
        },
        [&] (const expr::unresolved_identifier& ui) -> shared_ptr<selectable> {
            on_internal_error(slogger, "prepare_selectable() on an unresolved_identifier");
        },
        [&] (const expr::column_mutation_attribute& cma) -> shared_ptr<selectable> {
            auto column = expr::as<expr::column_value>(cma.column);
            bool is_writetime = cma.kind == expr::column_mutation_attribute::attribute_kind::writetime;
            return make_shared<selectable::writetime_or_ttl>(make_shared<column_identifier>(column.col->name(), column.col->name_as_text()), is_writetime);
        },
        [&] (const expr::function_call& fc) -> shared_ptr<selectable> {
            std::vector<shared_ptr<selectable>> prepared_args;
            prepared_args.reserve(fc.args.size());
            for (auto&& arg : fc.args) {
                prepared_args.push_back(do_prepare_selectable(s, arg));
            }
            return std::visit(overloaded_functor{
                [&] (const functions::function_name& named) -> shared_ptr<selectable> {
                    on_internal_error(slogger, "function name should have been resolved by prepare");
                },
                [&] (const shared_ptr<functions::function>& anon) -> shared_ptr<selectable> {
                    return ::make_shared<selectable::with_anonymous_function>(anon, std::move(prepared_args));
                },
            }, fc.func);
        },
        [&] (const expr::cast& c) -> shared_ptr<selectable> {
            on_internal_error(slogger, "cast should have been converted to a function call");
        },
        [&] (const expr::field_selection& fs) -> shared_ptr<selectable> {
            // static_pointer_cast<> needed due to lack of covariant return type
            // support with smart pointers
            return make_shared<selectable::with_field_selection>(do_prepare_selectable(s, fs.structure),
                    fs.field->prepare(s),
                    fs.field_idx);
        },
        [&] (const expr::bind_variable&) -> shared_ptr<selectable> {
            on_internal_error(slogger, "bind_variable found its way to selector context");
        },
        [&] (const expr::untyped_constant&) -> shared_ptr<selectable> {
            on_internal_error(slogger, "untyped_constant found its way to selector context");
        },
        [&] (const expr::tuple_constructor&) -> shared_ptr<selectable> {
            on_internal_error(slogger, "tuple_constructor found its way to selector context");
        },
        [&] (const expr::collection_constructor&) -> shared_ptr<selectable> {
            on_internal_error(slogger, "collection_constructor found its way to selector context");
        },
        [&] (const expr::usertype_constructor&) -> shared_ptr<selectable> {
            on_internal_error(slogger, "usertype_constructor found its way to selector context");
        },
    }, selectable_expr);
}

shared_ptr<selectable>
prepare_selectable(const schema& s, const expr::expression& raw_selectable, data_dictionary::database db, const sstring& keyspace) {
    // do_prepare_selectable() calls itself recursively, so the prepare_expression step
    // has to be done outside.
    auto prepared = expr::prepare_expression(raw_selectable, db, keyspace, &s, nullptr);
    return do_prepare_selectable(s, prepared);
}

bool
selectable_processes_selection(const expr::expression& raw_selectable) {
    return expr::visit(overloaded_functor{
        [&] (const expr::constant&) -> bool {
            on_internal_error(slogger, "no way to express SELECT constant in the grammar yet");
        },
        [&] (const expr::conjunction& conj) -> bool {
            on_internal_error(slogger, "no way to express 'SELECT a AND b' in the grammar yet");
        },
        [&] (const expr::binary_operator& conj) -> bool {
            on_internal_error(slogger, "no way to express 'SELECT a binop b' in the grammar yet");
        },
        [] (const expr::subscript&) -> bool {
            on_internal_error(slogger, "no way to express 'SELECT a[b]' in the grammar yet");
        },
        [&] (const expr::column_value& column) -> bool {
            // There is no path that reaches here, but expr::column_value and column_identifier are logically the same,
            // so bridge them.
            return false;
        },
        [&] (const expr::unresolved_identifier& ui) -> bool {
            return ui.ident->processes_selection();
        },
        [&] (const expr::column_mutation_attribute& cma) -> bool {
            return true;
        },
        [&] (const expr::function_call& fc) -> bool {
            return true;
        },
        [&] (const expr::cast& c) -> bool {
            return true;
        },
        [&] (const expr::field_selection& fs) -> bool {
            return true;
        },
        [&] (const expr::bind_variable&) -> bool {
            on_internal_error(slogger, "bind_variable found its way to selector context");
        },
        [&] (const expr::untyped_constant&) -> bool {
            on_internal_error(slogger, "untyped_constant found its way to selector context");
        },
        [&] (const expr::tuple_constructor&) -> bool {
            on_internal_error(slogger, "tuple_constructor found its way to selector context");
        },
        [&] (const expr::collection_constructor&) -> bool {
            on_internal_error(slogger, "collection_constructor found its way to selector context");
        },
        [&] (const expr::usertype_constructor&) -> bool {
            on_internal_error(slogger, "collection_constructor found its way to selector context");
        },
    }, raw_selectable);
};

std::ostream & operator<<(std::ostream &os, const selectable& s) {
    return os << s.to_string();
}

}

}
