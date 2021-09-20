/*
 * Copyright (C) 2015-present ScyllaDB
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

#include "selectable.hh"
#include "selectable_with_field_selection.hh"
#include "field_selector.hh"
#include "writetime_or_ttl.hh"
#include "selector_factories.hh"
#include "cql3/query_options.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/castas_fcts.hh"
#include "cql3/functions/aggregate_fcts.hh"
#include "abstract_function_selector.hh"
#include "writetime_or_ttl_selector.hh"

namespace cql3 {

namespace selection {

seastar::logger slogger("cql3_selection");

shared_ptr<selector::factory>
selectable::writetime_or_ttl::new_selector_factory(database& db, schema_ptr s, std::vector<const column_definition*>& defs) {
    auto&& def = s->get_column_definition(_id->name());
    if (!def || def->is_hidden_from_cql()) {
        throw exceptions::invalid_request_exception(format("Undefined name {} in selection clause", _id));
    }
    if (def->is_primary_key()) {
        throw exceptions::invalid_request_exception(
                format("Cannot use selection function {} on PRIMARY KEY part {}",
                              _is_writetime ? "writeTime" : "ttl",
                              def->name()));
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

shared_ptr<selector::factory>
selectable::with_function::new_selector_factory(database& db, schema_ptr s, std::vector<const column_definition*>& defs) {
    auto&& factories = selector_factories::create_factories_and_collect_column_definitions(_args, db, s, defs);

    // resolve built-in functions before user defined functions
    auto&& fun = functions::functions::get(db, s->ks_name(), _function_name, factories->new_instances(), s->ks_name(), s->cf_name());
    if (!fun) {
        throw exceptions::invalid_request_exception(format("Unknown function '{}'", _function_name));
    }
    if (!fun->return_type()) {
        throw exceptions::invalid_request_exception(format("Unknown function {} called in selection clause", _function_name));
    }

    return abstract_function_selector::new_factory(std::move(fun), std::move(factories));
}

sstring
selectable::with_function::to_string() const {
    return format("{}({})", _function_name.name, join(", ", _args));
}

expr::expression
make_count_rows_function_expression() {
    return expr::function_call{
            cql3::functions::function_name::native_function(cql3::functions::aggregate_fcts::COUNT_ROWS_FUNCTION_NAME),
                    std::vector<cql3::expr::expression>()};
}

shared_ptr<selector::factory>
selectable::with_anonymous_function::new_selector_factory(database& db, schema_ptr s, std::vector<const column_definition*>& defs) {
    auto&& factories = selector_factories::create_factories_and_collect_column_definitions(_args, db, s, defs);
    return abstract_function_selector::new_factory(_function, std::move(factories));
}

sstring
selectable::with_anonymous_function::to_string() const {
    return format("{}({})", _function->name().name, join(", ", _args));
}

shared_ptr<selector::factory>
selectable::with_field_selection::new_selector_factory(database& db, schema_ptr s, std::vector<const column_definition*>& defs) {
    auto&& factory = _selected->new_selector_factory(db, s, defs);
    auto&& type = factory->new_instance()->get_type();
    if (!type->underlying_type()->is_user_type()) {
        throw exceptions::invalid_request_exception(
                format("Invalid field selection: {} of type {} is not a user type", _selected->to_string(), type->as_cql3_type()));
    }

    auto ut = static_pointer_cast<const user_type_impl>(type->underlying_type());
    auto idx = ut->idx_of_field(_field->bytes_);
    if (!idx) {
        throw exceptions::invalid_request_exception(format("{} of type {} has no field {}",
                                                           _selected->to_string(), ut->as_cql3_type(), _field));
    }

    return field_selector::new_factory(std::move(ut), *idx, std::move(factory));
}

sstring
selectable::with_field_selection::to_string() const {
    return format("{}.{}", _selected->to_string(), _field->to_string());
}

shared_ptr<selector::factory>
selectable::with_cast::new_selector_factory(database& db, schema_ptr s, std::vector<const column_definition*>& defs) {
    std::vector<shared_ptr<selectable>> args{_arg};
    auto&& factories = selector_factories::create_factories_and_collect_column_definitions(args, db, s, defs);
    auto&& fun = functions::castas_functions::get(_type.get_type(), factories->new_instances());

    return abstract_function_selector::new_factory(std::move(fun), std::move(factories));
}

sstring
selectable::with_cast::to_string() const {
    return format("cast({} as {})", _arg->to_string(), _type.to_string());
}

shared_ptr<selectable>
prepare_selectable(const schema& s, const expr::expression& raw_selectable) {
    return std::visit(overloaded_functor{
        [&] (bool bool_constant) -> shared_ptr<selectable> {
            on_internal_error(slogger, "no way to express SELECT TRUE/FALSE in the grammar yet");
        },
        [&] (const expr::conjunction& conj) -> shared_ptr<selectable> {
            on_internal_error(slogger, "no way to express 'SELECT a AND b' in the grammar yet");
        },
        [&] (const expr::binary_operator& conj) -> shared_ptr<selectable> {
            on_internal_error(slogger, "no way to express 'SELECT a binop b' in the grammar yet");
        },
        [&] (const expr::column_value& column) -> shared_ptr<selectable> {
            // There is no path that reaches here, but expr::column_value and column_identifier are logically the same,
            // so bridge them.
            return ::make_shared<column_identifier>(column.col->name(), column.col->name_as_text());
        },
        [&] (const expr::token& tok) -> shared_ptr<selectable> {
            // expr::token implicitly the partition key as arguments, but
            // the selectable equivalent (with_function) needs explicit arguments,
            // so construct them here.
            auto name = functions::function_name("system", "token");
            auto args = boost::copy_range<std::vector<shared_ptr<selectable>>>(
                s.partition_key_columns()
                | boost::adaptors::transformed([&] (const column_definition& cdef) {
                    return ::make_shared<column_identifier>(cdef.name(), cdef.name_as_text());
                }));
            return ::make_shared<selectable::with_function>(std::move(name), std::move(args));
        },
        [&] (const expr::unresolved_identifier& ui) -> shared_ptr<selectable> {
            return ui.ident->prepare(s);
        },
        [&] (const expr::column_mutation_attribute& cma) -> shared_ptr<selectable> {
            auto unresolved_id = std::get<expr::unresolved_identifier>(*cma.column);
            bool is_writetime = cma.kind == expr::column_mutation_attribute::attribute_kind::writetime;
            return make_shared<selectable::writetime_or_ttl>(unresolved_id.ident->prepare_column_identifier(s), is_writetime);
        },
        [&] (const expr::function_call& fc) -> shared_ptr<selectable> {
            std::vector<shared_ptr<selectable>> prepared_args;
            prepared_args.reserve(fc.args.size());
            for (auto&& arg : fc.args) {
                prepared_args.push_back(prepare_selectable(s, arg));
            }
            return std::visit(overloaded_functor{
                [&] (const functions::function_name& named) -> shared_ptr<selectable> {
                    return ::make_shared<selectable::with_function>(named, std::move(prepared_args));
                },
                [&] (const shared_ptr<functions::function>& anon) -> shared_ptr<selectable> {
                    return ::make_shared<selectable::with_anonymous_function>(anon, std::move(prepared_args));
                },
            }, fc.func);
        },
        [&] (const expr::cast& c) -> shared_ptr<selectable> {
            auto t = std::get_if<cql3_type>(&c.type);
            if (!t) {
                // FIXME: adjust prepare_seletable() signature so we can prepare the type too
                on_internal_error(slogger, "unprepared type in selector type cast");
            }
            return ::make_shared<selectable::with_cast>(prepare_selectable(s, *c.arg), *t);
        },
        [&] (const expr::field_selection& fs) -> shared_ptr<selectable> {
            // static_pointer_cast<> needed due to lack of covariant return type
            // support with smart pointers
            return make_shared<selectable::with_field_selection>(prepare_selectable(s, *fs.structure),
                    static_pointer_cast<column_identifier>(fs.field->prepare(s)));
        },
        [&] (const expr::null&) -> shared_ptr<selectable> {
            on_internal_error(slogger, "null found its way to selector context");
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
    }, raw_selectable);
}

bool
selectable_processes_selection(const expr::expression& raw_selectable) {
    return std::visit(overloaded_functor{
        [&] (bool bool_constant) -> bool {
            on_internal_error(slogger, "no way to express SELECT TRUE/FALSE in the grammar yet");
        },
        [&] (const expr::conjunction& conj) -> bool {
            on_internal_error(slogger, "no way to express 'SELECT a AND b' in the grammar yet");
        },
        [&] (const expr::binary_operator& conj) -> bool {
            on_internal_error(slogger, "no way to express 'SELECT a binop b' in the grammar yet");
        },
        [&] (const expr::column_value& column) -> bool {
            // There is no path that reaches here, but expr::column_value and column_identifier are logically the same,
            // so bridge them.
            return false;
        },
        [&] (const expr::token&) -> bool {
            // Arguably, should return false, because it only processes the partition key.
            // But selectable::with_function considers it true now, so return that.
            return true;
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
        [&] (const expr::null&) -> bool {
            on_internal_error(slogger, "null found its way to selector context");
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
