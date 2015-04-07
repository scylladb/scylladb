/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "selectable.hh"
#include "selector_factories.hh"
#include "cql3/functions/functions.hh"
#include "abstract_function_selector.hh"

namespace cql3 {

namespace selection {

shared_ptr<selector::factory>
selectable::with_function::new_selector_factory(schema_ptr s, std::vector<const column_definition*>& defs) {
    auto&& factories = selector_factories::create_factories_and_collect_column_definitions(_args, s, defs);

    // resolve built-in functions before user defined functions
    auto&& fun = functions::functions::get(s->ks_name, _function_name, factories->new_instances(), s->ks_name, s->cf_name);
    if (!fun) {
        throw exceptions::invalid_request_exception(sprint("Unknown function '%s'", _function_name));
    }
    if (!fun->return_type()) {
        throw exceptions::invalid_request_exception(sprint("Unknown function %s called in selection clause", _function_name));
    }

    return abstract_function_selector::new_factory(std::move(fun), std::move(factories));
}

shared_ptr<selectable>
selectable::with_function::raw::prepare(schema_ptr s) {
        std::vector<shared_ptr<selectable>> prepared_args;
        prepared_args.reserve(_args.size());
        for (auto&& arg : _args) {
            prepared_args.push_back(arg->prepare(s));
        }
        return ::make_shared<with_function>(_function_name, std::move(prepared_args));
    }

bool
selectable::with_function::raw::processes_selection() const {
    return true;
}

}

}
