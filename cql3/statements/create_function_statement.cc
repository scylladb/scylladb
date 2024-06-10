/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include "cql3/statements/create_function_statement.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/user_function.hh"
#include "prepared_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "lang/lua.hh"
#include "data_dictionary/data_dictionary.hh"
#include "lang/manager.hh"
#include "cql3/query_processor.hh"

namespace cql3 {

namespace statements {


seastar::future<shared_ptr<functions::function>> create_function_statement::create(query_processor& qp, functions::function* old) const {
    if (old && !dynamic_cast<functions::user_function*>(old)) {
        throw exceptions::invalid_request_exception(format("Cannot replace '{}' which is not a user defined function", *old));
    }
    if (_language != "lua" && _language != "wasm") {
        throw exceptions::invalid_request_exception(format("Language '{}' is not supported", _language));
    }
    data_type return_type = prepare_type(qp, *_return_type);
    std::vector<sstring> arg_names;
    for (const auto& arg_name : _arg_names) {
        arg_names.push_back(arg_name->to_string());
    }

    auto ctx = co_await qp.lang().create(_language, _name.name, arg_names, _body);
    if (!ctx) {
        co_return nullptr;
    }
    co_return ::make_shared<functions::user_function>(_name, _arg_types, std::move(arg_names), _body, _language,
        std::move(return_type), _called_on_null_input, std::move(*ctx));
}

std::unique_ptr<prepared_statement> create_function_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<create_function_statement>(*this));
}

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>>
create_function_statement::prepare_schema_mutations(query_processor& qp, const query_options&, api::timestamp_type ts) const {
    ::shared_ptr<cql_transport::event::schema_change> ret;
    std::vector<mutation> m;

    auto func = dynamic_pointer_cast<functions::user_function>(co_await validate_while_executing(qp));

    if (func) {
        m = co_await service::prepare_new_function_announcement(qp.proxy(), func, ts);
        ret = create_schema_change(*func, true);
    }

    co_return std::make_tuple(std::move(ret), std::move(m), std::vector<sstring>());
}

create_function_statement::create_function_statement(functions::function_name name, sstring language, sstring body,
        std::vector<shared_ptr<column_identifier>> arg_names, std::vector<shared_ptr<cql3_type::raw>> arg_types,
        shared_ptr<cql3_type::raw> return_type, bool called_on_null_input, bool or_replace, bool if_not_exists)
    : create_function_statement_base(std::move(name), std::move(arg_types), or_replace, if_not_exists),
      _language(std::move(language)), _body(std::move(body)), _arg_names(std::move(arg_names)),
      _return_type(std::move(return_type)), _called_on_null_input(called_on_null_input) {}
}
}
