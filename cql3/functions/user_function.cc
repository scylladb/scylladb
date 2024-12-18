/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "user_function.hh"
#include "cql3/description.hh"
#include "cql3/util.hh"
#include "utils/log.hh"
#include "lang/wasm.hh"

#include <seastar/core/thread.hh>

#include <ranges>

namespace cql3 {
namespace functions {

extern logging::logger log;

user_function::user_function(function_name name, std::vector<data_type> arg_types, std::vector<sstring> arg_names,
        sstring body, sstring language, data_type return_type, bool called_on_null_input, context ctx)
    : abstract_function(std::move(name), std::move(arg_types), std::move(return_type)),
      _arg_names(std::move(arg_names)), _body(std::move(body)), _language(std::move(language)),
      _called_on_null_input(called_on_null_input), _ctx(std::move(ctx)) {}

bool user_function::is_pure() const { return true; }

bool user_function::is_native() const { return false; }

bool user_function::is_aggregate() const { return false; }

bool user_function::requires_thread() const { return true; }

bytes_opt user_function::execute(std::span<const bytes_opt> parameters) {
    const auto& types = arg_types();
    if (parameters.size() != types.size()) {
        throw std::logic_error("Wrong number of parameters");
    }

    if (!seastar::thread::running_in_thread()) {
        on_internal_error(log, "User function cannot be executed in this context");
    }
    for (auto& param : parameters) {
        if (!param && !_called_on_null_input) {
            return std::nullopt;
        }
    }
    return seastar::visit(_ctx,
        [&] (lua_context& ctx) -> bytes_opt {
            std::vector<data_value> values;
            values.reserve(parameters.size());
            for (int i = 0, n = types.size(); i != n; ++i) {
                const data_type& type = types[i];
                const bytes_opt& bytes = parameters[i];
                values.push_back(bytes ? type->deserialize(*bytes) : data_value::make_null(type));
            }
            return lua::run_script(lua::bitcode_view{ctx.bitcode}, values, return_type(), ctx.cfg).get();
        },
        [&] (wasm::context& ctx) -> bytes_opt {
            try {
                return wasm::run_script(name(), ctx, arg_types(), parameters, return_type(), _called_on_null_input).get();
            } catch (const wasm::exception& e) {
                throw exceptions::invalid_request_exception(format("UDF error: {}", e.what()));
            }
        });
}

description user_function::describe(with_create_statement with_stmt) const {
    auto maybe_create_statement = std::invoke([&] -> std::optional<sstring> {
        if (!with_stmt) {
            return std::nullopt;
        }

        auto arg_type_range = _arg_types | std::views::transform(std::mem_fn(&abstract_type::cql3_type_name_without_frozen));
        auto arg_range = std::views::zip(_arg_names, arg_type_range)
                | std::views::transform([] (std::tuple<std::string_view, std::string_view> arg) {
                    const auto [name, type] = arg;
                    return seastar::format("{} {}", name, type);
                });

        return seastar::format("CREATE FUNCTION {}.{}({})\n"
                "{} ON NULL INPUT\n"
                "RETURNS {}\n"
                "LANGUAGE {}\n"
                "AS $${}$$;",
                cql3::util::maybe_quote(name().keyspace), cql3::util::maybe_quote(name().name), fmt::join(arg_range, ", "),
                _called_on_null_input ? "CALLED" : "RETURNS NULL",
                _return_type->cql3_type_name_without_frozen(),
                _language,
                _body);
    });

    return description {
        .keyspace = name().keyspace,
        .type = "function",
        .name = name().name,
        .create_statement = std::move(maybe_create_statement)
    };
}

}
}
