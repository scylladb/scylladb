/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "user_function.hh"
#include "cql3/description.hh"
#include "cql3/util.hh"
#include "utils/log.hh"
#include "lang/wasm.hh"
#include "utils/managed_string.hh"
#include "types/concrete_types.hh"

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

template <typename T>
constexpr bool is_emptyable = false;
template <typename T>
constexpr bool is_emptyable<emptyable<T>> = true;

// Most types allow an "empty" (zero-length) value, which is not the same as
// null. For some types, like text and blob, the empty value is an ordinary
// value of the type (an empty string). For the others, like int, it isn't,
// so a function receives an empty value of such a type as a null.
static bool empty_value_is_null(const abstract_type& type) {
    return visit(type, [] <typename T> (const T& t) {
        if constexpr (std::is_same_v<T, reversed_type_impl>) {
            return empty_value_is_null(*t.underlying_type());
        } else if constexpr (std::is_same_v<T, counter_type_impl>) {
            // A counter has no native_type, but its values are bigints.
            return true;
        } else if constexpr (requires { typename T::native_type; }) {
            return is_emptyable<typename T::native_type>;
        } else {
            return false;
        }
    });
}

bool user_function::is_pure() const { return true; }

bool user_function::is_native() const { return false; }

bool user_function::is_aggregate() const { return false; }

bool user_function::requires_thread() const { return true; }

managed_bytes_opt user_function::execute(std::span<const managed_bytes_opt> parameters) {
    const auto& types = arg_types();
    if (parameters.size() != types.size()) {
        throw std::logic_error("Wrong number of parameters");
    }

    if (!seastar::thread::running_in_thread()) {
        on_internal_error(log, "User function cannot be executed in this context");
    }
    auto is_null = [&] (size_t i) {
        return !parameters[i] || (parameters[i]->empty() && empty_value_is_null(*types[i]));
    };
    if (!_called_on_null_input) {
        for (size_t i = 0; i < parameters.size(); ++i) {
            if (is_null(i)) {
                return std::nullopt;
            }
        }
    }
    return to_managed_bytes_opt(seastar::visit(_ctx,
        [&] (lua_context& ctx) -> bytes_opt {
            std::vector<data_value> values;
            values.reserve(parameters.size());
            for (int i = 0, n = types.size(); i != n; ++i) {
                const data_type& type = types[i];
                values.push_back(is_null(i) ? data_value::make_null(type) : type->deserialize(managed_bytes_view(*parameters[i])));
            }
            return lua::run_script(lua::bitcode_view{ctx.bitcode}, values, return_type(), ctx.cfg).get();
        },
        [&] (wasm::context& ctx) -> bytes_opt {
            // FIXME: change wasm::run_script to accept fragmented buffers
            auto linear_parameters = std::views::iota(size_t(0), parameters.size())
                    | std::views::transform([&] (size_t i) { return is_null(i) ? bytes_opt() : to_bytes_opt(parameters[i]); })
                    | std::ranges::to<std::vector>();
            try {
                return wasm::run_script(name(), ctx, arg_types(), linear_parameters, return_type(), _called_on_null_input).get();
            } catch (const wasm::exception& e) {
                throw exceptions::invalid_request_exception(format("UDF error: {}", e.what()));
            }
        }));
}

sstring signature_type_name(const data_type& type) {
    return type->is_collection() ? type->cql3_type_name() : type->cql3_type_name_without_frozen();
}

description user_function::describe(with_create_statement with_stmt) const {
    auto maybe_create_statement = std::invoke([&] -> std::optional<managed_string> {
        if (!with_stmt) {
            return std::nullopt;
        }

        fragmented_ostringstream stream;

        auto arg_type_range = _arg_types | std::views::transform(signature_type_name);
        auto arg_range = std::views::zip(_arg_names, arg_type_range)
                | std::views::transform([] (std::tuple<std::string_view, std::string_view> arg) {
                    const auto [name, type] = arg;
                    return seastar::format("{} {}", name, type);
                });

        fmt::format_to(stream.to_iter(), "CREATE FUNCTION {}.{}({})\n"
                "{} ON NULL INPUT\n"
                "RETURNS {}\n"
                "LANGUAGE {}\n"
                "AS $${}$$;",
                cql3::util::maybe_quote(name().keyspace), cql3::util::maybe_quote(name().name), fmt::join(arg_range, ", "),
                _called_on_null_input ? "CALLED" : "RETURNS NULL",
                signature_type_name(_return_type),
                _language,
                _body);

        return std::move(stream).to_managed_string();
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
