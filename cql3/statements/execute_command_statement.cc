/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/execute_command_statement.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>

#include "auth/resource.hh"
#include "cql3/column_specification.hh"
#include "cql3/commands/command.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/function.hh"
#include "cql3/query_processor.hh"
#include "cql3/result_set.hh"
#include "cql3/statements/prepared_statement.hh"
#include "db/system_keyspace.hh"
#include "exceptions/exceptions.hh"
#include "service/client_state.hh"
#include "service/storage_service.hh"
#include "transport/messages/result_message.hh"

namespace cql3 {

namespace statements {

namespace raw {

execute_command_statement::execute_command_statement(sstring command, std::vector<std::pair<sstring, expr::expression>> args)
    : _command(std::move(command))
    , _args(std::move(args)) {
}

std::unique_ptr<prepared_statement> execute_command_statement::prepare(data_dictionary::database db, cql_stats& stats, const cql_config& cfg) {
    const commands::command* cmd = commands::find_command(_command);
    if (!cmd) {
        throw exceptions::invalid_request_exception(format("Unknown command '{}'", _command));
    }
    auto params = cmd->params();
    std::vector<std::optional<expr::expression>> args(params.size());
    prepare_context& ctx = get_prepare_context();
    for (auto& [name, e] : _args) {
        // Reserved for running a command on another node; commands run on the receiving node.
        if (name == "host_id") {
            throw exceptions::invalid_request_exception("Argument 'host_id' is not supported yet: commands run on the receiving node");
        }
        auto it = std::ranges::find(params, name, &commands::command_param::name);
        if (it == params.end()) {
            throw exceptions::invalid_request_exception(format("Unknown argument '{}' for command '{}'", name, _command));
        }
        auto& slot = args[it - params.begin()];
        if (slot) {
            throw exceptions::invalid_request_exception(format("Argument '{}' given more than once", name));
        }
        slot = expr::prepare_expression(e, db, db::system_keyspace::NAME, nullptr,
                make_column_spec(db::system_keyspace::NAME, cmd->name(), name, it->type));
        expr::fill_prepare_context(*slot, ctx);
    }
    for (size_t i = 0; i < params.size(); ++i) {
        if (params[i].mandatory && !args[i]) {
            throw exceptions::invalid_request_exception(format("Argument '{}' is mandatory for command '{}'", params[i].name, _command));
        }
    }
    // Name the table in the audit log only when both names are literals: a keyspace alone
    // would hide the entry from table-scoped auditing, which logs entries with no keyspace.
    auto literal = [&] (std::string_view name) -> std::optional<sstring> {
        auto it = std::ranges::find(params, name, &commands::command_param::name);
        auto* c = it != params.end() && args[it - params.begin()] ? expr::as_if<expr::constant>(&*args[it - params.begin()]) : nullptr;
        if (!c || c->type != utf8_type || c->value.is_null()) {
            return std::nullopt;
        }
        return c->value.view().deserialize<sstring>(*utf8_type);
    };
    auto ks = literal("keyspace_name");
    auto cf = literal("table_name");
    auto audit = ks && cf ? audit::audit::create_audit_info(category(), *ks, *cf) : audit_info();
    auto stmt = ::make_shared<statements::execute_command_statement>(*cmd, std::move(args), ctx.bound_variables_size());
    return std::make_unique<prepared_statement>(std::move(audit), std::move(stmt), ctx, std::vector<uint16_t>());
}

audit::statement_category execute_command_statement::category() const {
    return audit::statement_category::ADMIN;
}

audit::audit_info_ptr execute_command_statement::audit_info() const {
    return audit::audit::create_audit_info(category(), sstring(), sstring());
}

} // namespace raw

template <typename Func>
static void for_each_function(const std::vector<std::optional<expr::expression>>& args, Func func) {
    for (auto& a : args) {
        if (a) {
            expr::for_each_expression<expr::function_call>(*a, [&] (const expr::function_call& fc) {
                func(*std::get<shared_ptr<functions::function>>(fc.func));
            });
        }
    }
}

execute_command_statement::execute_command_statement(const commands::command& cmd, std::vector<std::optional<expr::expression>> args, uint32_t bound_terms)
    : cql_statement(&timeout_config::other_timeout)
    , _command(cmd)
    , _args(std::move(args))
    , _bound_terms(bound_terms)
    , _requires_thread(false) {
    for_each_function(_args, [this] (const functions::function& f) {
        _requires_thread |= f.requires_thread();
    });
}

seastar::shared_ptr<const metadata> execute_command_statement::get_result_metadata() const {
    return _command.result_metadata();
}

// The command's own check needs the bound arguments and check_access() is skipped on an
// authorized-prepared-cache hit, so it runs in execute(); only UDF permissions are checked here.
future<> execute_command_statement::check_access(query_processor& qp, const service::client_state& state) const {
    std::vector<std::pair<sstring, sstring>> udfs;
    for_each_function(_args, [&] (const functions::function& f) {
        if (!f.is_native()) {
            udfs.emplace_back(f.name().keyspace, auth::encode_signature(f.name().name, f.arg_types()));
        }
    });
    for (auto& [ks, signature] : udfs) {
        co_await state.has_function_access(ks, signature, auth::permission::EXECUTE);
    }
}

future<::shared_ptr<cql_transport::messages::result_message>> execute_command_statement::execute(
        query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const {
    // Evaluated once, so what check_access() authorizes is what run() uses.
    auto evaluate_args = [&] {
        commands::command_args args{.params = _command.params()};
        for (auto& a : _args) {
            args.values.push_back(a ? expr::evaluate(*a, options) : raw_value::make_null());
        }
        return args;
    };
    auto args = _requires_thread ? co_await seastar::async(evaluate_args) : evaluate_args();
    co_await _command.check_access(qp, state.get_client_state(), args);
    for (size_t i = 0; i < args.params.size(); ++i) {
        if (args.params[i].mandatory && args.values[i].is_null()) {
            throw exceptions::invalid_request_exception(format("Argument '{}' must not be null", args.params[i].name));
        }
    }
    // Abort on node shutdown, CQL server shutdown, or the client closing its connection
    // (a driver gives up long before the longest run ends).
    abort_source as;
    auto abort = [&as] () noexcept { as.request_abort(); };
    auto& cs = state.get_client_state();
    std::vector<optimized_optional<abort_source::subscription>> subs;
    subs.reserve(3);
    // Internal client states have no server or connection abort source.
    for (auto* src : {&qp.storage_service().get_abort_source(), cs.get_abort_source_ptr(), cs.get_connection_abort_source_ptr()}) {
        if (src && !subs.emplace_back(src->subscribe(abort))) {
            as.request_abort();
        }
    }
    auto rs = co_await _command.run(qp, cs, args, as);
    co_return ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(rs)));
}

} // namespace statements

} // namespace cql3
