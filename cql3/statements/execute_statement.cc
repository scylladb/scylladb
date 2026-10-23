/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/execute_statement.hh"

#include <seastar/core/coroutine.hh>

#include "cql3/column_specification.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/prepared_statement.hh"
#include "db/system_keyspace.hh"
#include "exceptions/exceptions.hh"
#include "service/client_state.hh"
#include "service/storage_service.hh"
#include "transport/messages/result_message.hh"

namespace cql3 {

namespace statements {

const command* find_command(std::string_view name) {
    static const std::vector<const command*> commands{&toppartitions_command_instance()};
    auto it = std::ranges::find(commands, name, [](const command* c) { return c->name(); });
    return it == commands.end() ? nullptr : *it;
}

namespace raw {

execute_statement::execute_statement(sstring command, std::vector<std::pair<sstring, expr::expression>> args)
    : _command(std::move(command))
    , _args(std::move(args)) {
}

std::unique_ptr<prepared_statement> execute_statement::prepare(data_dictionary::database db, cql_stats& stats, const cql_config& cfg) {
    const command* cmd = find_command(_command);
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
        auto it = std::ranges::find(params, name, &command_param::name);
        if (it == params.end()) {
            throw exceptions::invalid_request_exception(format("Unknown argument '{}' for command '{}'", name, _command));
        }
        auto& slot = args[it - params.begin()];
        if (slot) {
            throw exceptions::invalid_request_exception(format("Argument '{}' given more than once", name));
        }
        slot = expr::prepare_expression(e, db, db::system_keyspace::NAME, nullptr,
                make_column_spec(db::system_keyspace::NAME, cmd->name(), name, it->type));
        expr::verify_no_aggregate_functions(*slot, "EXECUTE argument");
        expr::fill_prepare_context(*slot, ctx);
    }
    auto stmt = ::make_shared<execute_command_statement>(*cmd, std::move(args), ctx.bound_variables_size());
    return std::make_unique<prepared_statement>(audit_info(), std::move(stmt), ctx, std::vector<uint16_t>());
}

audit::statement_category execute_statement::category() const {
    return audit::statement_category::ADMIN;
}

audit::audit_info_ptr execute_statement::audit_info() const {
    return audit::audit::create_audit_info(category(), sstring(), sstring());
}

} // namespace raw

execute_command_statement::execute_command_statement(const command& cmd, std::vector<std::optional<expr::expression>> args, uint32_t bound_terms)
    : cql_statement(&timeout_config::other_timeout)
    , _command(cmd)
    , _args(std::move(args))
    , _bound_terms(bound_terms) {
}

seastar::shared_ptr<const metadata> execute_command_statement::get_result_metadata() const {
    return _command.result_metadata();
}

// Commands can expose data the session has no other access to, so they are superuser-only,
// including anonymous sessions (CQL auth can be disabled entirely).
static future<> require_superuser(const service::client_state& state, std::string_view name) {
    if (!co_await state.has_superuser()) {
        throw exceptions::unauthorized_exception(format("EXECUTE COMMAND {} can only be run by a superuser.", name));
    }
}

future<> execute_command_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return require_superuser(state, _command.name());
}

future<::shared_ptr<cql_transport::messages::result_message>> execute_command_statement::execute(
        query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const {
    // check_access() is skipped on an authorized-prepared-cache hit, and that cache is keyed by user only,
    // so an anonymous session could reuse an entry made via the maintenance socket.
    co_await require_superuser(state.get_client_state(), _command.name());
    // Node shutdown fires before the CQL server drains; the connection abort covers a server
    // stopped on its own (e.g. drain), after its drain timeout.
    abort_source as;
    auto abort = [&as] () noexcept { as.request_abort(); };
    auto node_sub = qp.storage_service().get_abort_source().subscribe(abort);
    auto conn_sub = state.get_client_state().get_abort_source().subscribe(abort);
    if (!node_sub || !conn_sub) {
        as.request_abort();
    }
    auto rs = co_await _command.run(qp, command_args(_args, options, _command.params()), as);
    co_return ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(rs)));
}

} // namespace statements

} // namespace cql3
