/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <optional>
#include <utility>
#include <vector>

#include <seastar/core/sstring.hh>

#include "cql3/cql_statement.hh"
#include "cql3/expr/expression.hh"
#include "cql3/statements/raw/parsed_statement.hh"

namespace cql3 {

namespace commands {
class command;
}

namespace statements {

namespace raw {

// EXECUTE COMMAND <name> [WITH <param> = <term> AND ...]: runs a built-in command.
class execute_command_statement : public parsed_statement {
    sstring _command;
    std::vector<std::pair<sstring, expr::expression>> _args;

public:
    execute_command_statement(sstring command, std::vector<std::pair<sstring, expr::expression>> args);
    std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats, const cql_config& cfg) override;

protected:
    audit::statement_category category() const override;
    audit::audit_info_ptr audit_info() const override;
};

} // namespace raw

class execute_command_statement : public cql_statement {
    const commands::command& _command;
    // Indexed like the command's params(); nullopt when not given.
    std::vector<std::optional<expr::expression>> _args;
    uint32_t _bound_terms;
    bool _requires_thread;

public:
    execute_command_statement(const commands::command& cmd, std::vector<std::optional<expr::expression>> args, uint32_t bound_terms);

    uint32_t get_bound_terms() const override {
        return _bound_terms;
    }
    bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override {
        return false;
    }
    // Administrative, never issued on a driver's control connection.
    bool should_reclassify_control_connection() const override {
        return true;
    }
    seastar::shared_ptr<const metadata> get_result_metadata() const override;
    future<> check_access(query_processor& qp, const service::client_state& state) const override;
    future<::shared_ptr<cql_transport::messages::result_message>> execute(
            query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const override;
};

} // namespace statements

} // namespace cql3
