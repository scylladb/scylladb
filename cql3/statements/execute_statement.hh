/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include <seastar/core/sstring.hh>

#include "cql3/cql_statement.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/query_options.hh"
#include "cql3/result_set.hh"
#include "cql3/statements/raw/parsed_statement.hh"

namespace cql3 {

class query_processor;

namespace statements {

// A named, typed parameter of an EXECUTE COMMAND command. All parameters are optional.
struct command_param {
    std::string_view name;
    data_type type;
};

// Evaluated arguments of one command invocation, indexed like command::params().
class command_args {
    const std::vector<std::optional<expr::expression>>& _args;
    const query_options& _options;
    std::span<const command_param> _params;

public:
    command_args(const std::vector<std::optional<expr::expression>>& args, const query_options& options, std::span<const command_param> params)
        : _args(args), _options(options), _params(params) {
    }

    // nullopt if the argument is absent or evaluates to null (e.g. a marker bound to null).
    template <typename T>
    std::optional<T> get(size_t idx) const {
        if (!_args[idx]) {
            return std::nullopt;
        }
        auto v = expr::evaluate(*_args[idx], _options);
        if (v.is_null()) {
            return std::nullopt;
        }
        return v.view().deserialize<T>(*_params[idx].type);
    }
};

// Describes one EXECUTE COMMAND command: its name, parameters, result shape and implementation.
// Commands are stateless singletons listed in find_command(). run() must return early when `as`
// fires (node or CQL server shutdown) and release whatever it installed.
class command {
public:
    virtual ~command() = default;
    virtual std::string_view name() const = 0;
    virtual std::span<const command_param> params() const = 0;
    virtual seastar::shared_ptr<const metadata> result_metadata() const = 0;
    virtual future<std::unique_ptr<result_set>> run(query_processor& qp, const command_args& args, abort_source& as) const = 0;
};

const command& toppartitions_command_instance();

// nullptr if there is no such command.
const command* find_command(std::string_view name);

namespace raw {

// EXECUTE COMMAND <name> [WITH <param> = <term> AND ...]: runs a built-in command.
class execute_statement : public parsed_statement {
    sstring _command;
    std::vector<std::pair<sstring, expr::expression>> _args;

public:
    execute_statement(sstring command, std::vector<std::pair<sstring, expr::expression>> args);
    std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats, const cql_config& cfg) override;

protected:
    audit::statement_category category() const override;
    audit::audit_info_ptr audit_info() const override;
};

} // namespace raw

class execute_command_statement : public cql_statement {
    const command& _command;
    std::vector<std::optional<expr::expression>> _args;
    uint32_t _bound_terms;

public:
    execute_command_statement(const command& cmd, std::vector<std::optional<expr::expression>> args, uint32_t bound_terms);

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
