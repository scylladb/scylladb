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
#include <vector>

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include "cql3/values.hh"
#include "exceptions/exceptions.hh"
#include "schema/schema_fwd.hh"
#include "types/types.hh"
#include "utils/on_internal_error.hh"

namespace service {
class client_state;
}

namespace cql3 {

class query_processor;
class metadata;
class result_set;

namespace commands {

// A named, typed parameter of an EXECUTE COMMAND command.
struct command_param {
    std::string_view name;
    data_type type;
    bool mandatory = false;
};

// Bound arguments of one invocation, indexed like command::params(); null when absent.
struct command_args {
    std::span<const command_param> params;
    std::vector<raw_value> values;
    // Set by check_access() so run() samples the table that was authorized, not a same-named recreation.
    std::optional<table_id> authorized_table;

    template <typename T>
    std::optional<T> get(size_t idx) const {
        if (params[idx].type != data_type_for<T>()) {
            utils::on_internal_error(fmt::format("command parameter '{}' is {}, not the requested type", params[idx].name, params[idx].type->name()));
        }
        if (values[idx].is_null()) {
            return std::nullopt;
        }
        // An empty value (e.g. blobasint(0x)) is not null, but has nothing to deserialize.
        if (!has_empty<T> && values[idx].is_empty_value()) {
            throw exceptions::invalid_request_exception(fmt::format("Argument '{}' must not be empty", params[idx].name));
        }
        return values[idx].view().deserialize<T>(*params[idx].type);
    }
};

// Describes one EXECUTE COMMAND command. Commands are stateless singletons, registered in all_commands().
class command {
public:
    virtual ~command() = default;
    virtual std::string_view name() const = 0;
    virtual std::span<const command_param> params() const = 0;
    virtual seastar::shared_ptr<const metadata> result_metadata() const = 0;
    // Runs on every execution, after the arguments are bound. Superuser-only by default.
    virtual seastar::future<> check_access(query_processor& qp, const service::client_state& state, command_args& args) const;
    // Must return early when `as` fires (shutdown or client disconnect) and release whatever it installed.
    virtual seastar::future<std::unique_ptr<result_set>> run(query_processor& qp, const command_args& args, seastar::abort_source& as) const = 0;
};

std::span<const command* const> all_commands();

// nullptr if there is no such command.
const command* find_command(std::string_view name);

} // namespace commands

} // namespace cql3
