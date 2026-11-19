/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/commands/command.hh"

#include <algorithm>

#include <seastar/core/coroutine.hh>

#include "exceptions/exceptions.hh"
#include "service/client_state.hh"

namespace cql3::commands {

// Commands can expose data the session has no other access to, so by default they are
// superuser-only, including anonymous sessions (CQL auth can be disabled entirely).
future<> command::check_access(query_processor&, const service::client_state& state, command_args&) const {
    if (!co_await state.has_superuser()) {
        throw exceptions::unauthorized_exception(format("EXECUTE COMMAND {} can only be run by a superuser.", name()));
    }
}

static std::vector<const command*> validated(std::vector<const command*> commands) {
    for (auto* cmd : commands) {
        auto params = cmd->params();
        for (auto it = params.begin(); it != params.end(); ++it) {
            // host_id is reserved for selecting the node a command runs on.
            if (it->name == "host_id" || std::ranges::find(params.begin(), it, it->name, &command_param::name) != it) {
                utils::on_internal_error(fmt::format("command {}: parameter '{}' is reserved or duplicated", cmd->name(), it->name));
            }
        }
    }
    return commands;
}

std::span<const command* const> all_commands() {
    static const std::vector<const command*> commands = validated({});
    return commands;
}

const command* find_command(std::string_view name) {
    auto commands = all_commands();
    auto it = std::ranges::find(commands, name, &command::name);
    return it == commands.end() ? nullptr : *it;
}

} // namespace cql3::commands
