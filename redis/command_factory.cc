/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "redis/command_factory.hh"
#include "service/storage_proxy.hh"
#include "redis/commands.hh"
#include "log.hh"

namespace redis {

static logging::logger logging("command_factory");

shared_ptr<abstract_command> command_factory::create(service::storage_proxy& proxy, request&& req)
{
    static thread_local std::unordered_map<bytes, std::function<shared_ptr<abstract_command> (service::storage_proxy& proxy, request&& req)>> _commands =
    { 
        { "ping",  [] (service::storage_proxy& proxy, request&& req) { return commands::ping::prepare(proxy, std::move(req)); } }, 
        { "select",  [] (service::storage_proxy& proxy, request&& req) { return commands::select::prepare(proxy, std::move(req)); } }, 
        { "get",  [] (service::storage_proxy& proxy, request&& req) { return commands::get::prepare(proxy, std::move(req)); } }, 
        { "set",  [] (service::storage_proxy& proxy, request&& req) { return commands::set::prepare(proxy, std::move(req)); } }, 
        { "del",  [] (service::storage_proxy& proxy, request&& req) { return commands::del::prepare(proxy, std::move(req)); } }, 
        { "echo",  [] (service::storage_proxy& proxy, request&& req) { return commands::echo::prepare(proxy, std::move(req)); } },
        { "lolwut", [] (service::storage_proxy& proxy, request&& req) { return commands::lolwut::prepare(proxy, std::move(req)); } },
    };
    auto&& command = _commands.find(req._command);
    if (command != _commands.end()) {
        return (command->second)(proxy, std::move(req));
    }
    auto& b = req._command;
    logging.error("receive unknown command = {}", sstring(reinterpret_cast<const char*>(b.data()), b.size()));
    return commands::unknown::prepare(proxy, std::move(req));
}

}
