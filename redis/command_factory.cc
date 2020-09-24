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

future<redis_message> command_factory::create_execute(service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit)
{
    static thread_local std::unordered_map<bytes, std::function<future<redis_message> (service::storage_proxy& proxy, request& req, redis::redis_options& options, service_permit permit)>> _commands =
    { 
        { "ping", commands::ping },
        { "select", commands::select },
        { "get", commands::get },
        { "exists", commands::exists },
        { "ttl", commands::ttl },
        { "strlen", commands::strlen },
        { "set", commands::set },
        { "setex", commands::setex },
        { "del", commands::del },
        { "echo", commands::echo },
        { "lolwut", commands::lolwut },
        { "hget", commands::hget },
        { "hset", commands::hset },
        { "hgetall", commands::hgetall },
        { "hdel", commands::hdel },
        { "hexists", commands::hexists },
    };
    auto&& command = _commands.find(req._command);
    if (command != _commands.end()) {
        return (command->second)(proxy, req, options, permit);
    }
    auto& b = req._command;
    logging.error("receive unknown command = {}", sstring(reinterpret_cast<const char*>(b.data()), b.size()));
    return commands::unknown(proxy, req, options, permit);
}

}
