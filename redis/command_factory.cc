/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
