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

#include "redis/commands/get.hh"
#include "seastar/core/shared_ptr.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "types.hh"
#include "service_permit.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "redis/options.hh"
#include "redis/query_utils.hh"

namespace redis {

namespace commands {

shared_ptr<abstract_command> get::prepare(service::storage_proxy& proxy, request&& req)
{
    if (req.arguments_size() != 1) {
        throw wrong_arguments_exception(1, req.arguments_size(), req._command);
    }
    return seastar::make_shared<get> (std::move(req._command), std::move(req._args[0]));
}

future<redis_message> get::execute(service::storage_proxy& proxy, redis::redis_options& options, service_permit permit)
{
    return redis::read_strings(proxy, options, _key, permit).then([] (auto result) {
        if (result->has_result()) {
            return redis_message::make_strings_result(std::move(result->result()));
        }
        // return nil string if key does not exist
        return redis_message::nil();
    });
}

}
}
