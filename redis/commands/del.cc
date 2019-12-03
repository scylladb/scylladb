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

#include "redis/commands/del.hh"
#include "seastar/core/shared_ptr.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "service_permit.hh"
#include "types.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "redis/options.hh"
#include "redis/mutation_utils.hh"

namespace redis {

namespace commands {

shared_ptr<abstract_command> del::prepare(service::storage_proxy& proxy, request&& req)
{
    if (req.arguments_size() == 0) {
        throw wrong_number_of_arguments_exception(req._command);
    }
    return seastar::make_shared<del> (std::move(req._command), std::move(req._args));
}

future<redis_message> del::execute(service::storage_proxy& proxy, redis::redis_options& options, service_permit permit)
{
    //FIXME: We should return the count of the actually deleted keys.
    auto size = _keys.size();
    return redis::delete_objects(proxy, options, std::move(_keys), permit).then([size] {
       return redis_message::number(size);
    });
}

}
}
