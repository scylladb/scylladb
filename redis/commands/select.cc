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

#include "redis/commands/select.hh"
#include "seastar/core/shared_ptr.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "types.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "redis/options.hh"
#include "service_permit.hh"
#include <string>

namespace redis {

namespace commands {

shared_ptr<abstract_command> select::prepare(service::storage_proxy& proxy, request&& req)
{
    if (req.arguments_size() != 1) {
        throw wrong_arguments_exception(1, req.arguments_size(), req._command);
    }
    long index = -1;
    try {
        index = std::stol(std::string(reinterpret_cast<const char*>(req._args[0].data()), req._args[0].size()));
    }
    catch (...) {
        throw invalid_db_index_exception();
    }
    return seastar::make_shared<select> (std::move(req._command), index);
}

future<redis_message> select::execute(service::storage_proxy&, redis::redis_options& options, service_permit)
{
    if (_index < 0 || static_cast<size_t>(_index) >= options.get_total_redis_db_count()) {
        throw invalid_db_index_exception();
    }
    options.set_keyspace_name(sprint("REDIS_%zu", static_cast<size_t>(_index)));
    return redis_message::ok();
}

}
}
