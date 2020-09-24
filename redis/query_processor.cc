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

#include "redis/query_processor.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "redis/command_factory.hh"
#include "timeout_config.hh"
#include "redis/options.hh"
#include "service_permit.hh"

namespace redis {

distributed<query_processor> _the_query_processor;

query_processor::query_processor(service::storage_proxy& proxy, distributed<database>& db)
        : _proxy(proxy)
        , _db(db)
{
}

query_processor::~query_processor() {
}

future<> query_processor::start() {
    return make_ready_future<>();
}

future<> query_processor::stop() {
    // wait for all running command finished.
    return _pending_command_gate.close().then([] {
        return make_ready_future<>();
    });
}

future<redis_message> query_processor::process(request&& req, redis::redis_options& opts, service_permit permit) {
    return do_with(std::move(req), [this, &opts, permit] (auto& req) {
        return with_gate(_pending_command_gate, [this, &req, &opts, permit] () mutable {
            return command_factory::create_execute(_proxy, req, opts, permit);
        });
    });
}

}
