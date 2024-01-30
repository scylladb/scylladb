/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "redis/query_processor.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "redis/command_factory.hh"
#include "redis/options.hh"
#include "service_permit.hh"

namespace redis {

distributed<query_processor> _the_query_processor;

query_processor::query_processor(service::storage_proxy& proxy, data_dictionary::database db)
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
