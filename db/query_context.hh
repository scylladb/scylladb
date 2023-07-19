/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <memory>
#include <seastar/core/sharded.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "db/timeout_clock.hh"
#include "timeout_config.hh"

namespace service {
class storage_proxy;
}


namespace db {
struct query_context {
    distributed<cql3::query_processor>& _qp;
    query_context(distributed<cql3::query_processor>& qp) : _qp(qp) {}

    template <typename... Args>
    future<::shared_ptr<cql3::untyped_result_set>> execute_cql(sstring req, Args&&... args) {
        return _qp.local().execute_internal(req, { data_value(std::forward<Args>(args))... }, cql3::query_processor::cache_internal::yes);
    }

    cql3::query_processor& qp() {
        return _qp.local();
    }
};

// This does not have to be thread local, because all cores will share the same context.
extern std::unique_ptr<query_context> qctx;
}
