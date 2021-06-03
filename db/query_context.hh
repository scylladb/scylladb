/*
 * Copyright (C) 2015-present ScyllaDB
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
#pragma once

#include <memory>
#include <seastar/core/sharded.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "db/timeout_clock.hh"
#include "exceptions/exceptions.hh"
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
        return _qp.local().execute_internal(req, { data_value(std::forward<Args>(args))... });
    }

    template <typename... Args>
    future<::shared_ptr<cql3::untyped_result_set>> execute_cql_with_timeout(sstring req,
            db::timeout_clock::time_point timeout,
            Args&&... args) {
        const db::timeout_clock::time_point now = db::timeout_clock::now();
        const db::timeout_clock::duration d =
            now < timeout ?
                timeout - now :
                // let the `storage_proxy` time out the query down the call chain
                db::timeout_clock::duration::zero();

        struct timeout_context {
            std::unique_ptr<service::client_state> client_state;
            service::query_state query_state;
            timeout_context(db::timeout_clock::duration d)
                    : client_state(std::make_unique<service::client_state>(service::client_state::internal_tag{}, timeout_config{d, d, d, d, d, d, d}))
                    , query_state(*client_state, empty_service_permit())
            {}
        };
        return do_with(timeout_context(d), [this, req = std::move(req), &args...] (auto& tctx) {
            return _qp.local().execute_internal(req,
                cql3::query_options::DEFAULT.get_consistency(),
                tctx.query_state,
                { data_value(std::forward<Args>(args))... },
                true);
        });
    }

    cql3::query_processor& qp() {
        return _qp.local();
    }
};

// This does not have to be thread local, because all cores will share the same context.
extern std::unique_ptr<query_context> qctx;
}
