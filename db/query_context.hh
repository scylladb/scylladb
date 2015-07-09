/*
 * Copyright 2015 Cloudius Systems
 */
#pragma once

#include <memory>
#include "core/sharded.hh"
#include "core/future.hh"
#include "cql3/query_processor.hh"

class database;

namespace service {
class storage_proxy;
}


namespace db {
struct query_context {
    distributed<database>& _db;
    distributed<cql3::query_processor>& _qp;
    query_context(distributed<database>& db, distributed<cql3::query_processor>& qp) : _db(db), _qp(qp) {}

    template <typename... Args>
    future<::shared_ptr<cql3::untyped_result_set>> execute_cql(sstring text, sstring cf, Args&&... args) {
        // FIXME: Would be better not to use sprint here.
        sstring req = sprint(text, cf);
        return this->_qp.local().execute_internal(req, { boost::any(std::forward<Args>(args))... });
    }
    database& db() {
        return _db.local();
    }

    service::storage_proxy& proxy() {
        return _qp.local().proxy().local();
    }

    api::timestamp_type next_timestamp() {
        return _qp.local().next_timestamp();
    }
};

// This does not have to be thread local, because all cores will share the same context.
extern std::unique_ptr<query_context> qctx;

// Sometimes we are not concerned about system tables at all - for instance, when we are testing. In those cases, just pretend
// we executed the query, and return an empty result
template <typename... Args>
static future<::shared_ptr<cql3::untyped_result_set>> execute_cql(sstring text, Args&&... args) {
    if (qctx) {
        return qctx->execute_cql(text, std::forward<Args>(args)...);
    }
    return make_ready_future<shared_ptr<cql3::untyped_result_set>>(::make_shared<cql3::untyped_result_set>(cql3::untyped_result_set::make_empty()));
}
}
