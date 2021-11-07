/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "locator/token_metadata.hh"
#include "message/messaging_service_fwd.hh"
#include "query-request.hh"
#include "replica/database_fwd.hh"

namespace service {

class storage_proxy;

// forward_service is a sharded service responsible for distributing and
// executing aggregation requests across a cluster.
class forward_service : public seastar::peering_sharded_service<forward_service> {
    netw::messaging_service& _messaging;
    service::storage_proxy& _proxy;
    distributed<replica::database>& _db;
    const locator::shared_token_metadata& _shared_token_metadata;

public:
    forward_service(netw::messaging_service& ms, service::storage_proxy& p, distributed<replica::database> &db,
        const locator::shared_token_metadata& stm)
        : _messaging(ms)
        , _proxy(p)
        , _db(db)
        , _shared_token_metadata(stm) {
        init_messaging_service();
    }

    future<> stop();

    // Splits given `forward_request` and distributes execution of resulting
    // subrequests across a cluster.
    future<query::forward_result> dispatch(query::forward_request req);

private:
    // Used to execute a `forward_request` on remote node.
    future<query::forward_result> execute(query::forward_request req);

    locator::token_metadata_ptr get_token_metadata_ptr() const noexcept;

    void init_messaging_service();
    future<> uninit_messaging_service();
};

} // namespace service
