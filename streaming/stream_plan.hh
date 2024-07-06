/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "utils/UUID_gen.hh"
#include <seastar/core/sstring.hh>
#include "gms/inet_address.hh"
#include "query-request.hh"
#include "streaming/stream_fwd.hh"
#include "streaming/stream_coordinator.hh"
#include "streaming/stream_detail.hh"
#include "streaming/stream_reason.hh"
#include <vector>

namespace streaming {

/**
 * {@link StreamPlan} is a helper class that builds StreamOperation of given configuration.
 *
 * This is the class you want to use for building streaming plan and starting streaming.
 */
class stream_plan {
private:
    using inet_address = gms::inet_address;
    using token = dht::token;
    stream_manager& _mgr;
    plan_id _plan_id;
    sstring _description;
    stream_reason _reason;
    service::frozen_topology_guard _topo_guard;
    std::vector<stream_event_handler*> _handlers;
    shared_ptr<stream_coordinator> _coordinator;
    bool _range_added = false;
    bool _aborted = false;
public:

    /**
     * Start building stream plan.
     *
     * @param description Stream type that describes this StreamPlan
     */
    stream_plan(stream_manager& mgr, sstring description, stream_reason reason = stream_reason::unspecified,
                service::frozen_topology_guard topo_guard = {})
        : _mgr(mgr)
        , _plan_id(plan_id{utils::UUID_gen::get_time_UUID()})
        , _description(description)
        , _reason(reason)
        , _topo_guard(topo_guard)
        , _coordinator(make_shared<stream_coordinator>())
    {
    }

    /**
     * Request data in {@code keyspace} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param connecting Actual connecting address for the endpoint
     * @param keyspace name of keyspace
     * @param ranges ranges to fetch
     * @return this object for chaining
     */
    stream_plan& request_ranges(inet_address from, sstring keyspace, dht::token_range_vector ranges);

    /**
     * Request data in {@code columnFamilies} under {@code keyspace} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param connecting Actual connecting address for the endpoint
     * @param keyspace name of keyspace
     * @param ranges ranges to fetch
     * @param columnFamilies specific column families
     * @return this object for chaining
     */
    stream_plan& request_ranges(inet_address from, sstring keyspace, dht::token_range_vector ranges, std::vector<sstring> column_families);

    /**
     * Add transfer task to send data of specific keyspace and ranges.
     *
     * @param to endpoint address of receiver
     * @param connecting Actual connecting address of the endpoint
     * @param keyspace name of keyspace
     * @param ranges ranges to send
     * @return this object for chaining
     */
    stream_plan& transfer_ranges(inet_address to, sstring keyspace, dht::token_range_vector ranges);

    /**
     * Add transfer task to send data of specific {@code columnFamilies} under {@code keyspace} and {@code ranges}.
     *
     * @param to endpoint address of receiver
     * @param connecting Actual connecting address of the endpoint
     * @param keyspace name of keyspace
     * @param ranges ranges to send
     * @param columnFamilies specific column families
     * @return this object for chaining
     */
    stream_plan& transfer_ranges(inet_address to, sstring keyspace, dht::token_range_vector ranges, std::vector<sstring> column_families);

    stream_plan& listeners(std::vector<stream_event_handler*> handlers);
public:
    /**
     * @return true if this plan has no plan to execute
     */
    bool is_empty() const {
        return !_coordinator->has_active_sessions();
    }

    /**
     * Execute this {@link StreamPlan} asynchronously.
     *
     * @return Future {@link StreamState} that you can use to listen on progress of streaming.
     */
    future<stream_state> execute();

    void abort() noexcept;
    void do_abort();
};

} // namespace streaming
