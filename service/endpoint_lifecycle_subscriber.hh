/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "gms/inet_address.hh"
#include "locator/host_id.hh"
#include "utils/atomic_vector.hh"

namespace service {

/**
 * Interface on which interested parties can be notified of high level endpoint
 * state changes.
 *
 * Note that while IEndpointStateChangeSubscriber notify about gossip related
 * changes (IEndpointStateChangeSubscriber.onJoin() is called when a node join
 * gossip), this interface allows to be notified about higher level events.
 */
class endpoint_lifecycle_subscriber {
public:
    virtual ~endpoint_lifecycle_subscriber()
    { }

    /**
     * Called when a new node joins the cluster, i.e. either has just been
     * bootstrapped or "instajoins".
     *
     * @param endpoint the newly added endpoint.
     */
    virtual void on_join_cluster(const gms::inet_address& endpoint) = 0;

    /**
     * Called when a new node leave the cluster (decommission or removeToken).
     *
     * @param endpoint the IP of the endpoint that is leaving.
     * @param host_id the host ID of the endpoint that is leaving.
     */
    virtual void on_leave_cluster(const gms::inet_address& endpoint, const locator::host_id& host_id) = 0;

    /**
     * Called when a node is marked UP.
     *
     * @param endpoint the endpoint marked UP.
     */
    virtual void on_up(const gms::inet_address& endpoint) = 0;

    /**
     * Called when a node is marked DOWN.
     *
     * @param endpoint the endpoint marked DOWN.
     */
    virtual void on_down(const gms::inet_address& endpoint) = 0;
};

class endpoint_lifecycle_notifier {
    atomic_vector<endpoint_lifecycle_subscriber*> _subscribers;

public:
    void register_subscriber(endpoint_lifecycle_subscriber* subscriber);
    future<> unregister_subscriber(endpoint_lifecycle_subscriber* subscriber) noexcept;

    future<> notify_down(gms::inet_address endpoint);
    future<> notify_left(gms::inet_address endpoint, locator::host_id host_id);
    future<> notify_up(gms::inet_address endpoint);
    future<> notify_joined(gms::inet_address endpoint);
};

}
