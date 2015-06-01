/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 */

#pragma once

#include <unordered_set>
#include <vector>

#include "gms/inet_address.hh"
#include "core/shared_ptr.hh"

namespace locator {

struct i_endpoint_snitch;

typedef gms::inet_address inet_address;
typedef std::unique_ptr<i_endpoint_snitch> snitch_ptr;

template <typename SnitchClass, typename... A>
static future<snitch_ptr> make_snitch(A&&... a);

struct i_endpoint_snitch
{
    /**
     * returns a String representing the rack this endpoint belongs to
     */
    virtual sstring get_rack(inet_address endpoint) = 0;

    /**
     * returns a String representing the datacenter this endpoint belongs to
     */
    virtual sstring get_datacenter(inet_address endpoint) = 0;

    /**
     * returns a new <tt>List</tt> sorted by proximity to the given endpoint
     */
    virtual std::vector<inet_address> get_sorted_list_by_proximity(
        inet_address address,
        std::unordered_set<inet_address>& unsorted_address) = 0;

    /**
     * This method will sort the <tt>List</tt> by proximity to the given
     * address.
     */
    virtual void sort_by_proximity(
        inet_address address, std::vector<inet_address>& addresses) = 0;

    /**
     * compares two endpoints in relation to the target endpoint, returning as
     * Comparator.compare would
     */
    virtual int compare_endpoints(
        inet_address& target, inet_address& a1, inet_address& a2) = 0;

    /**
     * called after Gossiper instance exists immediately before it starts
     * gossiping
     */
    virtual void gossiper_starting() = 0;

    /**
     * Returns whether for a range query doing a query against merged is likely
     * to be faster than 2 sequential queries, one against l1 followed by one
     * against l2.
     */
    virtual bool is_worth_merging_for_range_query(
        std::vector<inet_address>& merged,
        std::vector<inet_address>& l1,
        std::vector<inet_address>& l2) = 0;

    virtual ~i_endpoint_snitch() { assert(_state == snitch_state::stopped); };

    virtual future<> stop() = 0;

    template <typename SnitchClass, typename... A>
    friend future<snitch_ptr> make_snitch(A&&... a);

protected:
    promise<> _snitch_is_ready;
    enum class snitch_state {
        initializing,
        running,
        stopping,
        stopped
    } _state = snitch_state::initializing;
};

template <typename SnitchClass, typename... A>
static future<snitch_ptr> make_snitch(A&&... a) {
    snitch_ptr s(new SnitchClass(std::forward<A>(a)...));

    auto fu = s->_snitch_is_ready.get_future();
    return fu.then_wrapped([s = std::move(s)] (auto&& f) mutable {
        try {
            f.get();

            return make_ready_future<snitch_ptr>(std::move(s));
        } catch (...) {
            auto eptr = std::current_exception();
            auto fu = s->stop();

            return fu.then([eptr, s = std::move(s)] () mutable {
                std::rethrow_exception(eptr);
                // just to make a compiler happy
                return make_ready_future<snitch_ptr>(std::move(s));
            });
        }
    });
}

class snitch_base : public i_endpoint_snitch {
public:
    //
    // Sons have to implement:
    // virtual sstring get_rack(inet_address endpoint)        = 0;
    // virtual sstring get_datacenter(inet_address endpoint)  = 0;
    //

    virtual std::vector<inet_address> get_sorted_list_by_proximity(
        inet_address address,
        std::unordered_set<inet_address>& unsorted_address) override;

    virtual void sort_by_proximity(
        inet_address address, std::vector<inet_address>& addresses) override;

    virtual int compare_endpoints(
        inet_address& address, inet_address& a1, inet_address& a2) override;

    // noop by default
    virtual void gossiper_starting() override {}

    virtual bool is_worth_merging_for_range_query(
        std::vector<inet_address>& merged,
        std::vector<inet_address>& l1,
        std::vector<inet_address>& l2) override;

private:
    bool has_remote_node(std::vector<inet_address>& l);

protected:
    sstring _my_dc;
    sstring _my_rack;
};

} // namespace locator
