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
#include "core/distributed.hh"
#include "utils/class_registrator.hh"

namespace locator {

struct snitch_ptr;

typedef gms::inet_address inet_address;

struct i_endpoint_snitch {
    template <typename... A>
    static future<> create_snitch(const sstring& snitch_name, A... a);

    static future<> stop_snitch();

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

    // noop by default
    virtual future<> start() {
        _state = snitch_state::running;
        return make_ready_future<>();
    }

    // noop by default
    virtual void set_my_dc(const sstring& new_dc) {};
    virtual void set_my_rack(const sstring& new_rack) {};

    static distributed<snitch_ptr>& snitch_instance() {
        static distributed<snitch_ptr> snitch_inst;

        return snitch_inst;
    }

    static snitch_ptr& get_local_snitch_ptr() {
        return snitch_instance().local();
    }

    void set_snitch_ready() {
        _state = snitch_state::running;
    }

    virtual sstring get_name() const = 0;

protected:
    static unsigned& io_cpu_id() {
        static unsigned id = 0;
        return id;
    }

protected:
    static logging::logger snitch_logger;

    enum class snitch_state {
        initializing,
        running,
        stopping,
        stopped
    } _state = snitch_state::initializing;
};

struct snitch_ptr {
    typedef std::unique_ptr<i_endpoint_snitch> ptr_type;
    future<> stop() {
        if (_ptr) {
            return _ptr->stop();
        } else {
            return make_ready_future<>();
        }
    }

    future<> start() {
        if (_ptr) {
            return _ptr->start();
        } else {
            return make_ready_future<>();
        }
    }

    i_endpoint_snitch* operator->() {
        return _ptr.get();
    }

    snitch_ptr& operator=(ptr_type&& new_val) {
        _ptr = std::move(new_val);

        return *this;
    }

    operator bool() const {
        return _ptr ? true : false;
    }

private:
    ptr_type _ptr;
};

/**
 * Creates the distributed i_endpoint_snitch::snitch_instane object
 *
 * @param snitch_name name of the snitch class (comes from the cassandra.yaml)
 *
 * @return ready future when the distributed object is ready.
 */
template <typename... A>
future<> i_endpoint_snitch::create_snitch(
    const sstring& snitch_name, A... a) {

    // First, create the snitch_ptr objects...
    return snitch_instance().start().then(
        [snitch_name = std::move(snitch_name), a = std::make_tuple(std::forward<A>(a)...)] () {
        // ...then, create the snitches...
        return snitch_instance().invoke_on_all(
            [snitch_name, a] (snitch_ptr& local_inst) {
            try {
                auto s(std::move(apply([snitch_name] (A... a) {
                    return create_object<i_endpoint_snitch>(snitch_name, std::forward<A>(a)...);
                }, std::move(a))));

                local_inst = std::move(s);
            } catch (no_such_class& e) {
                snitch_logger.error("{}", e.what());
                throw;
            } catch (...) {
                throw;
            }
            
            return make_ready_future<>();
        }).then([] {
            // ...and finally - start them.
            return snitch_instance().invoke_on_all([] (snitch_ptr& local_inst) {
                return local_inst.start();
            }).then_wrapped([] (auto&& f) {
                try {
                    f.get();
                    return make_ready_future<>();
                } catch (...) {
                    auto eptr = std::current_exception();

                    return stop_snitch().then([eptr] () {
                        std::rethrow_exception(eptr);
                    });
                }
            });
        });
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
