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
 * Modified by ScyllaDB
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

#include <unordered_set>
#include <vector>
#include <boost/signals2.hpp>
#include <boost/signals2/dummy_mutex.hpp>

#include "gms/inet_address.hh"
#include "inet_address_vectors.hh"
#include "gms/versioned_value.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/distributed.hh>
#include "utils/class_registrator.hh"
#include "log.hh"

namespace gms {

enum class application_state;

}

namespace locator {

using snitch_signal_t = boost::signals2::signal_type<void (), boost::signals2::keywords::mutex_type<boost::signals2::dummy_mutex>>::type;
using snitch_signal_slot_t = std::function<future<>()>;
using snitch_signal_connection_t = boost::signals2::scoped_connection;

struct snitch_ptr;

typedef gms::inet_address inet_address;

struct i_endpoint_snitch {
private:
    template <typename... A>
    static future<> init_snitch_obj(
        distributed<snitch_ptr>& snitch_obj, const sstring& snitch_name, A&&... a);
public:
    template <typename... A>
    static future<> create_snitch(const sstring& snitch_name, A&&... a);

    template <typename... A>
    static future<> reset_snitch(const sstring& snitch_name, A&&... a);

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
    virtual inet_address_vector_replica_set get_sorted_list_by_proximity(
        inet_address address,
        inet_address_vector_replica_set& unsorted_address) = 0;

    /**
     * This method will sort the <tt>List</tt> by proximity to the given
     * address.
     */
    virtual void sort_by_proximity(
        inet_address address, inet_address_vector_replica_set& addresses) = 0;

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
    virtual future<> gossiper_starting() {
        _gossip_started = true;
        return gossip_snitch_info({});
    }

    /**
     * Returns whether for a range query doing a query against merged is likely
     * to be faster than 2 sequential queries, one against l1 followed by one
     * against l2.
     */
    virtual bool is_worth_merging_for_range_query(
        inet_address_vector_replica_set& merged,
        inet_address_vector_replica_set& l1,
        inet_address_vector_replica_set& l2) = 0;

    virtual ~i_endpoint_snitch() { assert(_state == snitch_state::stopped); };

    // noop by default
    virtual future<> stop() {
        _state = snitch_state::stopped;
        return make_ready_future<>();
    }

    // noop by default
    virtual future<> pause_io() {
        _state = snitch_state::io_paused;
        return make_ready_future<>();
    };

    // noop by default
    virtual void resume_io() {
        _state = snitch_state::running;
    };

    // noop by default
    virtual future<> start() {
        _state = snitch_state::running;
        return make_ready_future<>();
    }

    // noop by default
    virtual void set_my_dc(const sstring& new_dc) {};
    virtual void set_my_rack(const sstring& new_rack) {};
    virtual void set_prefer_local(bool prefer_local) {};
    virtual void set_local_private_addr(const sstring& addr_str) {};

    // DEPRECATED, DON'T USE!
    // Pass references to services through constructor/function parameters. Don't use globals.
    static distributed<snitch_ptr>& snitch_instance() {
        // FIXME: leaked intentionally to avoid shutdown problems, see #293
        static distributed<snitch_ptr>* snitch_inst = new distributed<snitch_ptr>();

        return *snitch_inst;
    }

    // DEPRECATED, DON'T USE!
    // Pass references to services through constructor/function parameters. Don't use globals.
    static snitch_ptr& get_local_snitch_ptr() {
        return snitch_instance().local();
    }

    void set_snitch_ready() {
        _state = snitch_state::running;
    }

    virtual sstring get_name() const = 0;

    // should be called for production snitches before calling start()
    virtual void set_my_distributed(distributed<snitch_ptr>* d)  {
        //noop by default
    }

    bool local_gossiper_started() {
        return _gossip_started;
    }

    virtual future<> reload_gossiper_state() {
        // noop by default
        return make_ready_future<>();
    }

    virtual future<> gossip_snitch_info(std::list<std::pair<gms::application_state, gms::versioned_value>> info) = 0;

    virtual snitch_signal_connection_t when_reconfigured(snitch_signal_slot_t& slot) {
        // no updates by default
        return snitch_signal_connection_t();
    }

protected:
    static logging::logger& logger() {
        static logging::logger snitch_logger("snitch_logger");
        return snitch_logger;
    }

    static unsigned& io_cpu_id() {
        static unsigned id = 0;
        return id;
    }

protected:
    enum class snitch_state {
        initializing,
        running,
        io_pausing,
        io_paused,
        stopping,
        stopped
    } _state = snitch_state::initializing;
    bool _gossip_started = false;
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

    snitch_ptr& operator=(snitch_ptr&& new_val) {
        _ptr = std::move(new_val._ptr);

        return *this;
    }

    operator bool() const {
        return _ptr ? true : false;
    }

private:
    ptr_type _ptr;
};

/**
 * Initializes the distributed<snitch_ptr> object
 *
 * @note The local snitch objects will remain not start()ed.
 *
 * @param snitch_obj distributed<> object to initialize
 * @param snitch_name name of the snitch class to create
 * @param a snitch constructor arguments
 *
 * @return ready future when the snitch has been successfully created
 */
template <typename... A>
future<> i_endpoint_snitch::init_snitch_obj(
    distributed<snitch_ptr>& snitch_obj, const sstring& snitch_name, A&&... a) {

    // First, create the snitch_ptr objects...
    return snitch_obj.start().then(
        [&snitch_obj, snitch_name = std::move(snitch_name), a = std::make_tuple(std::forward<A>(a)...)] () {
        // ...then, create the snitches...
        return snitch_obj.invoke_on_all(
            [snitch_name, a, &snitch_obj] (snitch_ptr& local_inst) {
            try {
                auto s(std::move(apply([snitch_name] (A&&... a) {
                    return create_object<i_endpoint_snitch>(snitch_name, std::forward<A>(a)...);
                }, std::move(a))));

                s->set_my_distributed(&snitch_obj);
                local_inst = std::move(s);
            } catch (no_such_class& e) {
                logger().error("Can't create snitch {}: not supported", snitch_name);
                throw;
            } catch (...) {
                throw;
            }

            return make_ready_future<>();
        });
    });
}
/**
 * Creates the distributed i_endpoint_snitch::snitch_instane object
 *
 * @param snitch_name name of the snitch class (comes from the cassandra.yaml)
 *
 * @return ready future when the distributed object is ready.
 */
template <typename... A>
future<> i_endpoint_snitch::create_snitch(
    const sstring& snitch_name, A&&... a) {

    // First, create and "start" the distributed snitch object...
    return init_snitch_obj(snitch_instance(), snitch_name, std::forward<A>(a)...).then([snitch_name] {
        // ...and then start each local snitch.
        return snitch_instance().invoke_on_all([] (snitch_ptr& local_inst) {
            return local_inst.start();
        }).handle_exception([snitch_name] (std::exception_ptr eptr) {
            logger().error("Failed to create {}: {}", snitch_name, eptr);
            return stop_snitch().finally([eptr = std::move(eptr)] {
                return make_exception_future<>(eptr);
            });
        });
    });
}

/**
 * Resets the global snitch instance with the new value
 *
 * @param snitch_name Name of a new snitch
 * @param A optional parameters for a new snitch constructor
 *
 * @return ready future when the transition is complete
 *
 * The flow goes as follows:
 *  1) Create a new distributed<snitch_ptr> and initialize it with the new
 *     snitch.
 *  2) Start the new snitches above - this will initialize the snitches objects
 *     and will make them ready to be used.
 *  3) Stop() the current global per-shard snitch objects.
 *  4) Pause the per-shard snitch objects from (1) - this will stop the async
 *     I/O parts of the snitches if any.
 *  5) Assign the per-shard snitch_ptr's from new distributed from (1) to the
 *     global one and update the distributed<> pointer in the new snitch
 *     instances.
 *  6) Start the new snitches.
 *  7) Stop() the temporary distributed<snitch_ptr> from (1).
 */
template <typename... A>
future<> i_endpoint_snitch::reset_snitch(
    const sstring& snitch_name, A&&... a) {
    return seastar::async(
            [snitch_name, a = std::make_tuple(std::forward<A>(a)...)] {

        // (1) create a new snitch
        distributed<snitch_ptr> tmp_snitch;
        try {
            apply([snitch_name,&tmp_snitch](A&& ... a) {
                return init_snitch_obj(tmp_snitch, snitch_name, std::forward<A>(a)...);
            }, std::move(a)).get();

            // (2) start the local instances of the new snitch
            tmp_snitch.invoke_on_all([] (snitch_ptr& local_inst) {
                return local_inst.start();
            }).get();
        } catch (...) {
            tmp_snitch.stop().get();
            throw;
        }

        // If we've got here then we may not fail

        // (3) stop the current snitch instances on all CPUs
        snitch_instance().invoke_on(io_cpu_id(), [] (snitch_ptr& s) {
            // stop the instance on an I/O CPU first
            return s->stop();
        }).get();
        snitch_instance().invoke_on_all([] (snitch_ptr& s) {
            return s->stop();
        }).get();

        //
        // (4) If we've got here - the new snitch has been successfully created
        // and initialized. We may pause its I/O it now and start moving
        // pointers...
        //
        tmp_snitch.invoke_on(io_cpu_id(), [] (snitch_ptr& local_inst) {
            // pause the instance on an I/O CPU first
            return local_inst->pause_io();
        }).get();
        tmp_snitch.invoke_on_all([] (snitch_ptr& local_inst) {
            return local_inst->pause_io();
        }).get();

        //
        // (5) move the pointers - this would ensure the atomicity on a
        // per-shard level (since users are holding snitch_ptr objects only)
        //
        tmp_snitch.invoke_on_all([] (snitch_ptr& local_inst) {
            local_inst->set_my_distributed(&snitch_instance());
            snitch_instance().local() = std::move(local_inst);

            return make_ready_future<>();
        }).get();

        // (6) re-start I/O on the new snitches
        snitch_instance().invoke_on_all([] (snitch_ptr& local_inst) {
            local_inst->resume_io();
        }).get();

        // (7) stop the temporary from (1)
        tmp_snitch.stop().get();
    });
}

class snitch_base : public i_endpoint_snitch {
public:
    //
    // Sons have to implement:
    // virtual sstring get_rack(inet_address endpoint)        = 0;
    // virtual sstring get_datacenter(inet_address endpoint)  = 0;
    //

    virtual inet_address_vector_replica_set get_sorted_list_by_proximity(
        inet_address address,
        inet_address_vector_replica_set& unsorted_address) override;

    virtual void sort_by_proximity(
        inet_address address, inet_address_vector_replica_set& addresses) override;

    virtual int compare_endpoints(
        inet_address& address, inet_address& a1, inet_address& a2) override;

    virtual bool is_worth_merging_for_range_query(
        inet_address_vector_replica_set& merged,
        inet_address_vector_replica_set& l1,
        inet_address_vector_replica_set& l2) override;

    virtual future<> gossip_snitch_info(std::list<std::pair<gms::application_state, gms::versioned_value>> info) override;

private:
    bool has_remote_node(inet_address_vector_replica_set& l);

protected:
    static std::optional<sstring> get_endpoint_info(inet_address endpoint,
                                                    gms::application_state key);
    sstring _my_dc;
    sstring _my_rack;
    bool _prefer_local = false;
};

} // namespace locator
