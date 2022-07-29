/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
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

class gossiper;
enum class application_state;

}

namespace locator {

using snitch_signal_t = boost::signals2::signal_type<void (), boost::signals2::keywords::mutex_type<boost::signals2::dummy_mutex>>::type;
using snitch_signal_slot_t = std::function<future<>()>;
using snitch_signal_connection_t = boost::signals2::scoped_connection;

struct snitch_ptr;

typedef gms::inet_address inet_address;

struct snitch_config {
    sstring name = "SimpleSnitch";
    sstring properties_file_name = "";
    unsigned io_cpu_id = 0;
    bool broadcast_rpc_address_specified_by_user = false;

    // GCE-specific
    sstring gce_meta_server_url = "";
};

struct i_endpoint_snitch {
public:
    using ptr_type = std::unique_ptr<i_endpoint_snitch>;

    static future<> reset_snitch(snitch_config cfg);

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
     * returns whatever info snitch wants to gossip
     */
    virtual std::list<std::pair<gms::application_state, gms::versioned_value>> get_app_states() const = 0;

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
    virtual void set_my_dc_and_rack(const sstring& new_dc, const sstring& enw_rack) {};
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
    virtual void set_backreference(snitch_ptr& d)  {
        //noop by default
    }

    virtual snitch_signal_connection_t when_reconfigured(snitch_signal_slot_t& slot) {
        // no updates by default
        return snitch_signal_connection_t();
    }

    static logging::logger& logger() {
        static logging::logger snitch_logger("snitch_logger");
        return snitch_logger;
    }

protected:
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
};

struct snitch_ptr : public peering_sharded_service<snitch_ptr> {
    using ptr_type = i_endpoint_snitch::ptr_type;
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

    i_endpoint_snitch& get() {
        return *_ptr;
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

    snitch_ptr(const snitch_config cfg, sharded<gms::gossiper>&);

    gms::gossiper& get_local_gossiper() noexcept { return _gossiper.local(); }
    const gms::gossiper& get_local_gossiper() const noexcept { return _gossiper.local(); }

    sharded<gms::gossiper>& get_gossiper() noexcept { return _gossiper; }
    const sharded<gms::gossiper>& get_gossiper() const noexcept { return _gossiper; }

private:
    ptr_type _ptr;
    sharded<gms::gossiper>& _gossiper;
};

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
inline future<> i_endpoint_snitch::reset_snitch(snitch_config cfg) {
    return seastar::async([cfg = std::move(cfg)] {
        // (1) create a new snitch
        distributed<snitch_ptr> tmp_snitch;
        try {
            tmp_snitch.start(cfg, std::ref(get_local_snitch_ptr().get_gossiper())).get();

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
        snitch_instance().invoke_on_all([] (snitch_ptr& s) {
            return s->stop();
        }).get();

        //
        // (4) If we've got here - the new snitch has been successfully created
        // and initialized. We may pause its I/O it now and start moving
        // pointers...
        //
        tmp_snitch.invoke_on_all([] (snitch_ptr& local_inst) {
            return local_inst->pause_io();
        }).get();

        //
        // (5) move the pointers - this would ensure the atomicity on a
        // per-shard level (since users are holding snitch_ptr objects only)
        //
        tmp_snitch.invoke_on_all([] (snitch_ptr& local_inst) {
            local_inst->set_backreference(snitch_instance().local());
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

    virtual std::list<std::pair<gms::application_state, gms::versioned_value>> get_app_states() const override;

private:
    bool has_remote_node(inet_address_vector_replica_set& l);

protected:
    sstring _my_dc;
    sstring _my_rack;
    bool _prefer_local = false;
};

} // namespace locator
