/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/test/unit_test.hpp>
#include <seastar/testing/on_internal_error.hh>
#include <seastar/testing/thread_test_case.hh>
#include <chrono>

#include "raft/raft.hh"
#include "service/raft/raft_address_map.hh"
#include "gms/inet_address.hh"
#include "utils/UUID.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/util/later.hh>
#include <seastar/util/defer.hh>

using namespace raft;
using namespace service;
using namespace std::chrono_literals;
using namespace seastar::testing;

// Can be used to wait for delivery of messages that were sent to other shards.
future<> ping_shards() {
    if (smp::count == 1) {
        co_return co_await seastar::yield();
    }

    // Submit an empty message to other shards 100 times to account for task reordering in debug mode.
    for (int i = 0; i < 100; ++i) {
        co_await parallel_for_each(boost::irange(0u, smp::count), [] (shard_id s) {
            return smp::submit_to(s, [](){});
        });
    }
}

SEASTAR_THREAD_TEST_CASE(test_raft_address_map_operations) {
    server_id id1{utils::UUID(0, 1)};
    server_id id2{utils::UUID(0, 2)};
    gms::inet_address addr1("127.0.0.1");
    gms::inet_address addr2("127.0.0.2");
    // Expiring entries stay in the cache for 1 hour, so take a bit larger value
    // in order to trigger cleanup
    auto expiration_time = 1h + 1s;

    using seastar::manual_clock;

    {
        // Set regular entry + set_expiring_flag works as expected
        sharded<raft_address_map<manual_clock>> m_svc;
        m_svc.start().get();
        auto stop_map = defer([&m_svc] { m_svc.stop().get(); });
        auto& m = m_svc.local();

        m.set(id1, addr1, false);
        {
            const auto found = m.find(id1);
            BOOST_CHECK(!!found && *found == addr1);
        }
        {
            const auto found = m.set_expiring_flag(id1);
            BOOST_CHECK(!!found && *found == addr1);
        }
        manual_clock::advance(expiration_time);
        BOOST_CHECK(!m.find(id1));
    }
    {
        // Set expiring entry + set_expiring_flag works as expected
        sharded<raft_address_map<manual_clock>> m_svc;
        m_svc.start().get();
        auto stop_map = defer([&m_svc] { m_svc.stop().get(); });
        auto& m = m_svc.local();

        m.set(id1, addr1, true);
        {
            const auto found = m.find(id1);
            BOOST_CHECK(!!found && *found == addr1);
        }
        {
            const auto found = m.set_expiring_flag(id1);
            BOOST_CHECK(!!found && *found == addr1);
        }
        manual_clock::advance(expiration_time);
        BOOST_CHECK(!m.find(id1));
    }
    {
        // Set expiring + wait for expiration works as expected
        sharded<raft_address_map<manual_clock>> m_svc;
        m_svc.start().get();
        auto stop_map = defer([&m_svc] { m_svc.stop().get(); });
        auto& m = m_svc.local();

        m.set(id1, addr1, true);
        auto found = m.find(id1);
        BOOST_CHECK(!!found && *found == addr1);
        // The entry stay in the cache for 1 hour
        manual_clock::advance(expiration_time);
        BOOST_CHECK(!m.find(id1));
    }
    {
        // Check that regular entries don't expire
        sharded<raft_address_map<manual_clock>> m_svc;
        m_svc.start().get();
        auto stop_map = defer([&m_svc] { m_svc.stop().get(); });
        auto& m = m_svc.local();

        m.set(id1, addr1, false);
        manual_clock::advance(expiration_time);
        BOOST_CHECK(!!m.find(id1) && *m.find(id1) == addr1);
    }
    {
        // Set two expirable entries with different timestamps, check for
        // automatic rearming of expiration timer after the first one expires.
        sharded<raft_address_map<manual_clock>> m_svc;
        m_svc.start().get();
        auto stop_map = defer([&m_svc] { m_svc.stop().get(); });
        auto& m = m_svc.local();

        m.set(id1, addr1, true);
        manual_clock::advance(30min);
        m.set(id2, addr2, true);
        // Here the expiration timer will rearm itself automatically since id2
        // hasn't expired yet and need to be collected some time later
        manual_clock::advance(30min + 1s);
        BOOST_CHECK(!m.find(id1));
        BOOST_CHECK(!!m.find(id2) && *m.find(id2) == addr2);
        // wait for another cleanup period
        manual_clock::advance(expiration_time);
        BOOST_CHECK(!m.find(id2));
    }
    {
        // Throw on re-mapping address for the same id
        scoped_no_abort_on_internal_error abort_guard;
        sharded<raft_address_map<manual_clock>> m_svc;
        m_svc.start().get();
        auto stop_map = defer([&m_svc] { m_svc.stop().get(); });
        auto& m = m_svc.local();

        m.set(id1, addr1, false);
        BOOST_CHECK_THROW(m.set(id1, addr2, false), std::runtime_error);
    }
    {
        // Check that transition from regular to expiring entry is not possible
        sharded<raft_address_map<manual_clock>> m_svc;
        m_svc.start().get();
        auto stop_map = defer([&m_svc] { m_svc.stop().get(); });
        auto& m = m_svc.local();

        m.set(id1, addr1, false);
        m.set(id1, addr1, true);
        manual_clock::advance(expiration_time);
        BOOST_CHECK(m.find(id1));
    }
    {
        // Check that transition from expiring to regular entry works
        sharded<raft_address_map<manual_clock>> m_svc;
        m_svc.start().get();
        auto stop_map = defer([&m_svc] { m_svc.stop().get(); });
        auto& m = m_svc.local();

        m.set(id1, addr1, true);
        m.set(id1, addr1, false);
        manual_clock::advance(expiration_time);
        BOOST_CHECK(!!m.find(id1) && *m.find(id1) == addr1);
    }
}

SEASTAR_THREAD_TEST_CASE(test_raft_address_map_replication) {
    if (smp::count < 2) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    static const server_id id1{utils::UUID(0, 1)};
    static const server_id id2{utils::UUID(0, 2)};
    static const gms::inet_address addr1("127.0.0.1");
    static const gms::inet_address addr2("127.0.0.2");
    // Expiring entries stay in the cache for 1 hour, so take a bit larger value
    // in order to trigger cleanup
    static const auto expiration_time = 1h + 1s;

    using seastar::manual_clock;

    {
        sharded<raft_address_map<manual_clock>> m_svc;
        m_svc.start().get();
        auto stop_map = defer([&m_svc] { m_svc.stop().get(); });
        auto& m = m_svc.local();

        // Replicate non-expiring entry, ensure it doesn't expire on the other shard
        m.set(id1, addr1, false);
        ping_shards().get();
        m_svc.invoke_on(1, [] (raft_address_map<manual_clock>& m) {
            BOOST_CHECK(m.find(id1) && *m.find(id1) == addr1);
            manual_clock::advance(expiration_time);
            BOOST_CHECK(m.find(id1) && *m.find(id1) == addr1);
        }).get();

        // Set it to expiring, ensure it expires on both shards
        m.set_expiring_flag(id1);
        ping_shards().get();
        m_svc.invoke_on(1, [] (raft_address_map<manual_clock>& m) {
            BOOST_CHECK(m.find(id1) && *m.find(id1) == addr1);
            manual_clock::advance(expiration_time);
            BOOST_CHECK(!m.find(id1));
        }).get();
        ping_shards().get(); // so this shard notices the clock advance
        BOOST_CHECK(!m.find(id1));

        // Expiring entries are not replicated
        m.set(id1, addr1, true);
        ping_shards().get();
        m_svc.invoke_on(1, [] (raft_address_map<manual_clock>& m) {
            BOOST_CHECK(!m.find(id1));
        }).get();

        // Can't add non-expiring entries on shard other than 0
        m_svc.invoke_on(1, [] (raft_address_map<manual_clock>& m) {
            scoped_no_abort_on_internal_error abort_guard;
            BOOST_CHECK_THROW(m.set(id2, addr2, false), std::runtime_error);
        }).get();

        // Can add expiring entries on shard other than 0 - and they indeed expire
        m_svc.invoke_on(1, [] (raft_address_map<manual_clock>& m) {
            m.set(id2, addr2, true);
            BOOST_CHECK(m.find(id2) && *m.find(id2) == addr2);
            manual_clock::advance(expiration_time);
            BOOST_CHECK(!m.find(id2));
        }).get();


        // Add entry on two shards, make it non-expiring on shard 0,
        // the non-expiration must be replicated
        m_svc.invoke_on(1, [] (raft_address_map<manual_clock>& m) {
            m.set(id2, addr2, true);
        }).get();
        m.set(id2, addr2, false);
        ping_shards().get();
        m_svc.invoke_on(1, [] (raft_address_map<manual_clock>& m) {
            manual_clock::advance(expiration_time);
            BOOST_CHECK(m.find(id2) && *m.find(id2) == addr2);
        }).get();

        // Cannot set it to expiring on shard 1
        m_svc.invoke_on(1, [] (raft_address_map<manual_clock>& m) {
            scoped_no_abort_on_internal_error abort_guard;
            BOOST_CHECK_THROW(m.set_expiring_flag(id2), std::runtime_error);
        }).get();
    }
}
