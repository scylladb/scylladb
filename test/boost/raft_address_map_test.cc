/*
 * Copyright (C) 2021-present ScyllaDB
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

#include <boost/test/unit_test.hpp>
#include <seastar/testing/on_internal_error.hh>
#include <seastar/testing/thread_test_case.hh>
#include <chrono>

#include "raft/raft.hh"
#include "service/raft/raft_address_map.hh"
#include "gms/inet_address.hh"
#include "utils/UUID.hh"

#include <seastar/core/manual_clock.hh>

using namespace raft;
using namespace std::chrono_literals;
using namespace seastar::testing;

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
        // Set + erase regular entry works as expected
        raft_address_map<manual_clock> m;
        m.set(id1, addr1, false);
        auto found = m.find(id1);
        BOOST_CHECK(!!found && *found == addr1);
        m.erase(id1);
        BOOST_CHECK(!m.find(id1));
    }
    {
        // Set expiring + erase works as expected
        raft_address_map<manual_clock> m;
        m.set(id1, addr1, true);
        auto found = m.find(id1);
        BOOST_CHECK(!!found && *found == addr1);
        m.erase(id1);
        BOOST_CHECK(!m.find(id1));
    }
    {
        // Set expiring + wait for expiration works as expected
        raft_address_map<manual_clock> m;
        m.set(id1, addr1, true);
        auto found = m.find(id1);
        BOOST_CHECK(!!found && *found == addr1);
        // The entry stay in the cache for 1 hour
        manual_clock::advance(expiration_time);
        BOOST_CHECK(!m.find(id1));
    }
    {
        // Check that regular entries don't expire
        raft_address_map<manual_clock> m;
        m.set(id1, addr1, false);
        manual_clock::advance(expiration_time);
        BOOST_CHECK(!!m.find(id1) && *m.find(id1) == addr1);
    }
    {
        // Set two expirable entries with different timestamps, check for
        // automatic rearming of expiration timer after the first one expires.
        raft_address_map<manual_clock> m;
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
        raft_address_map<manual_clock> m;
        m.set(id1, addr1, false);
        BOOST_CHECK_THROW(m.set(id1, addr2, false), std::runtime_error);
    }
    {
        // Check that transition from regular to expiring entry works
        raft_address_map<manual_clock> m;
        m.set(id1, addr1, false);
        m.set(id1, addr1, true);
        manual_clock::advance(expiration_time);
        BOOST_CHECK(!m.find(id1));
    }
    {
        // Check that transition from expiring to regular entry works
        raft_address_map<manual_clock> m;
        m.set(id1, addr1, true);
        m.set(id1, addr1, false);
        manual_clock::advance(expiration_time);
        BOOST_CHECK(!!m.find(id1) && *m.find(id1) == addr1);
    }
}
