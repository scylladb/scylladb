/*
 * Copyright (C) 2018 ScyllaDB
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

#include "tests/test-utils.hh"

#include <seastar/core/thread.hh>

#include "db/view/row_locking.hh"
#include "schema_builder.hh"

static row_locker::stats row_locker_stats;

static schema_ptr make_schema()
{
    return schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("ck", bytes_type, column_kind::clustering_key)
            .with_column("s", bytes_type, column_kind::static_column)
            .with_column("r", bytes_type)
            .build();
}

dht::decorated_key make_pk(const schema_ptr& s, const sstring& pk) {
    return dht::global_partitioner().decorate_key(*s,
            partition_key::from_single_value(*s, to_bytes(pk)));
}

clustering_key_prefix make_ck(const schema_ptr&s, const sstring& ck) {
    return clustering_key::from_single_value(*s, to_bytes(ck));
}

// Tests for locks that succeed without blocking:
// Note that these test doesn't check any conditions. Not crashing and
// not hanging is a success :-)

// Test that we can lock a row with an exclusive lock and unlock it.
// Check that we can do that again (so after releasing the lock, the row is
// again unlocked).
// Also check for locking entire partition, and for locking different rows
// in the same partition.
SEASTAR_TEST_CASE(test_nonblock_exclusive) {
    return seastar::async([&] {
        auto s = make_schema();
        row_locker rl(s);
        auto pk = make_pk(s, "pk1");
        auto ck = make_ck(s, "ck1") ;
        auto lock = rl.lock_ck(pk, ck, true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        auto ignore = [] (auto) { };
        // move out the lock object, thereby releasing the lock
        ignore(std::move(lock));
        // after we released the lock, we can take it again.
        lock = rl.lock_ck(pk, ck, true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        ignore(std::move(lock));
        // now do the same, but locking an entire partition. Should
        // be fine after we unlocked the row.
        lock = rl.lock_pk(pk, true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        ignore(std::move(lock));
        lock = rl.lock_pk(pk, true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        ignore(std::move(lock));
        // After we unlock the partition, we can lock the row
        lock = rl.lock_ck(pk, ck, true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        ignore(std::move(lock));
        // Check that we can hold an exclusive lock for two rows in the
        // same partition, and it doesn't hang.
        auto ck2 = make_ck(s, "ck2") ;
        lock = rl.lock_ck(pk, ck, true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        auto lock2 = rl.lock_ck(pk, ck2, true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        ignore(std::move(lock));
        ignore(std::move(lock2));
        BOOST_REQUIRE(rl.empty() == true);
    });
}

// Various tests for shared locks which do not need to block:
// Test that we can lock with a shared lock multiple times.
// Test that we can share-lock a partition and one of its row (the row can be
// locked with either exclusive or shared lock).
SEASTAR_TEST_CASE(test_nonblock_shared) {
    return seastar::async([&] {
        auto s = make_schema();
        row_locker rl(s);
        auto pk = make_pk(s, "pk1");
        auto ck = make_ck(s, "ck1") ;
        // Check that we can lock the same row multiple times with a shared lock:
        auto lock1 = rl.lock_ck(pk, ck, false, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        auto lock2 = rl.lock_ck(pk, ck, false, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        auto lock3 = rl.lock_ck(pk, ck, false, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        auto ignore = [] (auto) { };
        ignore(std::move(lock1));
        ignore(std::move(lock2));
        ignore(std::move(lock3));
        // Check that after unlocking, we can lock again. Also for exclusive lock:
        lock1 = rl.lock_ck(pk, ck, false, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        ignore(std::move(lock1));
        lock1 = rl.lock_ck(pk, ck, true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        ignore(std::move(lock1));
        // Same test but for the partition lock level
        lock1 = rl.lock_pk(pk, false, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        lock2 = rl.lock_pk(pk, false, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        lock3 = rl.lock_pk(pk, false, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        ignore(std::move(lock1));
        ignore(std::move(lock2));
        ignore(std::move(lock3));
        lock1 = rl.lock_pk(pk, false, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        ignore(std::move(lock1));
        lock1 = rl.lock_pk(pk, true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        ignore(std::move(lock1));
        // Check that we can hold a shared lock for a partition and a row
        // in it concurrently.
        lock1 = rl.lock_ck(pk, ck, false, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        lock2 = rl.lock_pk(pk, false, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        ignore(std::move(lock1));
        ignore(std::move(lock2));
        // Check that the above is fine also if the row lock is exclusive
        // (the "exclusivity" is only for the row).
        lock1 = rl.lock_ck(pk, ck, true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        lock2 = rl.lock_pk(pk, false, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        ignore(std::move(lock1));
        ignore(std::move(lock2));
        BOOST_REQUIRE(rl.empty() == true);
    });
}

// Test adding a lot of locks on different rows, and different partitions,
// concurrently (the previous tests only tried one or two concurrent locks,
// see we're not limited to that).
SEASTAR_TEST_CASE(test_nonblock_many) {
    return seastar::async([&] {
        auto s = make_schema();
        row_locker rl(s);
        std::vector<row_locker::lock_holder> locks;
        constexpr int N = 100;
        constexpr int M = 100;
        for (int i = 0; i < N; i++) {
            if (i % 2) {
                // lock the entire partition
                auto lock = rl.lock_pk(make_pk(s, to_sstring(i)), true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
                if (i % 4) {
                    // drop half of locks immediately, half kept until end.
                    locks.push_back(std::move(lock));
                }
            } else {
                // lock M rows, drop half of the locks immediately, keep half
                // until the end.
                for (int j = 0; j < M; j++) {
                    auto lock = rl.lock_ck(make_pk(s, to_sstring(i)), make_ck(s, to_sstring(j)), true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
                    if (j % 2) {
                        locks.push_back(std::move(lock));
                    }
                }
            }
        }
        // drop all the locks still held, now
        auto ignore = [] (auto) { };
        ignore(std::move(locks));
    });
}

static schema_ptr make_alternative_schema()
{
    return schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("ck", bytes_type, column_kind::clustering_key)
            .with_column("s0", bytes_type, column_kind::static_column)
            .with_column("s1", bytes_type, column_kind::static_column)
            .with_column("r", bytes_type)
            .with_column("r2", bytes_type)
            .build();
}

// Test schema change and upgrade (nonblocking test)
SEASTAR_TEST_CASE(test_nonblock_upgrade) {
    return seastar::async([&] {
        auto s = make_schema();
        auto s2 = make_alternative_schema();
        row_locker rl(s);
        auto lock = rl.lock_ck(make_pk(s, "pk1"), make_ck(s, "ck1"), true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        auto ignore = [] (auto) { };
        ignore(std::move(lock));
        rl.upgrade(s2);
        // verify that the row_locker does not not keep a reference to s any
        // more, so the only remaining reference is ours.
        BOOST_REQUIRE(s.use_count() == 1);
        lock = rl.lock_ck(make_pk(s2, "pk1"), make_ck(s2, "ck1"), true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        ignore(std::move(lock));
        // Same test, but upgrade the schema while a lock is still taken
        lock = rl.lock_ck(make_pk(s2, "pk1"), make_ck(s2, "ck1"), true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        rl.upgrade(s);
        BOOST_REQUIRE(s2.use_count() == 1);
        ignore(std::move(lock));
        lock = rl.lock_ck(make_pk(s, "pk1"), make_ck(s, "ck1"), true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        ignore(std::move(lock));
        BOOST_REQUIRE(rl.empty() == true);
    });
}

// Test for blocking cases of row_locker, e.g., trying to take the same lock
// twice with an exclusive lock, trying an exclusive lock together with a
// shared lock, trying to exclusively lock a partition while any lock is
// taken on a row, etc.

// Trying to lock the same row a second time with an exclusive lock should
// block until the first lock is released.
SEASTAR_TEST_CASE(test_block_exclusive_twice_row) {
    return seastar::async([&] {
        auto s = make_schema();
        row_locker rl(s);
        auto pk = make_pk(s, "pk1");
        auto ck = make_ck(s, "ck1") ;
        auto lock = rl.lock_ck(pk, ck, true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        // If we try to lock again *cannot* be ready now. It will
        // become ready (and get0() won't hang) when we drop
        // the first lock
        auto flock1 = rl.lock_ck(pk, ck, true, db::timeout_clock::time_point::max(), row_locker_stats);
        BOOST_REQUIRE(!flock1.available());
        auto ignore = [] (auto) { };
        ignore(std::move(lock));
        flock1.get0();
    });
}
// Trying to lock the same partition a second time with an exclusive lock should
// block until the first lock is released.
SEASTAR_TEST_CASE(test_block_exclusive_twice_partition) {
    return seastar::async([&] {
        auto s = make_schema();
        row_locker rl(s);
        auto pk = make_pk(s, "pk1");
        auto lock = rl.lock_pk(pk, true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        auto flock1 = rl.lock_pk(pk, true, db::timeout_clock::time_point::max(), row_locker_stats);
        BOOST_REQUIRE(!flock1.available());
        auto ignore = [] (auto) { };
        ignore(std::move(lock));
        flock1.get0();
    });
}
// Trying to shared lock together with an exclusive lock (in either order)
// should block the second lock until the first one is released.
SEASTAR_TEST_CASE(test_block_exclusive_and_shared_row) {
    return seastar::async([&] {
        auto s = make_schema();
        row_locker rl(s);
        auto pk = make_pk(s, "pk1");
        auto ck = make_ck(s, "ck1") ;
        // shared lock first, exclusive lock second:
        auto lock = rl.lock_ck(pk, ck, false, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        auto flock1 = rl.lock_ck(pk, ck, true, db::timeout_clock::time_point::max(), row_locker_stats);
        BOOST_REQUIRE(!flock1.available());
        auto ignore = [] (auto) { };
        ignore(std::move(lock));
        flock1.get0();
        // exclusive lock first, shared lock second
        lock = rl.lock_ck(pk, ck, true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        flock1 = rl.lock_ck(pk, ck, false, db::timeout_clock::time_point::max(), row_locker_stats);
        BOOST_REQUIRE(!flock1.available());
        ignore(std::move(lock));
        flock1.get0();
    });
}
SEASTAR_TEST_CASE(test_block_exclusive_and_shared_partition) {
    return seastar::async([&] {
        auto s = make_schema();
        row_locker rl(s);
        auto pk = make_pk(s, "pk1");
        auto lock = rl.lock_pk(pk, false, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        auto flock1 = rl.lock_pk(pk, true, db::timeout_clock::time_point::max(), row_locker_stats);
        BOOST_REQUIRE(!flock1.available());
        auto ignore = [] (auto) { };
        ignore(std::move(lock));
        flock1.get0();
        lock = rl.lock_pk(pk, true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        flock1 = rl.lock_pk(pk, false, db::timeout_clock::time_point::max(), row_locker_stats);
        BOOST_REQUIRE(!flock1.available());
        ignore(std::move(lock));
        flock1.get0();
    });
}
// Trying to lock the a row either exclusive or shared while its partition
// is locked exclusive should block. And also in opposite order.
SEASTAR_TEST_CASE(test_block_partition_row) {
    return seastar::async([&] {
        auto s = make_schema();
        row_locker rl(s);
        auto pk = make_pk(s, "pk1");
        auto ck = make_ck(s, "ck1") ;
        auto lock = rl.lock_pk(pk, true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        auto flock1 = rl.lock_ck(pk, ck, true, db::timeout_clock::time_point::max(), row_locker_stats); // try exclusive row lock
        BOOST_REQUIRE(!flock1.available());
        auto ignore = [] (auto) { };
        ignore(std::move(lock));
        flock1.get0();
        lock = rl.lock_pk(pk, true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        flock1 = rl.lock_ck(pk, ck, false, db::timeout_clock::time_point::max(), row_locker_stats); // also try shared row lock
        BOOST_REQUIRE(!flock1.available());
        ignore(std::move(lock));
        flock1.get0();
        // Now try the same in opposite order (the row lock first, then the
        // partition lock).
        lock = rl.lock_ck(pk, ck, true, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        flock1 = rl.lock_pk(pk, true, db::timeout_clock::time_point::max(), row_locker_stats);
        BOOST_REQUIRE(!flock1.available());
        ignore(std::move(lock));
        flock1.get0();
        lock = rl.lock_ck(pk, ck, false, db::timeout_clock::time_point::max(), row_locker_stats).get0();
        flock1 = rl.lock_pk(pk, true, db::timeout_clock::time_point::max(), row_locker_stats);
        BOOST_REQUIRE(!flock1.available());
        ignore(std::move(lock));
        flock1.get0();
    });
}
