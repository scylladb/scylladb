/*
 * Copyright (C) 2015 ScyllaDB
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

#include "sstables/atomic_deletion.hh"
#include <seastar/tests/test-utils.hh>
#include <deque>
#include <boost/range/numeric.hpp>
#include <boost/range/adaptor/transformed.hpp>

using namespace sstables;


class atomic_deletion_test_env {
public:
    using event = std::function<future<> (atomic_deletion_test_env& adm)>;
private:
    struct a_hash {
        size_t operator()(const std::unordered_set<sstring>& s) const {
            auto h = std::hash<sstring>();
            return boost::accumulate(s | boost::adaptors::transformed(h), size_t(0)); // sue me
        }
    };
    atomic_deletion_manager _adm;
    std::deque<event> _events;
    std::unordered_set<std::unordered_set<sstring>, a_hash> _deletes;
    semaphore _deletion_counter { 0 };
private:
    future<> delete_sstables(std::vector<sstring> names) {
        auto&& s1 = boost::copy_range<std::unordered_set<sstring>>(names);
        _deletes.insert(s1);
        _deletion_counter.signal();
        return make_ready_future<>();
    }
public:
    explicit atomic_deletion_test_env(unsigned shard_count, std::vector<event> events)
            : _adm(shard_count, [this] (std::vector<sstring> names) {
                    return delete_sstables(names);
               })
            , _events(events.begin(), events.end()) {
    }
    void expect_no_deletion() {
        BOOST_REQUIRE(_deletes.empty());
    }
    future<> schedule_delete(std::vector<sstable_to_delete> names, unsigned shard) {
        _adm.delete_atomically(names, shard).discard_result();
        return make_ready_future<>();
    }
    future<> expect_deletion(std::vector<sstring> names) {
        return _deletion_counter.wait().then([this, names] {
            auto&& s1 = boost::copy_range<std::unordered_set<sstring>>(names);
            auto erased = _deletes.erase(s1);
            BOOST_REQUIRE_EQUAL(erased, 1);
        });
    }
    future<> test() {
        // run all _events sequentially
        return repeat([this] {
            if (_events.empty()) {
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            auto ev = std::move(_events.front());
            _events.pop_front();
            return ev(*this).then([] {
                return stop_iteration::no;
            });
        });
    }
};

future<> test_atomic_deletion_manager(unsigned shards, std::vector<atomic_deletion_test_env::event> events) {
    auto env = make_lw_shared<atomic_deletion_test_env>(shards, events);
    return env->test().finally([env] {});
}

atomic_deletion_test_env::event
delete_many(std::vector<sstable_to_delete> v, unsigned shard) {
    return [v, shard] (atomic_deletion_test_env& env) {
        // verify we didn't have an early delete from previous deletion
        env.expect_no_deletion();
        return env.schedule_delete(v, shard);
    };
}

atomic_deletion_test_env::event
delete_one(sstable_to_delete s, unsigned shard) {
    return delete_many({s}, shard);
}

atomic_deletion_test_env::event
expect_many(std::vector<sstring> names) {
    return [names] (atomic_deletion_test_env& env) {
        return env.expect_deletion(names);
    };
}

atomic_deletion_test_env::event
expect_one(sstring name) {
    return expect_many({name});
}

SEASTAR_TEST_CASE(test_single_shard_single_sstable) {
    return test_atomic_deletion_manager(1, {
            delete_one({"1", false}, 0),
            expect_one("1"),
            delete_one({"2", true}, 0),
            expect_one("2"),
    });
}

SEASTAR_TEST_CASE(test_multi_shard_single_sstable) {
    return test_atomic_deletion_manager(3, {
            delete_one({"1", true}, 0),
            delete_one({"1", true}, 1),
            delete_one({"1", true}, 2),
            expect_one("1"),
            delete_one({"2", false}, 1),
            expect_one("2"),
    });
}

SEASTAR_TEST_CASE(test_nonshared_compaction) {
    return test_atomic_deletion_manager(5, {
            delete_many({{"1", false}, {"2", false}, {"3", false}}, 2),
            expect_many({"1", "2", "3"}),
    });
}

SEASTAR_TEST_CASE(test_shared_compaction) {
    return test_atomic_deletion_manager(3, {
            delete_one({"1", true}, 0),
            delete_many({{"1", true}, {"2", false}, {"3", false}}, 2),
            delete_one({"1", true}, 1),
            expect_many({"1", "2", "3"}),
    });
}

SEASTAR_TEST_CASE(test_overlapping_compaction) {
    return test_atomic_deletion_manager(3, {
            delete_one({"1", true}, 0),
            delete_one({"3", true}, 0),
            delete_many({{"1", true}, {"2", false}, {"3", true}}, 2),
            delete_one({"1", true}, 1),
            delete_many({{"3", true}, {"4", false}}, 1),
            expect_many({"1", "2", "3", "4"}),
    });
}


#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;
