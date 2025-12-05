/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "test/lib/mutation_source_test.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/log.hh"

#include "utils/replicator.hh"
#include "utils/throttle.hh"

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>
#include <fmt/format.h>
#include <fmt/std.h>
#include <map>
#include <optional>


BOOST_AUTO_TEST_SUITE(replicator_test)

using map_type = std::map<int, std::optional<int>>;
using key_setter = std::pair<int, int>;

static void require_multishard() {
    tests::require_greater(smp::count, 1);
}

// last-write-wins semantics for mutations.
struct map_container : public seastar::peering_sharded_service<map_container> {
    map_type _m;
    std::function<void()> _on_apply = [] {};
    uint64_t _failures = 0;

    void apply_locally(const map_type& m) {
        _on_apply();
        apply(_m, m);
    }

    void apply_locally(const key_setter& kv) {
        map_type src;
        apply(src, kv);
        apply_locally(src);
    }

    static void apply(map_type& dst, const map_type& src) {
        for (auto&& e : src) {
            if (e.second) {
                dst[e.first] = *e.second;
            }
        }
    }

    static void apply(map_type& dst, const key_setter& src) {
        apply(dst, map_type{std::make_pair(src.first, std::make_optional(src.second))});
    }

    static void prepare_apply(map_type& map, const key_setter& mf) {
        map.try_emplace(mf.first, std::nullopt);
    }

    void on_replication_failed(std::exception_ptr) {
        ++_failures;
    }
};

SEASTAR_THREAD_TEST_CASE(replicator_model) {
    require_multishard();

    seastar::sharded<map_container> c;
    c.start().get();
    auto stop_c = seastar::defer([&c] { c.stop().get(); });

    replicator<map_type, map_container> r(c.local());
    auto stop_r = seastar::defer([&r] { r.stop().get(); });

    r.apply_to_all(key_setter{2, 29});
    seastar::thread::yield();
    r.apply_to_all(key_setter{1, 15});
    r.apply_to_all(key_setter{2, 22});
    r.apply_to_all(key_setter{1, 10});

    r.barrier().get();

    c.invoke_on_all([] (map_container& con) {
        tests::require_equal(con._m.size(), 2);
        tests::require_equal(con._m[1], 10);
        tests::require_equal(con._m[2], 22);
    }).get();

    for (int i = 0; i < 100; ++i) {
        r.apply_to_all(key_setter{tests::random::get_int(0, 4), tests::random::get_int(0, 5)});
        if (tests::random::get_bool()) {
            seastar::thread::yield();
        }
    }

    r.barrier().get();
    auto expected = c.local()._m;
    c.invoke_on_others([&] (map_container& con) {
        tests::require_equal(con._m, expected);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(replicator_stress) {
    require_multishard();

    seastar::sharded<map_container> c;
    c.start().get();
    auto stop_c = seastar::defer([&c] { c.stop().get(); });

    c.invoke_on_all([] (map_container& con) {
        con._on_apply = [] {
            if (tests::random::get_int(0, 9) == 0) {
                throw std::bad_alloc();
            }
        };
    }).get();

    replicator<map_type, map_container> r(c.local());
    auto stop_r = seastar::defer([&r] { r.stop().get(); });

    for (int i = 0; i < 1000; ++i) {
        try {
            r.apply_to_all(key_setter {tests::random::get_int(0, 4), tests::random::get_int(0, 5)});
        } catch (const std::bad_alloc&) {
            // expected
        }
        if (tests::random::get_int(0, 2) == 0) {
            seastar::thread::yield();
        }
    }

    testlog.info("failures: {}", c.local()._failures);
    tests::require_greater(c.local()._m.size(), 0); // Some should succeed

    r.barrier().get();

    // But all converged
    auto expected = c.local()._m;
    c.invoke_on_others([&] (map_container& con) {
        tests::require_equal(con._m, expected);
    }).get();
}

BOOST_AUTO_TEST_SUITE_END()
