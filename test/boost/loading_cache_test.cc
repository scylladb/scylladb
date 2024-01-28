/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/test/unit_test.hpp>
#include "utils/loading_shared_values.hh"
#include "utils/loading_cache.hh"
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>


#include "seastarx.hh"
#include "test/lib/eventually.hh"
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/tmpdir.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/cql_test_env.hh"

#include <vector>
#include <numeric>
#include <random>

/// Get a random integer in the [0, max) range.
/// \param max bound of the random value range
/// \return The uniformly distributed random integer from the [0, \ref max) range.
static int rand_int(int max) {
    return tests::random::get_int(max - 1);
}

static const sstring test_file_name = "loading_cache_test.txt";
static const sstring test_string = "1";
static bool file_prepared = false;
static constexpr int num_loaders = 1000;

static thread_local int load_count;
static const tmpdir& get_tmpdir() {
    static thread_local tmpdir tmp;
    return tmp;
}

static future<> prepare() {
    if (file_prepared) {
        return make_ready_future<>();
    }

    return open_file_dma((get_tmpdir().path() / test_file_name.c_str()).c_str(), open_flags::create | open_flags::wo).then([] (file f) {
        return do_with(std::move(f), [] (file& f) {
            auto size = test_string.size() + 1;
            auto aligned_size = align_up(size, f.disk_write_dma_alignment());
            auto buf = allocate_aligned_buffer<char>(aligned_size, f.disk_write_dma_alignment());
            auto wbuf = buf.get();
            std::copy_n(test_string.c_str(), size, wbuf);
            return f.dma_write(0, wbuf, aligned_size).then([aligned_size, buf = std::move(buf)] (size_t s) {
                BOOST_REQUIRE_EQUAL(s, aligned_size);
                file_prepared = true;
            }).finally([&f] () mutable {
                return f.close();
            });
        });
    });
}

static future<sstring> loader(const int& k) {
    return open_file_dma((get_tmpdir().path() / test_file_name.c_str()).c_str(), open_flags::ro).then([] (file f) -> future<sstring> {
        return do_with(std::move(f), [] (file& f) -> future<sstring> {
            auto size = align_up(test_string.size() + 1, f.disk_read_dma_alignment());
            return f.dma_read_exactly<char>(0, size).then([] (auto buf) {
                sstring str(buf.get());
                BOOST_REQUIRE_EQUAL(str, test_string);
                ++load_count;
                return make_ready_future<sstring>(std::move(str));
            }).finally([&f] () mutable {
                return f.close();
            });
        });
    });
}

SEASTAR_TEST_CASE(test_loading_shared_values_parallel_loading_same_key) {
    return seastar::async([] {
        std::vector<int> ivec(num_loaders);
        load_count = 0;
        utils::loading_shared_values<int, sstring> shared_values;
        std::list<typename utils::loading_shared_values<int, sstring>::entry_ptr> anchors_list;

        prepare().get();

        std::fill(ivec.begin(), ivec.end(), 0);

        parallel_for_each(ivec, [&] (int& k) {
            return shared_values.get_or_load(k, loader).then([&] (auto entry_ptr) {
                anchors_list.emplace_back(std::move(entry_ptr));
            });
        }).get();

        // "loader" must be called exactly once
        BOOST_REQUIRE_EQUAL(load_count, 1);
        BOOST_REQUIRE_EQUAL(shared_values.size(), 1);
        anchors_list.clear();
    });
}

SEASTAR_TEST_CASE(test_loading_shared_values_parallel_loading_different_keys) {
    return seastar::async([] {
        std::vector<int> ivec(num_loaders);
        load_count = 0;
        utils::loading_shared_values<int, sstring> shared_values;
        std::list<typename utils::loading_shared_values<int, sstring>::entry_ptr> anchors_list;

        prepare().get();

        std::iota(ivec.begin(), ivec.end(), 0);

        parallel_for_each(ivec, [&] (int& k) {
            return shared_values.get_or_load(k, loader).then([&] (auto entry_ptr) {
                anchors_list.emplace_back(std::move(entry_ptr));
            });
        }).get();

        // "loader" must be called once for each key
        BOOST_REQUIRE_EQUAL(load_count, num_loaders);
        BOOST_REQUIRE_EQUAL(shared_values.size(), num_loaders);
        anchors_list.clear();
    });
}

SEASTAR_TEST_CASE(test_loading_shared_values_rehash) {
    return seastar::async([] {
        std::vector<int> ivec(num_loaders);
        load_count = 0;
        utils::loading_shared_values<int, sstring> shared_values;
        std::list<typename utils::loading_shared_values<int, sstring>::entry_ptr> anchors_list;

        prepare().get();

        std::iota(ivec.begin(), ivec.end(), 0);

        // verify that load factor is always in the (0.25, 0.75) range
        for (int k = 0; k < num_loaders; ++k) {
            shared_values.get_or_load(k, loader).then([&] (auto entry_ptr) {
                anchors_list.emplace_back(std::move(entry_ptr));
            }).get();
            BOOST_REQUIRE_LE(shared_values.size(), 3 * shared_values.buckets_count() / 4);
        }

        BOOST_REQUIRE_GE(shared_values.size(), shared_values.buckets_count() / 4);

        // minimum buckets count (by default) is 16, so don't check for less than 4 elements
        for (int k = 0; k < num_loaders - 4; ++k) {
            anchors_list.pop_back();
            shared_values.rehash();
            BOOST_REQUIRE_GE(shared_values.size(), shared_values.buckets_count() / 4);
        }

        anchors_list.clear();
    });
}

SEASTAR_TEST_CASE(test_loading_shared_values_parallel_loading_explicit_eviction) {
    return seastar::async([] {
        std::vector<int> ivec(num_loaders);
        load_count = 0;
        utils::loading_shared_values<int, sstring> shared_values;
        std::vector<typename utils::loading_shared_values<int, sstring>::entry_ptr> anchors_vec(num_loaders);

        prepare().get();

        std::iota(ivec.begin(), ivec.end(), 0);

        parallel_for_each(ivec, [&] (int& k) {
            return shared_values.get_or_load(k, loader).then([&] (auto entry_ptr) {
                anchors_vec[k] = std::move(entry_ptr);
            });
        }).get();

        int rand_key = rand_int(num_loaders);
        BOOST_REQUIRE(shared_values.find(rand_key) != nullptr);
        anchors_vec[rand_key] = nullptr;
        BOOST_REQUIRE_MESSAGE(shared_values.find(rand_key) == nullptr, format("explicit removal for key {} failed", rand_key));
        anchors_vec.clear();
    });
}

SEASTAR_TEST_CASE(test_loading_cache_disable_and_enable) {
    return seastar::async([] {
        using namespace std::chrono;
        load_count = 0;
        utils::loading_cache<int, sstring, 1, utils::loading_cache_reload_enabled::yes> loading_cache({num_loaders, 1h, 50ms}, testlog, loader);
        auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });

        prepare().get();

        loading_cache.get(0).discard_result().get();
        loading_cache.get(0).discard_result().get();

        // Disable
        load_count = 0;
        loading_cache.update_config({num_loaders, 0ms, 50ms});

        sleep(150ms).get();
        BOOST_REQUIRE_EQUAL(load_count, 0);

        // Re-enable
        load_count = 0;
        loading_cache.update_config({num_loaders, 1h, 50ms});
        sleep(50ms).get();

        // Push the entry into the privileged section. Make sure it's being reloaded.
        loading_cache.get_ptr(0).discard_result().get();
        loading_cache.get_ptr(0).discard_result().get();

        BOOST_REQUIRE(eventually_true([&] { return load_count >= 3; }));
    });
}

SEASTAR_TEST_CASE(test_loading_cache_reset) {
    return seastar::async([] {
        using namespace std::chrono;
        utils::loading_cache<int, sstring> loading_cache(num_loaders, 1h, testlog);
        auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });

        prepare().get();

        for (int i = 0; i < num_loaders; ++i) {
            loading_cache.get_ptr(i, loader).discard_result().get();
        }

        BOOST_REQUIRE_EQUAL(loading_cache.size(), num_loaders);

        loading_cache.reset();

        BOOST_REQUIRE_EQUAL(loading_cache.size(), 0);
    });
}

SEASTAR_TEST_CASE(test_loading_cache_update_config) {
    return seastar::async([] {
        using namespace std::chrono;
        utils::loading_cache<int, sstring, 1, utils::loading_cache_reload_enabled::yes> loading_cache({num_loaders, 1h, 1h}, testlog, loader);
        auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });

        prepare().get();

        for (int i = 0; i < num_loaders; ++i) {
            loading_cache.get_ptr(i, loader).discard_result().get();
        }

        BOOST_REQUIRE_EQUAL(loading_cache.size(), num_loaders);

        loading_cache.update_config({2, 50ms, 50ms});

        sleep(50ms).get();

        for (int i = num_loaders; i < 2 * num_loaders; ++i) {
            loading_cache.get_ptr(i, loader).discard_result().get();
        }

        REQUIRE_EVENTUALLY_EQUAL(loading_cache.size(), 2);
    });
}

SEASTAR_TEST_CASE(test_loading_cache_loading_same_key) {
    return seastar::async([] {
        using namespace std::chrono;
        std::vector<int> ivec(num_loaders);
        load_count = 0;
        utils::loading_cache<int, sstring> loading_cache(num_loaders, 1s, testlog);
        auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });

        prepare().get();

        std::fill(ivec.begin(), ivec.end(), 0);

        parallel_for_each(ivec, [&] (int& k) {
            return loading_cache.get_ptr(k, loader).discard_result();
        }).get();

        // "loader" must be called exactly once
        BOOST_REQUIRE_EQUAL(load_count, 1);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 1);
    });
}

SEASTAR_THREAD_TEST_CASE(test_loading_cache_removing_key) {
    using namespace std::chrono;
    load_count = 0;
    utils::loading_cache<int, sstring> loading_cache(num_loaders, 100s, testlog);
    auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });

    prepare().get();

    loading_cache.get_ptr(0, loader).discard_result().get();
    BOOST_REQUIRE_EQUAL(load_count, 1);
    BOOST_REQUIRE(loading_cache.find(0) != nullptr);

    loading_cache.remove(0);
    BOOST_REQUIRE(loading_cache.find(0) == nullptr);
}

SEASTAR_TEST_CASE(test_loading_cache_loading_different_keys) {
    return seastar::async([] {
        using namespace std::chrono;
        std::vector<int> ivec(num_loaders);
        load_count = 0;
        utils::loading_cache<int, sstring> loading_cache(num_loaders, 1h, testlog);
        auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });

        prepare().get();

        std::iota(ivec.begin(), ivec.end(), 0);

        parallel_for_each(ivec, [&] (int& k) {
            return loading_cache.get_ptr(k, loader).discard_result();
        }).get();

        BOOST_REQUIRE_EQUAL(load_count, num_loaders);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), num_loaders);
    });
}

SEASTAR_TEST_CASE(test_loading_cache_loading_expiry_eviction) {
    return seastar::async([] {
        using namespace std::chrono;
        utils::loading_cache<int, sstring, 1> loading_cache(num_loaders, 20ms, testlog);
        auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });

        prepare().get();

        loading_cache.get_ptr(0, loader).discard_result().get();

        // Check unprivileged section eviction
        BOOST_REQUIRE(loading_cache.size() == 1);
        sleep(20ms).get();
        REQUIRE_EVENTUALLY_EQUAL(loading_cache.size(), 0);

        // Check privileged section eviction
        loading_cache.get_ptr(0, loader).discard_result().get();
        BOOST_REQUIRE(loading_cache.find(0) != nullptr);

        sleep(20ms).get();
        REQUIRE_EVENTUALLY_EQUAL(loading_cache.size(), 0);
    });
}

SEASTAR_TEST_CASE(test_loading_cache_loading_expiry_reset_on_sync_op) {
    return seastar::async([] {
        using namespace std::chrono;
        utils::loading_cache<int, sstring> loading_cache(num_loaders, 30ms, testlog);
        auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });

        prepare().get();

        loading_cache.get_ptr(0, loader).discard_result().get();
        auto vp = loading_cache.find(0);
        auto load_time = steady_clock::now();

        // Check that the expiration timer is reset every time we call a find()
        for (int i = 0; i < 10; ++i) {
            // seastar::lowres_clock has 10ms resolution. This means that we should use 10ms threshold to compensate.
            if (steady_clock::now() <= load_time + 20ms) {
                BOOST_REQUIRE(vp != nullptr);
            } else {
                // If there was a delay and we weren't able to execute the next loop iteration during 20ms let's repopulate
                // the cache.
                loading_cache.get_ptr(0, loader).discard_result().get();
                BOOST_TEST_MESSAGE("Test " << i << " was skipped. Repopulating...");
            }
            vp = loading_cache.find(0);
            load_time = steady_clock::now();
            sleep(10ms).get();
        }

        sleep(30ms).get();
        REQUIRE_EVENTUALLY_EQUAL(loading_cache.size(), 0);
    });
}

SEASTAR_TEST_CASE(test_loading_cache_move_item_to_mru_list_front_on_sync_op) {
    return seastar::async([] {
        using namespace std::chrono;
        utils::loading_cache<int, sstring> loading_cache(2, 1h, testlog);
        auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });

        prepare().get();

        for (int i = 0; i < 2; ++i) {
            loading_cache.get_ptr(i, loader).discard_result().get();
        }

        auto vp0 = loading_cache.find(0);
        BOOST_REQUIRE(vp0 != nullptr);

        loading_cache.get_ptr(2, loader).discard_result().get();

        // "0" should be at the beginning of the list and "1" right after it before we try to add a new entry to the
        // cache ("2"). And hence "1" should get evicted.
        vp0 = loading_cache.find(0);
        auto vp1 = loading_cache.find(1);
        BOOST_REQUIRE(vp0 != nullptr);
        BOOST_REQUIRE(vp1 == nullptr);
    });
}

SEASTAR_TEST_CASE(test_loading_cache_loading_reloading_privileged_gen) {
    return seastar::async([] {
        using namespace std::chrono;
        load_count = 0;
        utils::loading_cache<int, sstring, 1, utils::loading_cache_reload_enabled::yes> loading_cache({num_loaders, 100ms, 20ms}, testlog, loader);
        auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });
        prepare().get();
        // Push the entry into the privileged section. Make sure it's being reloaded.
        loading_cache.get_ptr(0).discard_result().get();
        loading_cache.get_ptr(0).discard_result().get();
        sleep(60ms).get();
        BOOST_REQUIRE(eventually_true([&] { return load_count >= 3; }));
    });
}

SEASTAR_TEST_CASE(test_loading_cache_loading_reloading_unprivileged) {
    return seastar::async([] {
        using namespace std::chrono;
        load_count = 0;
        utils::loading_cache<int, sstring, 1, utils::loading_cache_reload_enabled::yes> loading_cache({num_loaders, 100ms, 20ms}, testlog, loader);
        auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });
        prepare().get();
        // Load one entry into the unprivileged section.
        // Make sure it's reloaded.
        loading_cache.get_ptr(0).discard_result().get();
        sleep(60ms).get();
        BOOST_REQUIRE(eventually_true([&] { return load_count >= 2; }));
    });
}

SEASTAR_TEST_CASE(test_loading_cache_max_size_eviction) {
    return seastar::async([] {
        using namespace std::chrono;
        load_count = 0;
        utils::loading_cache<int, sstring> loading_cache(1, 1s, testlog);
        auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });

        prepare().get();

        for (int i = 0; i < num_loaders; ++i) {
            loading_cache.get_ptr(i % 2, loader).discard_result().get();
        }

        BOOST_REQUIRE_EQUAL(load_count, num_loaders);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 1);
    });
}

SEASTAR_TEST_CASE(test_loading_cache_max_size_eviction_unprivileged_first) {
    return seastar::async([] {
        using namespace std::chrono;
        load_count = 0;
        utils::loading_cache<int, sstring, 1> loading_cache(4, 1h, testlog);
        auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });

        prepare().get();

        // Touch the value with the key "-1" twice
        loading_cache.get_ptr(-1, loader).discard_result().get();
        loading_cache.find(-1);

        for (int i = 0; i < num_loaders; ++i) {
            loading_cache.get_ptr(i, loader).discard_result().get();
        }

        BOOST_REQUIRE_EQUAL(load_count, num_loaders + 1);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 4);
        // Make sure that the value we touched twice is still in the cache
        BOOST_REQUIRE(loading_cache.find(-1) != nullptr);
    });
}

SEASTAR_TEST_CASE(test_loading_cache_eviction_unprivileged) {
    return seastar::async([] {
        using namespace std::chrono;
        load_count = 0;
        utils::loading_cache<int, sstring, 1> loading_cache(4, 10ms, testlog);
        auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });

        prepare().get();

        // Touch the value with the key "-1" twice
        loading_cache.get_ptr(-1, loader).discard_result().get();
        loading_cache.find(-1);

        for (int i = 0; i < num_loaders; ++i) {
            loading_cache.get_ptr(i, loader).discard_result().get();
        }

        // Make sure that the value we touched twice is eventually evicted
        REQUIRE_EVENTUALLY_EQUAL(loading_cache.find(-1), nullptr);
        REQUIRE_EVENTUALLY_EQUAL(loading_cache.size(), 0);
    });
}

SEASTAR_TEST_CASE(test_loading_cache_eviction_unprivileged_minimum_size) {
    return seastar::async([] {
        // Test that unprivileged section is not starved.
        //
        // This scenario is tested: cache max_size is 50 and there are 49 entries in
        // privileged section. After adding 5 elements (that go to unprivileged
        // section) all of them should stay in unprivileged section and elements
        // in privileged section should get evicted.
        //
        // Wrong handling of this situation caused problems with BATCH statements 
        // where all prepared statements in the batch have to stay in cache at 
        // the same time for the batch to correctly execute.

        using namespace std::chrono;
        utils::loading_cache<int, sstring, 1> loading_cache(50, 1h, testlog);
        auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });

        prepare().get();

        // Add 49 elements to privileged section
        for (int i = 0; i < 49; i++) {
            // Touch the value with the key "i" twice
            loading_cache.get_ptr(i, loader).discard_result().get();
            loading_cache.find(i);
        }

        // Add 5 elements to unprivileged section
        for (int i = 50; i < 55; i++) {
            loading_cache.get_ptr(i, loader).discard_result().get();            
        }

        // Make sure that none of 5 elements were evicted
        for (int i = 50; i < 55; i++) {
            BOOST_REQUIRE(loading_cache.find(i) != nullptr);
        } 

        BOOST_REQUIRE_EQUAL(loading_cache.size(), 50);
    });
}

struct sstring_length_entry_size {
    size_t operator()(const sstring& val) {
        return val.size();
    }
};

SEASTAR_TEST_CASE(test_loading_cache_section_size_correctly_calculated) {
    return seastar::async([] {
        auto load_len1 =  [] (const int& key) { return make_ready_future<sstring>(tests::random::get_sstring(1)); };
        auto load_len5 =  [] (const int& key) { return make_ready_future<sstring>(tests::random::get_sstring(5)); };
        auto load_len10 = [] (const int& key) { return make_ready_future<sstring>(tests::random::get_sstring(10)); };
        auto load_len95 = [] (const int& key) { return make_ready_future<sstring>(tests::random::get_sstring(95)); };

        using namespace std::chrono;
        utils::loading_cache<int, sstring, 1, utils::loading_cache_reload_enabled::no, sstring_length_entry_size> loading_cache(100, 1h, testlog);
        auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });

        BOOST_REQUIRE_EQUAL(loading_cache.privileged_section_memory_footprint(), 0);
        BOOST_REQUIRE_EQUAL(loading_cache.unprivileged_section_memory_footprint(), 0);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 0);

        loading_cache.get_ptr(1, load_len1).discard_result().get();
        BOOST_REQUIRE_EQUAL(loading_cache.privileged_section_memory_footprint(), 0);
        BOOST_REQUIRE_EQUAL(loading_cache.unprivileged_section_memory_footprint(), 1);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 1);

        loading_cache.get_ptr(2, load_len5).discard_result().get();
        BOOST_REQUIRE_EQUAL(loading_cache.privileged_section_memory_footprint(), 0);
        BOOST_REQUIRE_EQUAL(loading_cache.unprivileged_section_memory_footprint(), 6);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 2);

        // Move "2" to privileged section by touching it the second time.
        loading_cache.get_ptr(2, load_len5).discard_result().get();
        BOOST_REQUIRE_EQUAL(loading_cache.privileged_section_memory_footprint(), 5);
        BOOST_REQUIRE_EQUAL(loading_cache.unprivileged_section_memory_footprint(), 1);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 2);

        loading_cache.get_ptr(3, load_len10).discard_result().get();
        BOOST_REQUIRE_EQUAL(loading_cache.privileged_section_memory_footprint(), 5);
        BOOST_REQUIRE_EQUAL(loading_cache.unprivileged_section_memory_footprint(), 11);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 3);

        // Move "1" to privileged section. load_len10 should not get executed, as "1"
        // is already in the cache.
        loading_cache.get_ptr(1, load_len10).discard_result().get();
        BOOST_REQUIRE_EQUAL(loading_cache.privileged_section_memory_footprint(), 6);
        BOOST_REQUIRE_EQUAL(loading_cache.unprivileged_section_memory_footprint(), 10);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 3);

        // Flood cache with elements of size 10,
        // unprivileged. "1" and "2" should stay in the privileged section.
        for (int i = 11; i < 30; i++) {
            loading_cache.get_ptr(i, load_len10).discard_result().get();
        }
        BOOST_REQUIRE_EQUAL(loading_cache.privileged_section_memory_footprint(), 6);
        // We shrink the cache BEFORE adding element,
        // so after adding the element, the cache
        // can exceed max_size...
        BOOST_REQUIRE_EQUAL(loading_cache.unprivileged_section_memory_footprint(), 100); 
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 12);

        // Flood cache with elements of size 10, privileged.
        for (int i = 11; i < 30; i++) {
            loading_cache.get_ptr(i, load_len10).discard_result().get();
            loading_cache.get_ptr(i, load_len10).discard_result().get();
        }
        BOOST_REQUIRE_EQUAL(loading_cache.privileged_section_memory_footprint(), 100);
        BOOST_REQUIRE_EQUAL(loading_cache.unprivileged_section_memory_footprint(), 0);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 10);

        // Add one new unprivileged entry.
        loading_cache.get_ptr(31, load_len1).discard_result().get();
        BOOST_REQUIRE_EQUAL(loading_cache.privileged_section_memory_footprint(), 90);
        BOOST_REQUIRE_EQUAL(loading_cache.unprivileged_section_memory_footprint(), 1);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 10);

        // Add another unprivileged entry, privileged entry should get evicted.
        loading_cache.get_ptr(32, load_len5).discard_result().get();
        BOOST_REQUIRE_EQUAL(loading_cache.privileged_section_memory_footprint(), 90);
        BOOST_REQUIRE_EQUAL(loading_cache.unprivileged_section_memory_footprint(), 6);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 11);

        // Make it privileged by touching it again.
        loading_cache.get_ptr(32, load_len5).discard_result().get();
        BOOST_REQUIRE_EQUAL(loading_cache.privileged_section_memory_footprint(), 95);
        BOOST_REQUIRE_EQUAL(loading_cache.unprivileged_section_memory_footprint(), 1);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 11);

        // Add another unprivileged entry.
        loading_cache.get_ptr(33, load_len10).discard_result().get();
        BOOST_REQUIRE_EQUAL(loading_cache.privileged_section_memory_footprint(), 95);
        // We shrink the cache BEFORE adding element,
        // so after adding the element, the cache
        // can exceed max_size...
        BOOST_REQUIRE_EQUAL(loading_cache.unprivileged_section_memory_footprint(), 11);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 12);

        // Add another unprivileged entry, privileged entry should get evicted.
        loading_cache.get_ptr(34, load_len10).discard_result().get();
        BOOST_REQUIRE_EQUAL(loading_cache.privileged_section_memory_footprint(), 85);
        // We shrink the cache BEFORE adding element,
        // so after adding the element, the cache
        // can exceed max_size...
        BOOST_REQUIRE_EQUAL(loading_cache.unprivileged_section_memory_footprint(), 21);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 12);

        // Add a big unprivileged entry, filling almost entire cache.
        loading_cache.get_ptr(35, load_len95).discard_result().get();
        BOOST_REQUIRE_EQUAL(loading_cache.privileged_section_memory_footprint(), 75);
        // We shrink the cache BEFORE adding element,
        // so after adding the element, the cache
        // can exceed max_size...
        BOOST_REQUIRE_EQUAL(loading_cache.unprivileged_section_memory_footprint(), 95 + 21);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 12);
    });
}

SEASTAR_TEST_CASE(test_loading_cache_reload_during_eviction) {
    return seastar::async([] {
        using namespace std::chrono;
        load_count = 0;
        utils::loading_cache<int, sstring, 0, utils::loading_cache_reload_enabled::yes> loading_cache({1, 100ms, 10ms}, testlog, loader);
        auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });

        prepare().get();

        auto curr_time = lowres_clock::now();
        int i = 0;

        // this will cause reloading when values are being actively evicted due to the limited cache size
        do_until(
            [&] { return lowres_clock::now() - curr_time > 1s; },
            [&] { return loading_cache.get_ptr(i++ % 2).discard_result(); }
        ).get();

        BOOST_REQUIRE_EQUAL(loading_cache.size(), 1);
    });
}

SEASTAR_THREAD_TEST_CASE(test_loading_cache_remove_leaves_no_old_entries_behind) {
    using namespace std::chrono;
    load_count = 0;

    auto load_v1 = [] (auto key) { return make_ready_future<sstring>("v1"); };
    auto load_v2 = [] (auto key) { return make_ready_future<sstring>("v2"); };
    auto load_v3 = [] (auto key) { return make_ready_future<sstring>("v3"); };

    {
        utils::loading_cache<int, sstring> loading_cache(num_loaders, 100s, testlog);
        auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });

        //
        // Test remove() concurrent with loading
        //

        auto f = loading_cache.get_ptr(0, [&](auto key) {
            return yield().then([&load_v1, key] {
                return load_v1(key);
            });
        });

        loading_cache.remove(0);

        BOOST_REQUIRE_EQUAL(loading_cache.find(0), nullptr);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 0);

        auto ptr1 = f.get();
        BOOST_REQUIRE_EQUAL(*ptr1, "v1");

        BOOST_REQUIRE_EQUAL(loading_cache.find(0), nullptr);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 0);

        ptr1 = loading_cache.get_ptr(0, load_v2).get();
        loading_cache.remove(0);
        BOOST_REQUIRE_EQUAL(*ptr1, "v2");

        //
        // Test that live ptr1, removed from cache, does not prevent reload of new value
        //
        auto ptr2 = loading_cache.get_ptr(0, load_v3).get();
        ptr1 = nullptr;
        BOOST_REQUIRE_EQUAL(*ptr2, "v3");
    }

    // Test remove_if()
    {
        utils::loading_cache<int, sstring> loading_cache(num_loaders, 100s, testlog);
        auto stop_cache_reload = seastar::defer([&loading_cache] { loading_cache.stop().get(); });

        //
        // Test remove_if() concurrent with loading
        //
        auto f = loading_cache.get_ptr(0, [&](auto key) {
            return yield().then([&load_v1, key] {
                return load_v1(key);
            });
        });

        loading_cache.remove_if([] (auto&& v) { return v == "v1"; });

        BOOST_REQUIRE_EQUAL(loading_cache.find(0), nullptr);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 0);

        auto ptr1 = f.get();
        BOOST_REQUIRE_EQUAL(*ptr1, "v1");

        BOOST_REQUIRE_EQUAL(loading_cache.find(0), nullptr);
        BOOST_REQUIRE_EQUAL(loading_cache.size(), 0);

        ptr1 = loading_cache.get_ptr(0, load_v2).get();
        loading_cache.remove_if([] (auto&& v) { return v == "v2"; });
        BOOST_REQUIRE_EQUAL(*ptr1, "v2");

        //
        // Test that live ptr1, removed from cache, does not prevent reload of new value
        //
        auto ptr2 = loading_cache.get_ptr(0, load_v3).get();
        ptr1 = nullptr;
        BOOST_REQUIRE_EQUAL(*ptr2, "v3");
        ptr2 = nullptr;
    }
}

SEASTAR_TEST_CASE(test_prepared_statement_small_cache) {
    // CQL prepared statement cache uses loading_cache
    // internally.
    constexpr auto CACHE_SIZE = 950000;

    cql_test_config small_cache_config;
    small_cache_config.qp_mcfg = {CACHE_SIZE, CACHE_SIZE};
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE tbl1 (a int, b int, PRIMARY KEY (a))").get();

        auto current_uid = 0;

        // Prepare 100 queries and execute them twice,
        // filling "privileged section" of loading_cache.
        std::vector<cql3::prepared_cache_key_type> prepared_ids_privileged;
        for (int i = 0; i < 100; i++) {
            auto prepared_id = e.prepare(fmt::format("SELECT * FROM tbl1 WHERE a = {}", current_uid++)).get();
            e.execute_prepared(prepared_id, {}).get();
            e.execute_prepared(prepared_id, {}).get();
            prepared_ids_privileged.push_back(prepared_id);
        }

        int how_many_in_cache = 0;
        for (auto& prepared_id : prepared_ids_privileged) {
            if (e.local_qp().get_prepared(prepared_id)) {
                how_many_in_cache++;
            }
        }

        // Assumption: CACHE_SIZE should hold at least 50 queries,
        // but not more than 99 queries. Other checks in this 
        // test rely on that fact.
        BOOST_REQUIRE(how_many_in_cache >= 50);
        BOOST_REQUIRE(how_many_in_cache <= 99);

        // Then prepare 5 queries and execute them one time,
        // which will occupy "unprivileged section" of loading_cache.
        std::vector<cql3::prepared_cache_key_type> prepared_ids_unprivileged;
        for (int i = 0; i < 5; i++) {
            auto prepared_id = e.prepare(fmt::format("SELECT * FROM tbl1 WHERE a = {}", current_uid++)).get();
            e.execute_prepared(prepared_id, {}).get();
            prepared_ids_unprivileged.push_back(prepared_id);
        }

        // Check that all of those prepared queries can still be
        // executed. This simulates as if you wanted to execute
        // a BATCH with all of them, which requires all of those
        // prepared statements to be executable (in the cache).
        for (auto& prepared_id : prepared_ids_unprivileged) {
            e.execute_prepared(prepared_id, {}).get();
        } 

        // Deterministic random for reproducibility.
        testing::local_random_engine.seed(12345);

        // Prepare 500 queries and execute them a random number of times.
        for (int i = 0; i < 500; i++) {
            auto prepared_id = e.prepare(fmt::format("SELECT * FROM tbl1 WHERE a = {}", current_uid++)).get();
            auto times = rand_int(4);
            for (int j = 0; j < times; j++) {
                e.execute_prepared(prepared_id, {}).get();
            }
        }

        // Prepare 100 simulated "batches" and execute them 
        // a random number of times.
        for (int i = 0; i < 100; i++) {
            std::vector<cql3::prepared_cache_key_type> prepared_ids_batch;
            for (int j = 0; j < 5; j++) {
                auto prepared_id = e.prepare(fmt::format("SELECT * FROM tbl1 WHERE a = {}", current_uid++)).get();
                prepared_ids_batch.push_back(prepared_id);
            }
            auto times = rand_int(4);
            for (int j = 0; j < times; j++) {
                for (auto& prepared_id : prepared_ids_batch) {
                    e.execute_prepared(prepared_id, {}).get();
                } 
            }
        }        
    }, small_cache_config);
}
