/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/testing/test_case.hh>
#include <seastar/testing/on_internal_error.hh>
#include <unordered_set>
#include <ranges>
#include <seastar/core/coroutine.hh>

#include "db/config.hh"
#include "raft/raft.hh"
#include "utils/UUID_gen.hh"

#include "test/lib/random_utils.hh"
#include "test/lib/log.hh"

#include "service/raft/raft_sys_table_storage.hh"
#include "service/strong_consistency/raft_groups_storage.hh"
#include "dht/fixed_shard.hh"

#include "test/lib/cql_test_env.hh"
#include "cql3/query_processor.hh"
#include "replica/database.hh"

#include "gms/inet_address_serializer.hh"

namespace raft{

// these operators provided exclusively for testing purposes

static bool operator==(const configuration& lhs, const configuration& rhs) {
    return lhs.current == rhs.current && lhs.previous == rhs.previous;
}

static bool operator==(const snapshot_descriptor& lhs, const snapshot_descriptor& rhs) {
    return lhs.idx == rhs.idx &&
        lhs.term == rhs.term &&
        lhs.config == rhs.config &&
        lhs.id == rhs.id;
}

static bool operator==(const log_entry::dummy&, const log_entry::dummy&) {
    return true;
}

static bool operator==(const log_entry& lhs, const log_entry& rhs) {
    return lhs.term == rhs.term &&
        lhs.idx == rhs.idx &&
        lhs.data == rhs.data;
}

} // namespace raft

using namespace service::strong_consistency;

static raft::group_id gid{utils::UUID_gen::min_time_UUID()};
static constexpr shard_id test_shard = 0;

// Create a randomized test log to harden Raft storage tests.
// Randomizes: length (min_entries..max_entries, default 0..20), first index,
// entry types (command/configuration/dummy), command payloads, configuration
// member counts, and terms (monotonically non-decreasing).
//
// Empty logs and logs missing some entry types are intentional — they
// exercise corner cases that are also valid real-world states.
//
// To reproduce a failure, pass --random-seed=<N> printed by the test runner.
// server_id values inside configuration entries are derived from tests::random
// so the same seed always produces the identical log.
static std::vector<raft::log_entry_ptr> create_test_log(size_t min_entries = 0, size_t max_entries = 20) {
    SCYLLA_ASSERT(min_entries <= max_entries);

    uint64_t first_idx = tests::random::get_int<uint64_t>(1, 1000);

    // Helper: build one configuration entry (data only, no idx/term yet).
    auto make_config_data = []() -> raft::configuration {
        int num_members = tests::random::get_int(1, 5);
        raft::config_member_set members;
        for (int m = 0; m < num_members; ++m) {
            members.emplace(raft::config_member{
                raft::server_address{
                    raft::server_id{utils::UUID(
                        tests::random::get_int<uint64_t>(),
                        tests::random::get_int<uint64_t>())},
                    {}},
                raft::is_voter::yes});
        }
        return raft::configuration{std::move(members)};
    };

    // Helper: build one command entry (data only).
    auto make_cmd_data = []() -> raft::command {
        raft::command cmd;
        ser::serialize(cmd, tests::random::get_int(0, 100000));
        return cmd;
    };

    size_t count = tests::random::get_int(min_entries, max_entries);

    using data_t = std::variant<raft::command, raft::configuration, raft::log_entry::dummy>;
    std::vector<data_t> data_vec;
    data_vec.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        switch (tests::random::get_int(0, 2)) {
        case 0: data_vec.push_back(make_cmd_data());          break;
        case 1: data_vec.push_back(make_config_data());       break;
        case 2: data_vec.push_back(raft::log_entry::dummy()); break;
        }
    }

    // Assign monotonically non-decreasing terms and contiguous indices.
    std::vector<raft::log_entry_ptr> entries;
    entries.reserve(data_vec.size());
    uint64_t term = 1;
    for (size_t i = 0; i < data_vec.size(); ++i) {
        term += tests::random::get_int(0, 3);
        entries.push_back(make_lw_shared(raft::log_entry{
            .term = raft::term_t(term),
            .idx  = raft::index_t(first_idx + i),
            .data = std::move(data_vec[i])}));
    }

    // Log generated entries to aid reproduction: rerun with same --random-seed=<N>.
    testlog.info("create_test_log: {} entries, first_idx={}", entries.size(), first_idx);
    for (const auto& e : entries) {
        const char* type = std::holds_alternative<raft::command>(e->data)       ? "command"
                         : std::holds_alternative<raft::configuration>(e->data) ? "configuration"
                         :                                                         "dummy";
        testlog.info("  idx={} term={} type={}", e->idx, e->term, type);
    }

    return entries;
}

// Factory functions to create storage instances with uniform interface
static service::raft_sys_table_storage make_sys_table_storage(cql_test_env& env, raft::group_id group_id) {
    return service::raft_sys_table_storage(env.local_qp(), group_id, raft::server_id::create_random_id());
}

static raft_groups_storage make_groups_storage(cql_test_env& env, raft::group_id group_id) {
    return raft_groups_storage(env.local_qp(), env.local_db(), group_id, raft::server_id::create_random_id(), test_shard,
            *env.local_db().commitlog(), table_id(utils::UUID_gen::get_time_UUID()), {});
}

static future<> do_with_cql_env_strongly_consistent(std::function<future<>(cql_test_env&)> func) {
    auto db_cfg_ptr = make_shared<db::config>();
    auto& db_cfg = *db_cfg_ptr;
    db_cfg.experimental_features({db::experimental_features_t::feature::STRONGLY_CONSISTENT_TABLES});
    return do_with_cql_env(std::move(func), std::move(db_cfg_ptr));
}

// Same, with the commitlog segment size lowered so that a group's entries land
// in more than one segment — and therefore in more than one record — without
// having to write 64MB.
static future<> do_with_cql_env_small_segments(std::function<future<>(cql_test_env&)> func) {
    auto db_cfg_ptr = make_shared<db::config>();
    auto& db_cfg = *db_cfg_ptr;
    db_cfg.experimental_features({db::experimental_features_t::feature::STRONGLY_CONSISTENT_TABLES});
    db_cfg.commitlog_segment_size_in_mb(1);
    return do_with_cql_env(std::move(func), std::move(db_cfg_ptr));
}

// A command entry with a trivial payload.
// A command entry padded to `payload` bytes, for tests that need the commitlog
// to roll to a new segment.
static raft::log_entry_ptr make_command_entry_sized(raft::term_t term, raft::index_t idx, size_t payload) {
    raft::command cmd;
    ser::serialize(cmd, int32_t(idx.value()));
    ser::serialize(cmd, bytes(bytes::initialized_later(), payload));
    return make_lw_shared<raft::log_entry>(
            raft::log_entry{.term = term, .idx = idx, .data = std::move(cmd)});
}

static raft::log_entry_ptr make_command_entry(raft::term_t term, raft::index_t idx) {
    raft::command cmd;
    ser::serialize(cmd, int32_t(idx.value()));
    return make_lw_shared<raft::log_entry>(
            raft::log_entry{.term = term, .idx = idx, .data = std::move(cmd)});
}

// A command entry with a payload of the given size, for filling segments.
static raft::log_entry_ptr make_sized_command(raft::term_t term, raft::index_t idx, size_t payload) {
    raft::command cmd;
    ser::serialize(cmd, bytes(payload, 'x'));
    return make_lw_shared<raft::log_entry>(
            raft::log_entry{.term = term, .idx = idx, .data = std::move(cmd)});
}

// Commit, apply and close everything up to now, which is what a record needs
// before it can be released.
static future<> release_everything(raft_groups_storage& storage,
        const std::vector<raft::log_entry_ptr>& entries) {
    BOOST_REQUIRE(!entries.empty());
    for (const auto& e : entries) {
        if (std::holds_alternative<raft::command>(e->data)) {
            storage.note_applied(e->idx);
        }
    }
    co_await storage.store_commit_idx(entries.back()->idx);
    storage.note_closed_up_to(db::replay_position(
            std::numeric_limits<db::segment_id_type>::max(), std::numeric_limits<db::position_type>::max()));
}

//
// Templated test implementations for common storage tests
//

template <typename StorageFactory>
future<> test_store_load_term_and_vote_impl(StorageFactory&& make_storage) {
    return do_with_cql_env_strongly_consistent([make_storage = std::forward<StorageFactory>(make_storage)] (cql_test_env& env) -> future<> {
        auto storage = make_storage(env, gid);

        raft::term_t vote_term(1);
        auto vote_id = raft::server_id::create_random_id();

        co_await storage.store_term_and_vote(vote_term, vote_id);
        auto persisted = co_await storage.load_term_and_vote();

        BOOST_CHECK_EQUAL(vote_term, persisted.first);
        BOOST_CHECK_EQUAL(vote_id, persisted.second);
    });
}

template <typename StorageFactory>
future<> test_store_load_snapshot_impl(StorageFactory&& make_storage) {
    return do_with_cql_env_strongly_consistent([make_storage = std::forward<StorageFactory>(make_storage)] (cql_test_env& env) -> future<> {
        auto storage = make_storage(env, gid);

        raft::term_t snp_term(1);
        raft::index_t snp_idx(1);
        raft::config_member srv{raft::server_address{
                raft::server_id::create_random_id(),
                ser::serialize_to_buffer<bytes>(gms::inet_address("localhost"))
            }, raft::is_voter::yes};
        raft::configuration snp_cfg({std::move(srv)});
        auto snp_id = raft::snapshot_id::create_random_id();

        raft::snapshot_descriptor snp{
            .idx = snp_idx,
            .term = snp_term,
            .config = std::move(snp_cfg),
            .id = std::move(snp_id)};

        // deliberately larger than log size to keep the log intact
        static constexpr size_t preserve_log_entries = 10;

        co_await storage.store_snapshot_descriptor(snp, preserve_log_entries);
        raft::snapshot_descriptor loaded_snp = co_await storage.load_snapshot_descriptor();

        BOOST_CHECK(snp == loaded_snp);
    });
}

template <typename StorageFactory>
future<> test_store_load_log_entries_impl(StorageFactory&& make_storage) {
    return do_with_cql_env_strongly_consistent([make_storage = std::forward<StorageFactory>(make_storage)] (cql_test_env& env) -> future<> {
        auto storage = make_storage(env, gid);

        // Empty-log round-trip: store nothing, load nothing.
        std::vector<raft::log_entry_ptr> empty_entries;
        co_await storage.store_log_entries(empty_entries);
        raft::log_entries loaded_empty = co_await storage.load_log();
        BOOST_CHECK_EQUAL(0u, loaded_empty.size());

        std::vector<raft::log_entry_ptr> entries = create_test_log();
        co_await storage.store_log_entries(entries);
        raft::log_entries loaded_entries = co_await storage.load_log();

        BOOST_CHECK_EQUAL(entries.size(), loaded_entries.size());
        for (size_t i = 0, end = entries.size(); i != end; ++i) {
            BOOST_CHECK(*entries[i] == *loaded_entries[i]);
        }
    });
}

template <typename StorageFactory>
future<> test_truncate_log_impl(StorageFactory&& make_storage) {
    return do_with_cql_env_strongly_consistent([make_storage = std::forward<StorageFactory>(make_storage)] (cql_test_env& env) -> future<> {
        auto storage = make_storage(env, gid);

        std::vector<raft::log_entry_ptr> entries = create_test_log(3, 20);
        co_await storage.store_log_entries(entries);
        // truncate the last entry from the log
        co_await storage.truncate_log(entries.back()->idx);

        raft::log_entries loaded_entries = co_await storage.load_log();
        BOOST_CHECK_EQUAL(loaded_entries.size(), entries.size() - 1);
        for (size_t i = 0, end = loaded_entries.size(); i != end; ++i) {
            BOOST_CHECK(*entries[i] == *loaded_entries[i]);
        }
    });
}

template <typename StorageFactory>
future<> test_store_snapshot_truncate_log_tail_impl(StorageFactory&& make_storage) {
    return do_with_cql_env_strongly_consistent([make_storage = std::forward<StorageFactory>(make_storage)] (cql_test_env& env) -> future<> {
        auto storage = make_storage(env, gid);

        std::vector<raft::log_entry_ptr> entries = create_test_log(3, 20);
        co_await storage.store_log_entries(entries);

        raft::term_t snp_term = entries.back()->term;
        raft::index_t snp_idx = entries.back()->idx;
        raft::config_member srv{raft::server_address{
                raft::server_id::create_random_id(),
                ser::serialize_to_buffer<bytes>(gms::inet_address("localhost"))
            }, raft::is_voter::yes};
        raft::configuration snp_cfg({std::move(srv)});
        auto snp_id = raft::snapshot_id::create_random_id();

        raft::snapshot_descriptor snp{
            .idx = snp_idx,
            .term = snp_term,
            .config = std::move(snp_cfg),
            .id = std::move(snp_id)};

        // leave the last 2 entries in the log after saving the snapshot
        static constexpr size_t preserve_log_entries = 2;

        co_await storage.store_snapshot_descriptor(snp, preserve_log_entries);
        raft::log_entries loaded_entries = co_await storage.load_log();
        BOOST_CHECK_EQUAL(loaded_entries.size(), preserve_log_entries);
        for (size_t i = 0, end = loaded_entries.size(); i != end; ++i) {
            BOOST_CHECK(*entries[entries.size() - preserve_log_entries + i] == *loaded_entries[i]);
        }
    });
}

template <typename StorageFactory>
future<> test_storage_bootstrap_impl(StorageFactory&& make_storage) {
    return do_with_cql_env_strongly_consistent([make_storage = std::forward<StorageFactory>(make_storage)] (cql_test_env& env) -> future<> {
        raft::group_id bootstrap_gid{utils::UUID_gen::get_time_UUID()};
        auto storage = make_storage(env, bootstrap_gid);

        raft::config_member srv1{raft::server_address{
                raft::server_id::create_random_id(), {}
            }, raft::is_voter::yes};
        raft::config_member srv2{raft::server_address{
                raft::server_id::create_random_id(), {}
            }, raft::is_voter::yes};
        raft::configuration initial_cfg({srv1, srv2});

        co_await storage.bootstrap(initial_cfg, false);

        auto snap = co_await storage.load_snapshot_descriptor();
        BOOST_CHECK(snap.id);
        BOOST_CHECK_EQUAL(snap.idx, raft::index_t{0});
        BOOST_CHECK_EQUAL(snap.config.current.size(), 2);
    });
}

//
// raft_sys_table_storage tests
//

SEASTAR_TEST_CASE(test_sys_table_store_load_term_and_vote) {
    return test_store_load_term_and_vote_impl(make_sys_table_storage);
}

SEASTAR_TEST_CASE(test_sys_table_store_load_snapshot) {
    return test_store_load_snapshot_impl(make_sys_table_storage);
}

SEASTAR_TEST_CASE(test_sys_table_store_load_log_entries) {
    return test_store_load_log_entries_impl(make_sys_table_storage);
}

SEASTAR_TEST_CASE(test_sys_table_truncate_log) {
    return test_truncate_log_impl(make_sys_table_storage);
}

SEASTAR_TEST_CASE(test_sys_table_store_snapshot_truncate_log_tail) {
    return test_store_snapshot_truncate_log_tail_impl(make_sys_table_storage);
}

SEASTAR_TEST_CASE(test_sys_table_storage_bootstrap) {
    return test_storage_bootstrap_impl(make_sys_table_storage);
}

//
// raft_groups_storage tests
//
// Note: raft_groups_storage uses commitlog-based persistence for log entries.
// store_log_entries() writes to the commitlog, while load_log() returns entries
// from the replay buffer provided at construction time. Tests that involve
// log entries must construct the storage with pre-populated replayed_data
// to simulate a commitlog replay cycle.
//

SEASTAR_TEST_CASE(test_groups_store_load_term_and_vote) {
    return test_store_load_term_and_vote_impl(make_groups_storage);
}

// The descriptor a strongly consistent group persists is written by its record
// releases, not by raft asking for it: store_snapshot_descriptor() is a no-op,
// and what loads is what bootstrap() (or the last release) put in the row.
SEASTAR_TEST_CASE(test_groups_store_snapshot_descriptor_is_a_noop) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        auto storage = make_groups_storage(env, gid);

        raft::config_member srv{raft::server_address{
                raft::server_id::create_random_id(), {}
            }, raft::is_voter::yes};
        co_await storage.bootstrap(raft::configuration({srv}), false);
        const auto after_bootstrap = co_await storage.load_snapshot_descriptor();
        BOOST_CHECK(bool(after_bootstrap.id));
        BOOST_CHECK_EQUAL(after_bootstrap.idx, raft::index_t(0));

        // Asking to store a descriptor at a higher index changes nothing.
        raft::snapshot_descriptor snp{
            .idx = raft::index_t(1000),
            .term = raft::term_t(7),
            .config = raft::configuration({srv}),
            .id = raft::snapshot_id::create_random_id()};
        co_await storage.store_snapshot_descriptor(snp, 10);

        const auto loaded = co_await storage.load_snapshot_descriptor();
        BOOST_CHECK_EQUAL(loaded.idx, after_bootstrap.idx);
        BOOST_CHECK_EQUAL(loaded.term, after_bootstrap.term);
        BOOST_CHECK(loaded.id == after_bootstrap.id);
    });
}

// Test that load_log returns entries from the replayed data provided at construction
SEASTAR_TEST_CASE(test_groups_load_log_from_replayed_data) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());

        std::vector<raft::log_entry_ptr> entries = create_test_log();

        // Simulate a commitlog replay by populating replayed_data_per_group
        replayed_data_per_group replayed_data;
        for (auto& e : entries) {
            replayed_data.entries.push_back(e);
        }

        raft_groups_storage storage(qp, env.local_db(), gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, std::move(replayed_data));

        raft::log_entries loaded_entries = co_await storage.load_log();

        BOOST_CHECK_EQUAL(entries.size(), loaded_entries.size());
        for (size_t i = 0, end = entries.size(); i != end; ++i) {
            BOOST_CHECK(*entries[i] == *loaded_entries[i]);
        }
    });
}

// Test that store_log_entries writes to the commitlog and truncate_log drops the
// entries at or above the truncation point: the surviving indexes still have a
// record holding them, the truncated ones do not.
SEASTAR_TEST_CASE(test_groups_truncate_log) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());

        raft_groups_storage storage(qp, env.local_db(), gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});

        std::vector<raft::log_entry_ptr> entries = create_test_log(3, 20);
        co_await storage.store_log_entries(entries);

        co_await storage.truncate_log(entries.back()->idx);

        // Every surviving index is still held by a record, so a reference for it
        // can be taken.
        for (size_t i = 0; i + 1 < entries.size(); ++i) {
            auto pin = storage.pin_for_apply(entries[i]->idx);
            BOOST_CHECK(bool(pin));
        }

        // The truncated index is not held by any record.
        {
            seastar::testing::scoped_no_abort_on_internal_error no_abort;
            try {
                storage.pin_for_apply(entries.back()->idx);
                BOOST_FAIL("Expected on_internal_error for a truncated index");
            } catch (...) {
                // Expected.
            }
        }
    });
}

// A group's descriptor tracks what its records have made durable, so storing
// log entries alone does not move it: the release does, and that is covered by
// test_groups_release_persists_the_descriptor.
SEASTAR_TEST_CASE(test_groups_descriptor_unmoved_by_log_writes) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());

        raft_groups_storage storage(qp, env.local_db(), gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});

        raft::config_member srv{raft::server_address{
                raft::server_id::create_random_id(), {}
            }, raft::is_voter::yes};
        co_await storage.bootstrap(raft::configuration({srv}), false);

        std::vector<raft::log_entry_ptr> entries = create_test_log(3, 20);
        co_await storage.store_log_entries(entries);

        // The entries are in the commitlog and held by a record, but nothing
        // has been released, so the persisted index is still the bootstrap one.
        const auto loaded = co_await storage.load_snapshot_descriptor();
        BOOST_CHECK_EQUAL(loaded.idx, raft::index_t(0));
        BOOST_CHECK_EQUAL(co_await raft_groups_storage::load_commit_idx(qp, gid, test_shard), raft::index_t(0));
    });
}

// Verify partitioner round-trip: token_for_shard -> shard_of returns the original shard
SEASTAR_TEST_CASE(test_fixed_shard_partitioner_shard_mapping) {
    for (uint16_t shard = 0; shard < 256; ++shard) {
        uint64_t group_id_hash = 0x123456789ABCDEF0ULL + shard;
        auto token = dht::fixed_shard_partitioner::token_for_shard(shard, group_id_hash);
        unsigned computed_shard = dht::fixed_shard_partitioner::shard_of(token);
        BOOST_CHECK_EQUAL(shard, computed_shard);
    }

    // Edge cases
    auto zero_hash_token = dht::fixed_shard_partitioner::token_for_shard(0, 0);
    BOOST_CHECK_EQUAL(0u, dht::fixed_shard_partitioner::shard_of(zero_hash_token));

    auto max_shard_token = dht::fixed_shard_partitioner::token_for_shard(dht::fixed_shard_partitioner::max_shard, 0xFFFFFFFFFFFFFFFFULL);
    BOOST_CHECK_EQUAL(dht::fixed_shard_partitioner::max_shard, dht::fixed_shard_partitioner::shard_of(max_shard_token));

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_groups_storage_bootstrap) {
    return test_storage_bootstrap_impl(make_groups_storage);
}

// Verify raft group storages of different shards do not interfere with each other
SEASTAR_TEST_CASE(test_groups_storage_shard_isolation) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        raft::group_id iso_gid{utils::UUID_gen::get_time_UUID()};
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());

        // Simulate replayed entries for shard 0 only. At least one entry, since
        // the release below needs an index to commit to.
        std::vector<raft::log_entry_ptr> entries = create_test_log(1, 20);
        replayed_data_per_group replayed_data0;
        for (auto& e : entries) {
            replayed_data0.entries.push_back(e);
        }

        raft_groups_storage storage0(qp, env.local_db(), iso_gid, raft::server_id::create_random_id(), 0, cl, dummy_table, std::move(replayed_data0));
        raft_groups_storage storage1(qp, env.local_db(), iso_gid, raft::server_id::create_random_id(), 1, cl, dummy_table, {});

        // Log entries - shard 0 has replayed entries, shard 1 does not
        auto loaded0 = co_await storage0.load_log();
        BOOST_CHECK_EQUAL(entries.size(), loaded0.size());

        auto loaded1 = co_await storage1.load_log();
        BOOST_CHECK_EQUAL(0u, loaded1.size());

        // Vote/term
        co_await storage0.store_term_and_vote(raft::term_t(100), raft::server_id::create_random_id());

        auto vote0 = co_await storage0.load_term_and_vote();
        BOOST_CHECK_EQUAL(raft::term_t(100), vote0.first);

        auto vote1 = co_await storage1.load_term_and_vote();
        BOOST_CHECK_EQUAL(raft::term_t{}, vote1.first);

        // Commit index: store_commit_idx() is in-memory, so it writes nothing —
        // the row only moves when a record is released.
        co_await storage0.store_commit_idx(raft::index_t(42));
        BOOST_CHECK_EQUAL(raft::index_t(0), co_await storage0.load_commit_idx());
        BOOST_CHECK_EQUAL(raft::index_t(0), co_await storage1.load_commit_idx());

        // Releasing shard 0's record writes shard 0's row and leaves shard 1's
        // alone: the two are separate partitions of system.raft_groups. This
        // shard's entries came from replay as log entries only, so give it a
        // record of its own to release.
        co_await storage0.store_log_entries(entries);
        for (const auto& e : entries) {
            if (std::holds_alternative<raft::command>(e->data)) {
                storage0.note_applied(e->idx);
            }
        }
        co_await storage0.store_commit_idx(entries.back()->idx);
        storage0.note_closed_up_to(db::replay_position(
                std::numeric_limits<db::segment_id_type>::max(), std::numeric_limits<db::position_type>::max()));
        BOOST_CHECK_EQUAL(entries.back()->idx, co_await storage0.load_commit_idx());
        BOOST_CHECK_EQUAL(raft::index_t(0), co_await storage1.load_commit_idx());
    });
}

// Test store_snapshot_index: advances persisted snapshot index atomically,
// and refuses to go backwards (guard against repeated replays).
SEASTAR_TEST_CASE(test_groups_store_snapshot_index) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());
        raft::group_id gid{utils::UUID_gen::get_time_UUID()};

        raft_groups_storage storage(qp, env.local_db(), gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});

        // Initially no snapshot
        auto snp0 = co_await storage.load_snapshot_descriptor();
        BOOST_CHECK(!snp0.id);

        // Advance snapshot index to 10
        co_await raft_groups_storage::store_snapshot_index(qp, gid, test_shard, raft::snapshot_descriptor{
            .idx = raft::index_t(10),
            .term = raft::term_t(3),
            .id = raft::snapshot_id(utils::make_random_uuid()),
        });

        auto snp1 = co_await storage.load_snapshot_descriptor();
        BOOST_CHECK_EQUAL(snp1.idx, raft::index_t(10));
        BOOST_CHECK_EQUAL(snp1.term, raft::term_t(3));

        // Advance further to 20
        co_await raft_groups_storage::store_snapshot_index(qp, gid, test_shard, raft::snapshot_descriptor{
            .idx = raft::index_t(20),
            .term = raft::term_t(4),
            .id = raft::snapshot_id(utils::make_random_uuid()),
        });

        auto snp2 = co_await storage.load_snapshot_descriptor();
        BOOST_CHECK_EQUAL(snp2.idx, raft::index_t(20));
        BOOST_CHECK_EQUAL(snp2.term, raft::term_t(4));

        // Attempt to go backwards to 15 — should be a no-op
        co_await raft_groups_storage::store_snapshot_index(qp, gid, test_shard, raft::snapshot_descriptor{
            .idx = raft::index_t(15),
            .term = raft::term_t(3),
            .id = raft::snapshot_id(utils::make_random_uuid()),
        });

        auto snp3 = co_await storage.load_snapshot_descriptor();
        BOOST_CHECK_EQUAL(snp3.idx, raft::index_t(20));
        BOOST_CHECK_EQUAL(snp3.term, raft::term_t(4));

        // Same index — should also be a no-op
        co_await raft_groups_storage::store_snapshot_index(qp, gid, test_shard, raft::snapshot_descriptor{
            .idx = raft::index_t(20),
            .term = raft::term_t(5),
            .id = raft::snapshot_id(utils::make_random_uuid()),
        });

        auto snp4 = co_await storage.load_snapshot_descriptor();
        BOOST_CHECK_EQUAL(snp4.idx, raft::index_t(20));
        BOOST_CHECK_EQUAL(snp4.term, raft::term_t(4));
    });
}

// Test store_log_entries -> pin_for_apply roundtrip: every stored index is held
// by a record, and the reference it hands out sits at the position the batch was
// really written at.
SEASTAR_TEST_CASE(test_groups_store_and_get_replay_positions) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());

        raft_groups_storage storage(qp, env.local_db(), gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});

        std::vector<raft::log_entry_ptr> entries = create_test_log(3, 20);
        co_await storage.store_log_entries(entries);

        // One batch, so one position, and every entry's reference reports it.
        std::optional<db::replay_position> batch_pos;
        for (const auto& e : entries) {
            auto pin = storage.pin_for_apply(e->idx);
            BOOST_CHECK(bool(pin));
            BOOST_CHECK_GT(pin.rp().pos, 0u);
            if (!batch_pos) {
                batch_pos = pin.rp();
            } else {
                BOOST_CHECK(pin.rp() == *batch_pos);
            }
        }
    });
}

// Test that taking a reference does not consume anything: the record keeps its
// own, so the same index can be pinned again, and the references are
// independent of each other.
SEASTAR_TEST_CASE(test_groups_get_partial_replay_positions) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());

        raft_groups_storage storage(qp, env.local_db(), gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});

        std::vector<raft::log_entry_ptr> entries = create_test_log(3, 20);
        co_await storage.store_log_entries(entries);

        const auto idx = entries.front()->idx;
        auto first = storage.pin_for_apply(idx);
        auto second = storage.pin_for_apply(idx);
        BOOST_CHECK(bool(first));
        BOOST_CHECK(bool(second));
        BOOST_CHECK(first.rp() == second.rp());

        // Dropping one leaves the other, and the record, holding the segment.
        { auto dropped = std::move(first); }
        auto third = storage.pin_for_apply(idx);
        BOOST_CHECK(bool(third));
        BOOST_CHECK(third.rp() == second.rp());
    });
}

// Test that commands interleaved with dummy and configuration entries can each
// be pinned: the record holds a whole index range, so a command's index does not
// have to be contiguous with the previous one.
SEASTAR_TEST_CASE(test_groups_acquire_handles_skips_non_command_entries) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());

        raft_groups_storage storage(qp, env.local_db(), gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});

        // A log whose commands sit at non-contiguous indexes.
        std::vector<raft::log_entry_ptr> entries;
        const uint64_t base = 100;
        for (uint64_t i = 0; i < 6; ++i) {
            const auto idx = raft::index_t(base + i);
            if (i % 2 == 0) {
                raft::command cmd;
                ser::serialize(cmd, int32_t(i));
                entries.push_back(make_lw_shared<raft::log_entry>(
                        raft::log_entry{.term = raft::term_t(1), .idx = idx, .data = std::move(cmd)}));
            } else {
                entries.push_back(make_lw_shared<raft::log_entry>(
                        raft::log_entry{.term = raft::term_t(1), .idx = idx, .data = raft::log_entry::dummy{}}));
            }
        }
        co_await storage.store_log_entries(entries);

        for (uint64_t i = 0; i < 6; i += 2) {
            auto pin = storage.pin_for_apply(raft::index_t(base + i));
            BOOST_CHECK(bool(pin));
        }
    });
}

// Test truncate_log at an index inside the record's range: the record is clamped,
// so the indexes below the cut are still held and the ones at or above it are not.
SEASTAR_TEST_CASE(test_groups_truncate_log_then_get_handles_for_remaining) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());

        raft_groups_storage storage(qp, env.local_db(), gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});

        std::vector<raft::log_entry_ptr> entries = create_test_log(3, 20);
        co_await storage.store_log_entries(entries);

        co_await storage.truncate_log(entries[1]->idx);

        auto pin = storage.pin_for_apply(entries[0]->idx);
        BOOST_CHECK(bool(pin));

        {
            seastar::testing::scoped_no_abort_on_internal_error no_abort;
            for (size_t i = 1; i < entries.size(); ++i) {
                try {
                    storage.pin_for_apply(entries[i]->idx);
                    BOOST_FAIL("Expected on_internal_error for a truncated index");
                } catch (...) {
                    // Expected.
                }
            }
        }
    });
}

// Test the release: once every index in a record is committed, its commands
// applied, and its segment closed, the record's (max, term) becomes the group's
// persisted descriptor — written by a mutation, so it is readable straight back
// out of system.raft_groups — and the record is gone.
SEASTAR_TEST_CASE(test_groups_release_persists_the_descriptor) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());

        raft_groups_storage storage(qp, env.local_db(), gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});

        raft::config_member srv{raft::server_address{
                raft::server_id::create_random_id(), {}
            }, raft::is_voter::yes};
        co_await storage.bootstrap(raft::configuration({srv}), false);

        std::vector<raft::log_entry_ptr> entries = create_test_log(3, 20);
        co_await storage.store_log_entries(entries);

        // Committed, but the segment is still the newest and still open, so the
        // record cannot be released yet and its entries stay pinned.
        co_await storage.store_commit_idx(entries.back()->idx);
        for (const auto& e : entries) {
            if (std::holds_alternative<raft::command>(e->data)) {
                storage.note_applied(e->idx);
            }
        }
        {
            auto pin = storage.pin_for_apply(entries.front()->idx);
            BOOST_CHECK(bool(pin));
        }

        // Once the commitlog reports the segment closed, the record is final and
        // is released.
        storage.note_closed_up_to(db::replay_position(
                std::numeric_limits<db::segment_id_type>::max(), std::numeric_limits<db::position_type>::max()));

        {
            seastar::testing::scoped_no_abort_on_internal_error no_abort;
            try {
                storage.pin_for_apply(entries.front()->idx);
                BOOST_FAIL("Expected the record to have been released");
            } catch (...) {
                // Expected: no record holds the index any more.
            }
        }

        // The descriptor the release wrote is in the row, and it is the record's
        // own (max, term).
        const auto persisted = co_await raft_groups_storage::load_commit_idx(qp, gid, test_shard);
        BOOST_CHECK_EQUAL(persisted, entries.back()->idx);
        const auto snap = co_await storage.load_snapshot_descriptor();
        BOOST_CHECK_EQUAL(snap.idx, entries.back()->idx);
        BOOST_CHECK_EQUAL(snap.term, entries.back()->term);
        BOOST_CHECK(bool(snap.id));
    });
}

// Test: truncate_log() clamps the record it lands inside and persists a record
// of what it discarded. The entries stay on disk — the commitlog is append-only
// — so that persisted record is the only thing that tells a later replay which
// copies of 6..10 are superseded.
SEASTAR_TEST_CASE(test_groups_truncate_persists_the_truncation_record) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());
        raft::group_id gid{utils::UUID_gen::get_time_UUID()};

        raft_groups_storage storage(qp, env.local_db(), gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});
        raft::config_member srv{raft::server_address{raft::server_id::create_random_id(), {}}, raft::is_voter::yes};
        co_await storage.bootstrap(raft::configuration({srv}), false);

        std::vector<raft::log_entry_ptr> entries;
        for (int i = 1; i <= 10; ++i) {
            entries.push_back(make_command_entry(raft::term_t(1), raft::index_t(i)));
        }
        co_await storage.store_log_entries(entries);

        // A new leader overwrites from 6.
        co_await storage.truncate_log(raft::index_t(6));

        // 1..5 survive, so the record is still there and still holds them.
        {
            auto pin = storage.pin_for_apply(raft::index_t(5));
            BOOST_CHECK(bool(pin));
        }

        // Releasing what survived writes both the clamped index and the
        // truncation record.
        std::vector<raft::log_entry_ptr> survivors(entries.begin(), entries.begin() + 5);
        co_await release_everything(storage, survivors);

        const auto persisted = co_await raft_groups_storage::load_descriptor(qp, gid, test_shard);
        BOOST_CHECK(persisted.exists);
        BOOST_CHECK_EQUAL(persisted.idx, raft::index_t(5));
        BOOST_REQUIRE_EQUAL(persisted.truncations.size(), 1);
        BOOST_CHECK_EQUAL(persisted.truncations[0].from, raft::index_t(6));
        BOOST_CHECK_EQUAL(persisted.truncations[0].to, raft::index_t(10));
        BOOST_CHECK_GT(persisted.truncations[0].segment, 0u);
    });
}

// Test: a truncation reaching back past the newest segment pops those records
// whole, one truncation record each, and clamps the one it lands inside. All of
// them reach the row. Their order there does not matter: replay keys a cursor on
// each record's own segment, and within one segment the records still arrive in
// the order they were made. One truncate_log() call appends newest segment
// first, since it pops the queue from the back.
SEASTAR_TEST_CASE(test_groups_truncate_pops_whole_records) {
    return do_with_cql_env_small_segments([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());
        raft::group_id gid{utils::UUID_gen::get_time_UUID()};

        raft_groups_storage storage(qp, env.local_db(), gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});
        raft::config_member srv{raft::server_address{raft::server_id::create_random_id(), {}}, raft::is_voter::yes};
        co_await storage.bootstrap(raft::configuration({srv}), false);

        // Fill more than one 1MB segment, four 64KB entries per batch, so the
        // group ends up with several records.
        std::vector<raft::log_entry_ptr> all;
        raft::index_t next{1};
        for (int batch = 0; batch < 8; ++batch) {
            std::vector<raft::log_entry_ptr> entries;
            for (int i = 0; i < 4; ++i) {
                entries.push_back(make_sized_command(raft::term_t(1), next, 64 * 1024));
                next = next + raft::index_t{1};
            }
            co_await storage.store_log_entries(entries);
            all.insert(all.end(), entries.begin(), entries.end());
        }

        // Truncate deep enough to invalidate several records: from index 6,
        // which is inside the first batch.
        const auto cut = raft::index_t(6);
        co_await storage.truncate_log(cut);

        // Everything from the cut on is gone from the log...
        {
            seastar::testing::scoped_no_abort_on_internal_error no_abort;
            try {
                storage.pin_for_apply(cut);
                BOOST_FAIL("Expected no record to hold a truncated index");
            } catch (...) {
            }
        }
        // ...and 1..5 are still held.
        {
            auto pin = storage.pin_for_apply(raft::index_t(5));
            BOOST_CHECK(bool(pin));
        }

        std::vector<raft::log_entry_ptr> survivors(all.begin(), all.begin() + 5);
        co_await release_everything(storage, survivors);

        const auto persisted = co_await raft_groups_storage::load_descriptor(qp, gid, test_shard);
        BOOST_CHECK_EQUAL(persisted.idx, raft::index_t(5));
        // Several records were invalidated, since the entries spanned several
        // segments. Within one truncate_log() call they are appended
        // newest-range-first, because the queue is popped from the back; what
        // has to hold is that together they tile exactly what was discarded,
        // one record per segment. Replay does not depend on the order across
        // segments either, since it matches cursors per segment.
        BOOST_REQUIRE_GT(persisted.truncations.size(), 1);
        auto sorted = persisted.truncations;
        std::ranges::sort(sorted, {}, &service::strong_consistency::truncation_record::from);
        BOOST_CHECK_EQUAL(sorted.front().from, cut);
        BOOST_CHECK_EQUAL(sorted.back().to, all.back()->idx);
        raft::index_t expected_next = cut;
        std::unordered_set<db::segment_id_type> segments;
        for (const auto& t : sorted) {
            BOOST_CHECK_EQUAL(t.from, expected_next);
            BOOST_CHECK_LE(t.from, t.to);
            BOOST_CHECK_GT(t.segment, 0u);
            // One record per segment: a segment's live tail is one range.
            BOOST_CHECK(segments.insert(t.segment).second);
            expected_next = t.to + raft::index_t{1};
        }
        // Ranges ascend with segment ids, since later entries went to later
        // segments.
        for (size_t i = 0; i + 1 < sorted.size(); ++i) {
            BOOST_CHECK_LT(sorted[i].segment, sorted[i + 1].segment);
        }
    });
}

// Test: truncating everything leaves no record to release, so the truncation
// records wait in memory and are written by the next release. Losing them would
// leave a replay unable to tell which copies of those indexes were superseded.
SEASTAR_TEST_CASE(test_groups_truncations_survive_until_the_next_release) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());
        raft::group_id gid{utils::UUID_gen::get_time_UUID()};

        raft_groups_storage storage(qp, env.local_db(), gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});
        raft::config_member srv{raft::server_address{raft::server_id::create_random_id(), {}}, raft::is_voter::yes};
        co_await storage.bootstrap(raft::configuration({srv}), false);

        std::vector<raft::log_entry_ptr> first;
        for (int i = 1; i <= 4; ++i) {
            first.push_back(make_command_entry(raft::term_t(1), raft::index_t(i)));
        }
        co_await storage.store_log_entries(first);

        // The new leader discards the whole log, so there is nothing left to
        // release and the row is untouched for now.
        co_await storage.truncate_log(raft::index_t(1));
        auto after_truncate = co_await raft_groups_storage::load_descriptor(qp, gid, test_shard);
        BOOST_CHECK_EQUAL(after_truncate.idx, raft::index_t(0));
        BOOST_CHECK(after_truncate.truncations.empty());

        // It then appends its own entries at the same indexes.
        std::vector<raft::log_entry_ptr> second;
        for (int i = 1; i <= 3; ++i) {
            second.push_back(make_command_entry(raft::term_t(2), raft::index_t(i)));
        }
        co_await storage.store_log_entries(second);
        co_await release_everything(storage, second);

        // The release carries the earlier truncation to disk, alongside the new
        // leader's own index and term.
        const auto persisted = co_await raft_groups_storage::load_descriptor(qp, gid, test_shard);
        BOOST_CHECK_EQUAL(persisted.idx, raft::index_t(3));
        BOOST_CHECK_EQUAL(persisted.term, raft::term_t(2));
        BOOST_REQUIRE_EQUAL(persisted.truncations.size(), 1);
        BOOST_CHECK_EQUAL(persisted.truncations[0].from, raft::index_t(1));
        BOOST_CHECK_EQUAL(persisted.truncations[0].to, raft::index_t(4));
    });
}

// Test: a release leaves the truncations cell alone when the history has not
// moved. A release rewrites the whole list, which grows with leader changes, so
// rewriting it unconditionally would put an ever-longer cell on disk for a
// history that did not change. The cell is independent of the others, so
// skipping it keeps the last written value.
SEASTAR_TEST_CASE(test_groups_unchanged_truncations_are_not_rewritten) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());
        raft::group_id gid{utils::UUID_gen::get_time_UUID()};

        raft_groups_storage storage(qp, env.local_db(), gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});
        raft::config_member srv{raft::server_address{raft::server_id::create_random_id(), {}}, raft::is_voter::yes};
        co_await storage.bootstrap(raft::configuration({srv}), false);

        // A truncation, then a release that carries it to the row.
        std::vector<raft::log_entry_ptr> first;
        for (int i = 1; i <= 4; ++i) {
            first.push_back(make_command_entry(raft::term_t(1), raft::index_t(i)));
        }
        co_await storage.store_log_entries(first);
        co_await storage.truncate_log(raft::index_t(1));

        std::vector<raft::log_entry_ptr> second;
        for (int i = 1; i <= 3; ++i) {
            second.push_back(make_command_entry(raft::term_t(2), raft::index_t(i)));
        }
        co_await storage.store_log_entries(second);
        co_await release_everything(storage, second);

        const auto writetime_of_truncations = [&] () -> future<int64_t> {
            const auto cql = format("SELECT WRITETIME(truncations) AS wt FROM system.{} "
                    "WHERE shard = ? AND group_id = ?", db::system_keyspace::RAFT_GROUPS);
            auto rs = co_await qp.execute_internal(cql, {int16_t(test_shard), gid.id},
                    cql3::query_processor::cache_internal::no);
            BOOST_REQUIRE(!rs->empty());
            co_return rs->one().get_as<api::timestamp_type>("wt");
        };

        const auto after_first = co_await writetime_of_truncations();

        // A second release, with no truncation in between. The descriptor moves,
        // the truncation history does not.
        std::vector<raft::log_entry_ptr> third;
        for (int i = 4; i <= 6; ++i) {
            third.push_back(make_command_entry(raft::term_t(2), raft::index_t(i)));
        }
        co_await storage.store_log_entries(third);
        co_await release_everything(storage, third);

        const auto persisted = co_await raft_groups_storage::load_descriptor(qp, gid, test_shard);
        BOOST_CHECK_EQUAL(persisted.idx, raft::index_t(6));
        // The history is still there, read back intact...
        BOOST_REQUIRE_EQUAL(persisted.truncations.size(), 1);
        BOOST_CHECK_EQUAL(persisted.truncations[0].from, raft::index_t(1));
        BOOST_CHECK_EQUAL(persisted.truncations[0].to, raft::index_t(4));
        // ...but the cell was not written again.
        BOOST_CHECK_EQUAL(co_await writetime_of_truncations(), after_first);
    });
}

// Test: a batch whose commands are never applied must not wedge the queue.
//
// state_machine::apply() swallows no_such_column_family / no_such_keyspace when
// a DROP races the applier, and the release gate waits for the record's last
// command — so without note_batch_discarded() the record stays at the front for
// good, and the next batch's pin_for_apply() asks for an index past it, which is
// an internal error. This checks that call is what keeps the queue moving.
SEASTAR_TEST_CASE(test_groups_discarded_batch_does_not_wedge_the_queue) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());
        raft::group_id gid{utils::UUID_gen::get_time_UUID()};

        raft_groups_storage storage(qp, env.local_db(), gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});
        raft::config_member srv{raft::server_address{raft::server_id::create_random_id(), {}}, raft::is_voter::yes};
        co_await storage.bootstrap(raft::configuration({srv}), false);

        // A batch whose commands the applier will drop on the floor. Large
        // enough to fill the 1MB segment, so the next batch lands in a new one
        // and therefore in a *different* record — which is the whole point: the
        // front record must be released before the next batch can be pinned.
        std::vector<raft::log_entry_ptr> dropped;
        raft::index_t next_idx{1};
        while (cl.get_num_dirty_segments() == 0) {
            std::vector<raft::log_entry_ptr> batch;
            for (int i = 0; i < 4; ++i) {
                batch.push_back(make_command_entry_sized(raft::term_t(1), next_idx, 64 * 1024));
                next_idx = next_idx + raft::index_t{1};
            }
            co_await storage.store_log_entries(batch);
            for (auto& e : batch) {
                dropped.push_back(e);
            }
        }
        BOOST_REQUIRE(!dropped.empty());

        // Committed and closed, but nothing applied: the gate waits for the last
        // command, so the front record cannot be released.
        co_await storage.store_commit_idx(dropped.back()->idx);
        storage.note_closed_up_to(db::replay_position(
                std::numeric_limits<db::segment_id_type>::max(), std::numeric_limits<db::position_type>::max()));
        const auto stuck = co_await raft_groups_storage::load_descriptor(qp, gid, test_shard);
        BOOST_CHECK_EQUAL(stuck.idx, raft::index_t(0));

        // This is the call the swallow paths make.
        storage.note_applied(dropped.back()->idx);

        // The front record is gone, so the next batch — which lives in a later
        // segment, and so a later record — can be pinned instead of tripping
        // pin_for_apply()'s internal error.
        std::vector<raft::log_entry_ptr> following;
        for (int i = 0; i < 3; ++i) {
            following.push_back(make_command_entry(raft::term_t(1), next_idx));
            next_idx = next_idx + raft::index_t{1};
        }
        co_await storage.store_log_entries(following);
        BOOST_REQUIRE(bool(storage.pin_for_apply(following.front()->idx)));

        const auto released = co_await raft_groups_storage::load_descriptor(qp, gid, test_shard);
        BOOST_CHECK_EQUAL(released.idx, dropped.back()->idx);
    });
}

