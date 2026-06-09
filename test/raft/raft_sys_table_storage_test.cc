/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/testing/test_case.hh>
#include <seastar/testing/on_internal_error.hh>
#include <seastar/core/coroutine.hh>

#include "db/config.hh"
#include "raft/raft.hh"
#include "utils/UUID_gen.hh"

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

// Create a test log with entries of each kind to test that these get
// serialized/deserialized properly
static std::vector<raft::log_entry_ptr> create_test_log() {
    raft::command cmd;
    ser::serialize(cmd, 123);

    return {
        // command
        make_lw_shared(raft::log_entry{
            .term = raft::term_t(1),
            .idx = raft::index_t(1),
            .data = std::move(cmd)}),
        // configuration
        make_lw_shared(raft::log_entry{
            .term = raft::term_t(2),
            .idx = raft::index_t(2),
            .data = raft::configuration{{raft::config_member{raft::server_address{raft::server_id::create_random_id(), {}}, raft::is_voter::yes}}}}),
        // dummy
        make_lw_shared(raft::log_entry{
            .term = raft::term_t(3),
            .idx = raft::index_t(3),
            .data = raft::log_entry::dummy()})
    };
}

// Factory functions to create storage instances with uniform interface
static service::raft_sys_table_storage make_sys_table_storage(cql_test_env& env, raft::group_id group_id) {
    return service::raft_sys_table_storage(env.local_qp(), group_id, raft::server_id::create_random_id());
}

static raft_groups_storage make_groups_storage(cql_test_env& env, raft::group_id group_id) {
    return raft_groups_storage(env.local_qp(), group_id, raft::server_id::create_random_id(), test_shard,
            *env.local_db().commitlog(), table_id(utils::UUID_gen::get_time_UUID()), {});
}

static future<> do_with_cql_env_strongly_consistent(std::function<future<>(cql_test_env&)> func) {
    auto db_cfg_ptr = make_shared<db::config>();
    auto& db_cfg = *db_cfg_ptr;
    db_cfg.experimental_features({db::experimental_features_t::feature::STRONGLY_CONSISTENT_TABLES});
    return do_with_cql_env(std::move(func), std::move(db_cfg_ptr));
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

        std::vector<raft::log_entry_ptr> entries = create_test_log();
        co_await storage.store_log_entries(entries);
        // truncate the last entry from the log
        co_await storage.truncate_log(raft::index_t(3));

        raft::log_entries loaded_entries = co_await storage.load_log();
        BOOST_CHECK_EQUAL(loaded_entries.size(), 2);
        for (size_t i = 0, end = loaded_entries.size(); i != end; ++i) {
            BOOST_CHECK(*entries[i] == *loaded_entries[i]);
        }
    });
}

template <typename StorageFactory>
future<> test_store_snapshot_truncate_log_tail_impl(StorageFactory&& make_storage) {
    return do_with_cql_env_strongly_consistent([make_storage = std::forward<StorageFactory>(make_storage)] (cql_test_env& env) -> future<> {
        auto storage = make_storage(env, gid);

        std::vector<raft::log_entry_ptr> entries = create_test_log();
        co_await storage.store_log_entries(entries);

        raft::term_t snp_term(3);
        raft::index_t snp_idx(3);
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
            BOOST_CHECK(*entries[i + 1] == *loaded_entries[i]);
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

SEASTAR_TEST_CASE(test_groups_store_load_snapshot) {
    return test_store_load_snapshot_impl(make_groups_storage);
}

// Test that load_log returns entries from the replayed data provided at construction
SEASTAR_TEST_CASE(test_groups_store_load_log_entries) {
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

        raft_groups_storage storage(qp, gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, std::move(replayed_data));

        raft::log_entries loaded_entries = co_await storage.load_log();

        BOOST_CHECK_EQUAL(entries.size(), loaded_entries.size());
        for (size_t i = 0, end = entries.size(); i != end; ++i) {
            BOOST_CHECK(*entries[i] == *loaded_entries[i]);
        }
    });
}

// Test that store_log_entries writes to the commitlog and truncate_log removes
// entries from the in-memory replay position map
SEASTAR_TEST_CASE(test_groups_truncate_log) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());

        raft_groups_storage storage(qp, gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});

        std::vector<raft::log_entry_ptr> entries = create_test_log();
        co_await storage.store_log_entries(entries);

        // Truncate the last entry from the log (index >= 3)
        co_await storage.truncate_log(raft::index_t(3));

        // Verify we can get replay position handles only for the remaining entries (indices 1, 2)
        // After truncation at index 3, only entries with index 1 and 2 remain
        raft::log_entry_ptr_list entries_to_get(entries.begin(), entries.begin() + 2);
        auto handles = storage.acquire_replay_position_handles_for(entries_to_get);
        BOOST_CHECK_EQUAL(handles.size(), 2);
        BOOST_CHECK_EQUAL(handles[0].index, raft::index_t(1));
        BOOST_CHECK_EQUAL(handles[1].index, raft::index_t(2));
    });
}

// Test that store_snapshot_descriptor works correctly (snapshot metadata is CQL-based)
SEASTAR_TEST_CASE(test_groups_store_load_snapshot_descriptor) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());

        std::vector<raft::log_entry_ptr> entries = create_test_log();

        // Simulate replayed entries so load_log returns them
        replayed_data_per_group replayed_data;
        for (auto& e : entries) {
            replayed_data.entries.push_back(e);
        }

        raft_groups_storage storage(qp, gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, std::move(replayed_data));

        // Also store entries to have replay position handles in the commitlog
        co_await storage.store_log_entries(entries);

        raft::term_t snp_term(3);
        raft::index_t snp_idx(3);
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

        co_await storage.store_snapshot_descriptor(snp, 2 /* preserve_log_entries */);

        // Verify the snapshot was stored correctly
        raft::snapshot_descriptor loaded_snp = co_await storage.load_snapshot_descriptor();
        BOOST_CHECK(snp == loaded_snp);
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

        // Simulate replayed entries for shard 0 only
        std::vector<raft::log_entry_ptr> entries = create_test_log();
        replayed_data_per_group replayed_data0;
        for (auto& e : entries) {
            replayed_data0.entries.push_back(e);
        }

        raft_groups_storage storage0(qp, iso_gid, raft::server_id::create_random_id(), 0, cl, dummy_table, std::move(replayed_data0));
        raft_groups_storage storage1(qp, iso_gid, raft::server_id::create_random_id(), 1, cl, dummy_table, {});

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

        // Commit index
        co_await storage0.store_commit_idx(raft::index_t(42));

        auto idx0 = co_await storage0.load_commit_idx();
        BOOST_CHECK_EQUAL(raft::index_t(42), idx0);

        auto idx1 = co_await storage1.load_commit_idx();
        BOOST_CHECK_EQUAL(raft::index_t(0), idx1);
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

        raft_groups_storage storage(qp, gid, raft::server_id::create_random_id(), test_shard,
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

// Test store_log_entries -> acquire_replay_position_handles_for roundtrip:
// entries written to the commitlog produce valid replay position handles.
SEASTAR_TEST_CASE(test_groups_store_and_get_replay_positions) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());

        raft_groups_storage storage(qp, gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});

        std::vector<raft::log_entry_ptr> entries = create_test_log();
        co_await storage.store_log_entries(entries);

        // Acquire replay position handles for all entries.
        // The handles are moved out of the internal map, transferring
        // ownership to the caller (for attaching to memtables on apply).
        raft::log_entry_ptr_list entries_to_get(entries.begin(), entries.end());
        auto handles = storage.acquire_replay_position_handles_for(entries_to_get);
        BOOST_CHECK_EQUAL(handles.size(), 3);
        BOOST_CHECK_EQUAL(handles[0].index, raft::index_t(1));
        BOOST_CHECK_EQUAL(handles[1].index, raft::index_t(2));
        BOOST_CHECK_EQUAL(handles[2].index, raft::index_t(3));

    });
}

// Test that acquire_replay_position_handles_for returns a partial set of handles
// when called with an index less than the maximum stored index.
SEASTAR_TEST_CASE(test_groups_get_partial_replay_positions) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());

        raft_groups_storage storage(qp, gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});

        std::vector<raft::log_entry_ptr> entries = create_test_log();
        co_await storage.store_log_entries(entries);

        // Get handles only for the first 2 entries
        raft::log_entry_ptr_list entries_batch1(entries.begin(), entries.begin() + 2);
        auto handles1 = storage.acquire_replay_position_handles_for(entries_batch1);
        BOOST_CHECK_EQUAL(handles1.size(), 2);
        BOOST_CHECK_EQUAL(handles1[0].index, raft::index_t(1));
        BOOST_CHECK_EQUAL(handles1[1].index, raft::index_t(2));

        // Get a clone for index 3 only. The map still has all 3 originals.
        raft::log_entry_ptr_list entries_batch2(entries.begin() + 2, entries.end());
        auto handles2 = storage.acquire_replay_position_handles_for(entries_batch2);
        BOOST_CHECK_EQUAL(handles2.size(), 1);
        BOOST_CHECK_EQUAL(handles2[0].index, raft::index_t(3));
    });
}

// Test that acquire_replay_position_handles_for correctly handles non-contiguous
// command entries when configuration/dummy entries are interleaved.
// This simulates what happens in practice: the raft server filters out non-command
// entries before calling state_machine::apply(), so acquire_replay_position_handles_for
// receives only command entries which may have gaps in their indices.
SEASTAR_TEST_CASE(test_groups_acquire_handles_skips_non_command_entries) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());

        raft_groups_storage storage(qp, gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});

        // Create a log with interleaved entry types:
        // idx=1: command, idx=2: configuration, idx=3: command, idx=4: dummy, idx=5: command
        raft::command cmd1, cmd3, cmd5;
        ser::serialize(cmd1, 100);
        ser::serialize(cmd3, 300);
        ser::serialize(cmd5, 500);

        std::vector<raft::log_entry_ptr> entries = {
            make_lw_shared(raft::log_entry{.term = raft::term_t(1), .idx = raft::index_t(1), .data = std::move(cmd1)}),
            make_lw_shared(raft::log_entry{.term = raft::term_t(1), .idx = raft::index_t(2),
                .data = raft::configuration{{raft::config_member{raft::server_address{raft::server_id::create_random_id(), {}}, raft::is_voter::yes}}}}),
            make_lw_shared(raft::log_entry{.term = raft::term_t(1), .idx = raft::index_t(3), .data = std::move(cmd3)}),
            make_lw_shared(raft::log_entry{.term = raft::term_t(1), .idx = raft::index_t(4), .data = raft::log_entry::dummy()}),
            make_lw_shared(raft::log_entry{.term = raft::term_t(1), .idx = raft::index_t(5), .data = std::move(cmd5)}),
        };

        co_await storage.store_log_entries(entries);

        // Simulate what the raft server does: filter only command entries for apply().
        raft::log_entry_ptr_list command_entries;
        for (auto& e : entries) {
            if (std::holds_alternative<raft::command>(e->data)) {
                command_entries.push_back(e);
            }
        }
        BOOST_CHECK_EQUAL(command_entries.size(), 3);

        // acquire_replay_position_handles_for should succeed even though
        // the command indices (1, 3, 5) are not contiguous in the replay position list.
        auto handles = storage.acquire_replay_position_handles_for(command_entries);
        BOOST_CHECK_EQUAL(handles.size(), 3);
        BOOST_CHECK_EQUAL(handles[0].index, raft::index_t(1));
        BOOST_CHECK_EQUAL(handles[1].index, raft::index_t(3));
        BOOST_CHECK_EQUAL(handles[2].index, raft::index_t(5));
    });
}

// Test truncate_log (head truncation) then verify remaining handles.
// Store 3 entries, truncate at idx 2 (removes entries with idx >= 2),
// verify only entry 1 can produce a valid handle, and entries 2-3 cannot.
SEASTAR_TEST_CASE(test_groups_truncate_log_then_get_handles_for_remaining) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());

        raft_groups_storage storage(qp, gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});

        std::vector<raft::log_entry_ptr> entries = create_test_log();
        co_await storage.store_log_entries(entries);

        // Truncate at idx 2 — entries with idx >= 2 should be removed.
        co_await storage.truncate_log(raft::index_t(2));

        // Entry 1 should still have a valid handle.
        raft::log_entry_ptr_list first_entry = {entries[0]};
        auto handles = storage.acquire_replay_position_handles_for(first_entry);
        BOOST_CHECK_EQUAL(handles.size(), 1);
        BOOST_CHECK_EQUAL(handles[0].index, raft::index_t(1));

        // Entries 2-3 should trigger an error (their replay positions were removed).
        {
            seastar::testing::scoped_no_abort_on_internal_error no_abort;
            raft::log_entry_ptr_list truncated_entries(entries.begin() + 1, entries.end());
            try {
                storage.acquire_replay_position_handles_for(truncated_entries);
                BOOST_FAIL("Expected on_internal_error for truncated entries");
            } catch (...) {
                // Expected — entries 2-3 were truncated.
            }
        }
    });
}

// Test store_snapshot_descriptor (which truncates the log tail) then verify handles.
// Store 3 entries, store a snapshot at idx 2 with preserve_log_entries=1
// (which truncates entries with idx <= 2-1 = 1), verify entry 1 is gone
// but entries 2-3 are still accessible.
SEASTAR_TEST_CASE(test_groups_store_snapshot_truncate_tail_then_get_handles) {
    return do_with_cql_env_strongly_consistent([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto& cl = *env.local_db().commitlog();
        auto dummy_table = table_id(utils::UUID_gen::get_time_UUID());

        raft_groups_storage storage(qp, gid, raft::server_id::create_random_id(), test_shard,
                cl, dummy_table, {});

        // Bootstrap so we can store a snapshot later.
        raft::config_member srv{raft::server_address{
                raft::server_id::create_random_id(), {}
            }, raft::is_voter::yes};
        co_await storage.bootstrap(raft::configuration({srv}), false);

        std::vector<raft::log_entry_ptr> entries = create_test_log();
        co_await storage.store_log_entries(entries);

        // Store snapshot at idx=2 with preserve_log_entries=1.
        // This truncates the tail: entries with idx <= (2 - 1) = 1 are removed.
        raft::snapshot_descriptor snp{
            .idx = raft::index_t(2),
            .term = raft::term_t(2),
            .config = raft::configuration({srv}),
            .id = raft::snapshot_id::create_random_id()};
        co_await storage.store_snapshot_descriptor(snp, 1 /* preserve_log_entries */);

        // Entries 2-3 should still have valid handles.
        raft::log_entry_ptr_list remaining(entries.begin() + 1, entries.end());
        auto handles = storage.acquire_replay_position_handles_for(remaining);
        BOOST_CHECK_EQUAL(handles.size(), 2);
        BOOST_CHECK_EQUAL(handles[0].index, raft::index_t(2));
        BOOST_CHECK_EQUAL(handles[1].index, raft::index_t(3));

        // Entry 1 should trigger an error (tail-truncated by snapshot).
        {
            seastar::testing::scoped_no_abort_on_internal_error no_abort;
            raft::log_entry_ptr_list truncated_entry = {entries[0]};
            try {
                storage.acquire_replay_position_handles_for(truncated_entry);
                BOOST_FAIL("Expected on_internal_error for tail-truncated entry");
            } catch (...) {
                // Expected — entry 1 was tail-truncated by snapshot.
            }
        }
    });
}
