/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/testing/test_case.hh>
#include <seastar/core/coroutine.hh>

#include "db/config.hh"
#include "raft/raft.hh"
#include "utils/UUID_gen.hh"

#include "service/raft/raft_sys_table_storage.hh"
#include "service/strong_consistency/raft_groups_storage.hh"
#include "dht/fixed_shard.hh"

#include "test/lib/cql_test_env.hh"
#include "cql3/query_processor.hh"

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
static service::raft_sys_table_storage make_sys_table_storage(cql3::query_processor& qp, raft::group_id group_id) {
    return service::raft_sys_table_storage(qp, group_id, raft::server_id::create_random_id());
}

static raft_groups_storage make_groups_storage(cql3::query_processor& qp, raft::group_id group_id) {
    return raft_groups_storage(qp, group_id, raft::server_id::create_random_id(), test_shard);
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
        cql3::query_processor& qp = env.local_qp();
        auto storage = make_storage(qp, gid);

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
        cql3::query_processor& qp = env.local_qp();
        auto storage = make_storage(qp, gid);

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
        cql3::query_processor& qp = env.local_qp();
        auto storage = make_storage(qp, gid);

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
        cql3::query_processor& qp = env.local_qp();
        auto storage = make_storage(qp, gid);

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
        cql3::query_processor& qp = env.local_qp();
        auto storage = make_storage(qp, gid);

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
        cql3::query_processor& qp = env.local_qp();
        raft::group_id bootstrap_gid{utils::UUID_gen::get_time_UUID()};
        auto storage = make_storage(qp, bootstrap_gid);

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

SEASTAR_TEST_CASE(test_groups_store_load_term_and_vote) {
    return test_store_load_term_and_vote_impl(make_groups_storage);
}

SEASTAR_TEST_CASE(test_groups_store_load_snapshot) {
    return test_store_load_snapshot_impl(make_groups_storage);
}

SEASTAR_TEST_CASE(test_groups_store_load_log_entries) {
    return test_store_load_log_entries_impl(make_groups_storage);
}

SEASTAR_TEST_CASE(test_groups_truncate_log) {
    return test_truncate_log_impl(make_groups_storage);
}

SEASTAR_TEST_CASE(test_groups_store_snapshot_truncate_log_tail) {
    return test_store_snapshot_truncate_log_tail_impl(make_groups_storage);
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

        raft_groups_storage storage0(qp, iso_gid, raft::server_id::create_random_id(), 0);
        raft_groups_storage storage1(qp, iso_gid, raft::server_id::create_random_id(), 1);

        // Log entries
        std::vector<raft::log_entry_ptr> entries = create_test_log();
        co_await storage0.store_log_entries(entries);

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
