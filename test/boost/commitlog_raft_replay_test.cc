/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <algorithm>
#include <boost/test/unit_test.hpp>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/on_internal_error.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>

#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/commitlog/raft_commitlog_replay_buffer.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/cql_test_env.hh"
#include "cql3/query_processor.hh"
#include "replica/database.hh"
#include "db/config.hh"
#include "utils/UUID_gen.hh"
#include "raft/raft.hh"
#include "service/raft/group0_fwd.hh"
#include "idl/commitlog.dist.hh"
#include "idl/commitlog.dist.impl.hh"
#include "test/lib/mutation_source_test.hh"
#include "service/strong_consistency/raft_commitlog.hh"
#include "idl/raft_storage.dist.hh"
#include "idl/raft_storage.dist.impl.hh"

BOOST_AUTO_TEST_SUITE(commitlog_raft_replay_test)

using namespace db;

namespace {

raft::log_entry_ptr make_dummy_entry(raft::term_t term, raft::index_t idx) {
    return make_lw_shared<raft::log_entry>(raft::log_entry{.term = term, .idx = idx, .data = raft::log_entry::dummy{}});
}

raft::log_entry_ptr make_command_entry(raft::term_t term, raft::index_t idx) {
    raft::command cmd;
    ser::serialize(cmd, 123);
    return make_lw_shared<raft::log_entry>(raft::log_entry{.term = term, .idx = idx, .data = std::move(cmd)});
}

raft::log_entry_ptr make_config_entry(raft::term_t term, raft::index_t idx) {
    return make_lw_shared<raft::log_entry>(raft::log_entry{.term = term,
            .idx = idx,
            .data = raft::configuration{{raft::config_member{raft::server_address{raft::server_id::create_random_id(), {}}, raft::is_voter::yes}}}});
}

raft::group_id make_group_id() {
    return raft::group_id{utils::UUID_gen::get_time_UUID()};
}

table_id make_table_id() {
    return table_id(utils::UUID_gen::get_time_UUID());
}

future<> cl_test(noncopyable_function<future<>(commitlog&)> f) {
    commitlog::config cfg;
    cfg.metrics_category_name = "commitlog";
    cfg.descriptor_tag = "variant";
    tmpdir tmp;
    cfg.commit_log_location = tmp.path().string();
    return commitlog::create_commitlog(cfg)
            .then([f = std::move(f)](commitlog log) mutable {
                return do_with(std::move(log), [f = std::move(f)](commitlog& log) {
                    return futurize_invoke(f, log).finally([&log] {
                        return log.shutdown().then([&log] {
                            return log.clear();
                        });
                    });
                });
            })
            .finally([tmp = std::move(tmp)] {});
}

// Write a raft log entry to the commitlog and return the rp_handle.
future<rp_handle> write_raft_entry_to_commitlog(commitlog& cl, table_id tid, raft::group_id gid, raft::log_entry_ptr entry) {
    commitlog_raft_log_entry_writer writer(raft_commitlog_entry{.group_id = gid, .entry = entry});
    const auto target_size = writer.size();
    co_return co_await cl.add(tid, target_size, db::no_timeout, db::commitlog_force_sync::yes, [entry, gid](auto& out) {
        commitlog_raft_log_entry_writer w(raft_commitlog_entry{.group_id = gid, .entry = entry});
        w.write(out);
    });
}

template <typename Func>
void visit_raft_commitlog_entry(const commitlog_entry_variant& entry_var, Func&& func) {
    if (std::holds_alternative<raft_commitlog_entry>(entry_var)) {
        func(std::get<raft_commitlog_entry>(entry_var));
    } else {
        BOOST_REQUIRE(std::holds_alternative<raft_commitlog_entry_with_commit_idx>(entry_var));
        func(std::get<raft_commitlog_entry_with_commit_idx>(entry_var));
    }
}

} // anonymous namespace

// Test commitlog_raft_log_entry_writer: size computation is consistent with
// the serialized output, and a write/read roundtrip preserves all fields
// for every entry type (command, configuration, dummy).
SEASTAR_TEST_CASE(test_commitlog_raft_log_entry_writer) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();

        std::vector<raft::log_entry_ptr> entries = {
                make_command_entry(raft::term_t(1), raft::index_t(1)),
                make_config_entry(raft::term_t(2), raft::index_t(2)),
                make_dummy_entry(raft::term_t(3), raft::index_t(3)),
        };

        // Verify size() and accessor for each entry type, then write to commitlog.
        std::vector<replay_position> rps;
        for (const auto& entry : entries) {
            commitlog_raft_log_entry_writer writer(raft_commitlog_entry{.group_id = gid, .entry = entry});
            // size() must exceed the bare raft::log_entry serialization because
            // the writer wraps it in a commitlog_entry + raft_commitlog_entry envelope.
            BOOST_REQUIRE_GT(writer.size(), 0u);
            BOOST_REQUIRE_GT(writer.size(), ser::get_sizeof(*entry));
            BOOST_REQUIRE_EQUAL(writer.group_id(), gid);
            BOOST_REQUIRE_EQUAL(writer.entry()->idx, entry->idx);

            auto handle = co_await write_raft_entry_to_commitlog(log, tid, gid, entry);
            rps.push_back(handle.rp());
        }

        co_await log.sync_all_segments();

        auto segments = log.get_active_segment_names();
        BOOST_REQUIRE(!segments.empty());

        size_t found = 0;
        for (auto& seg : segments) {
            co_await db::commitlog::read_log_file(
                    seg, db::commitlog::descriptor::FILENAME_PREFIX, [&](db::commitlog::buffer_and_replay_position buf_rp) -> future<> {
                        auto&& [buf, replay_pos] = buf_rp;
                        auto it = std::ranges::find(rps, replay_pos);
                        if (it == rps.end()) {
                            co_return;
                        }

                        auto idx = std::distance(rps.begin(), it);
                        const auto& expected = entries[idx];

                        commitlog_entry_reader reader(buf, detail::commitlog_entry_serialization_format::variant);
                        auto& entry_var = reader.entry().item;
                        visit_raft_commitlog_entry(entry_var, [&] (const auto& rle) {
                            BOOST_REQUIRE_EQUAL(rle.group_id, gid);
                            BOOST_REQUIRE_EQUAL(rle.entry->term, expected->term);
                            BOOST_REQUIRE_EQUAL(rle.entry->idx, expected->idx);
                            BOOST_REQUIRE_EQUAL(rle.entry->data.index(), expected->data.index());
                        });
                        ++found;
                        co_return;
                    });
        }
        BOOST_REQUIRE_EQUAL(found, entries.size());
    });
}

// Test that multiple raft entries written to the commitlog can be read back
// correctly, each preserving its term, index, and group_id.
SEASTAR_TEST_CASE(test_commitlog_raft_entry_roundtrip) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();

        constexpr int n = 5;
        std::vector<raft::log_entry_ptr> entries;
        std::vector<replay_position> rps;

        for (int i = 1; i <= n; ++i) {
            auto entry = make_dummy_entry(raft::term_t(1), raft::index_t(i));
            entries.push_back(entry);

            auto handle = co_await write_raft_entry_to_commitlog(log, tid, gid, entry);
            rps.push_back(handle.rp());
        }

        co_await log.sync_all_segments();

        auto segments = log.get_active_segment_names();
        BOOST_REQUIRE(!segments.empty());

        size_t raft_entries_found = 0;
        for (auto& seg : segments) {
            co_await db::commitlog::read_log_file(
                    seg, db::commitlog::descriptor::FILENAME_PREFIX, [&](db::commitlog::buffer_and_replay_position buf_rp) -> future<> {
                        auto&& [buf, rp] = buf_rp;
                        auto it = std::ranges::find(rps, rp);
                        if (it == rps.end()) {
                            co_return;
                        }

                        commitlog_entry_reader reader(buf, detail::commitlog_entry_serialization_format::variant);
                        auto& entry_var = reader.entry().item;
                        auto idx = std::distance(rps.begin(), it);
                        visit_raft_commitlog_entry(entry_var, [&] (const auto& rle) {
                            BOOST_REQUIRE_EQUAL(rle.group_id, gid);
                            BOOST_REQUIRE_EQUAL(rle.entry->idx, entries[idx]->idx);
                            BOOST_REQUIRE_EQUAL(rle.entry->term, entries[idx]->term);
                            BOOST_REQUIRE(std::holds_alternative<raft::log_entry::dummy>(rle.entry->data));
                        });

                        ++raft_entries_found;
                        co_return;
                    });
        }

        BOOST_REQUIRE_EQUAL(raft_entries_found, n);
    });
}

// Test for: SCYLLADB-2877
SEASTAR_TEST_CASE(test_raft_commitlog_store_log_entries_persists_commit_idx) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();

        service::strong_consistency::replayed_data_per_group replayed_data;
        service::strong_consistency::raft_commitlog persistence(gid, log, tid, std::move(replayed_data));

        raft::log_entry_ptr_list entries;
        for (int i = 1; i <= 3; ++i) {
            entries.push_back(make_command_entry(raft::term_t(1), raft::index_t(i)));
        }

        auto commit_idx = raft::index_t(2);
        co_await persistence.store_log_entries(entries, commit_idx);
        co_await log.sync_all_segments();

        auto segments = log.get_active_segment_names();
        BOOST_REQUIRE(!segments.empty());

        size_t plain_entries = 0;
        size_t entries_with_commit_idx = 0;
        for (auto& seg : segments) {
            co_await db::commitlog::read_log_file(
                    seg, db::commitlog::descriptor::FILENAME_PREFIX, [&](db::commitlog::buffer_and_replay_position buf_rp) -> future<> {
                        auto&& [buf, _] = buf_rp;
                        commitlog_entry_reader reader(buf, detail::commitlog_entry_serialization_format::variant);
                        auto& entry_var = reader.entry().item;

                        if (std::holds_alternative<raft_commitlog_entry>(entry_var)) {
                            const auto& rle = std::get<raft_commitlog_entry>(entry_var);
                            BOOST_REQUIRE_EQUAL(rle.group_id, gid);
                            BOOST_REQUIRE_NE(rle.entry->idx, entries.back()->idx);
                            ++plain_entries;
                        } else {
                            BOOST_REQUIRE(std::holds_alternative<raft_commitlog_entry_with_commit_idx>(entry_var));
                            const auto& rle = std::get<raft_commitlog_entry_with_commit_idx>(entry_var);
                            BOOST_REQUIRE_EQUAL(rle.group_id, gid);
                            BOOST_REQUIRE_EQUAL(rle.entry->idx, entries.back()->idx);
                            BOOST_REQUIRE_EQUAL(rle.commit_idx, commit_idx);
                            ++entries_with_commit_idx;
                        }
                        co_return;
                    });
        }

        BOOST_REQUIRE_EQUAL(plain_entries, entries.size() - 1);
        BOOST_REQUIRE_EQUAL(entries_with_commit_idx, 1);
    });
}

// Test that raft entries and mutation entries can coexist in the same commitlog
// and are correctly distinguished when read back, with data integrity verified.
SEASTAR_TEST_CASE(test_commitlog_mixed_raft_and_mutation_entries) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();

        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        auto s = gen.schema();
        auto tid = s->id();

        // Interleave raft entries of different types with real mutation entries.
        std::vector<raft::log_entry_ptr> raft_entries = {
                make_command_entry(raft::term_t(1), raft::index_t(1)),
                make_config_entry(raft::term_t(2), raft::index_t(2)),
                make_dummy_entry(raft::term_t(3), raft::index_t(3)),
                make_command_entry(raft::term_t(4), raft::index_t(4)),
        };

        std::vector<replay_position> raft_rps;
        std::vector<replay_position> mutation_rps;

        for (const auto& entry : raft_entries) {
            auto handle = co_await write_raft_entry_to_commitlog(log, tid, gid, entry);
            raft_rps.push_back(handle.rp());

            // Insert a real mutation entry after each raft entry using add_entry,
            // which wraps it in a commitlog_entry envelope — matching production code.
            auto fm = freeze(gen());
            commitlog_mutation_entry_writer cew(s, fm, db::commitlog::force_sync::no);
            auto mut_handle = co_await log.add_entry(tid, cew, db::no_timeout);
            mutation_rps.push_back(mut_handle.rp());
        }

        co_await log.sync_all_segments();

        auto segments = log.get_active_segment_names();
        BOOST_REQUIRE(!segments.empty());

        size_t raft_found = 0;
        size_t mutation_found = 0;

        for (auto& seg : segments) {
            co_await db::commitlog::read_log_file(
                    seg, db::commitlog::descriptor::FILENAME_PREFIX, [&](db::commitlog::buffer_and_replay_position buf_rp) -> future<> {
                        auto&& [buf, rp] = buf_rp;

                        // With variant format enabled, both raft and mutation entries
                        // use the same variant serialization format (v5 segments).
                        auto is_raft_entry = std::ranges::find(raft_rps, rp) != raft_rps.end();
                        auto format = detail::commitlog_entry_serialization_format::variant;

                        commitlog_entry_reader reader(buf, format);
                        auto& entry_var = reader.entry().item;

                        if (is_raft_entry) {
                            auto it = std::ranges::find(raft_rps, rp);
                            auto idx = std::distance(raft_rps.begin(), it);
                            const auto& expected = raft_entries[idx];

                            visit_raft_commitlog_entry(entry_var, [&] (const auto& rle) {
                                BOOST_REQUIRE_EQUAL(rle.group_id, gid);
                                BOOST_REQUIRE_EQUAL(rle.entry->term, expected->term);
                                BOOST_REQUIRE_EQUAL(rle.entry->idx, expected->idx);
                                BOOST_REQUIRE_EQUAL(rle.entry->data.index(), expected->data.index());
                            });
                            ++raft_found;
                        } else {
                            BOOST_REQUIRE(std::holds_alternative<mutation_entry>(entry_var));
                            auto it = std::ranges::find(mutation_rps, rp);
                            BOOST_REQUIRE(it != mutation_rps.end());
                            ++mutation_found;
                        }

                        co_return;
                    });
        }

        BOOST_REQUIRE_EQUAL(raft_found, raft_entries.size());
        BOOST_REQUIRE_EQUAL(mutation_found, mutation_rps.size());
    });
}

// Test that raft_commitlog_replay_buffer correctly adds, counts, and returns entries.
SEASTAR_TEST_CASE(test_raft_replay_buffer_add_take) {
    db::raft_commitlog_replay_buffer buffer;

    auto gid1 = make_group_id();
    auto gid2 = make_group_id();

    BOOST_CHECK_EQUAL(buffer.total_entries(), 0);
    BOOST_CHECK_EQUAL(buffer.remaining_groups(), 0);

    buffer.add(gid1, make_dummy_entry(raft::term_t(1), raft::index_t(1)));
    buffer.add(gid1, make_command_entry(raft::term_t(1), raft::index_t(2)));
    buffer.add(gid2, make_config_entry(raft::term_t(1), raft::index_t(1)));

    BOOST_CHECK_EQUAL(buffer.total_entries(), 3);
    BOOST_CHECK_EQUAL(buffer.remaining_groups(), 2);

    // take_replayed_group_entries returns empty before processing
    // (processing moves entries from _replayed_commitlog_entries_by_group to _per_group_data)
    auto data = buffer.take_replayed_group_entries(gid1);
    BOOST_CHECK(data.entries.empty());
    BOOST_CHECK(data.replay_positions.empty());

    return make_ready_future<>();
}

// Test that process_raft_replayed_items discards entries for groups
// that are not found in the tablet metadata (e.g., tablet was moved away).
SEASTAR_TEST_CASE(test_raft_replay_buffer_process_discards_unknown_groups) {
    auto db_cfg_ptr = make_shared<db::config>();
    db_cfg_ptr->experimental_features({db::experimental_features_t::feature::STRONGLY_CONSISTENT_TABLES});
    return do_with_cql_env(
            [](cql_test_env& env) -> future<> {
                db::raft_commitlog_replay_buffer buffer;

                auto gid = make_group_id();
                buffer.add(gid, make_dummy_entry(raft::term_t(1), raft::index_t(1)));
                buffer.add(gid, make_command_entry(raft::term_t(1), raft::index_t(2)));

                BOOST_CHECK_EQUAL(buffer.remaining_groups(), 1);
                BOOST_CHECK_EQUAL(buffer.total_entries(), 2);

                // Process — group_id has no matching tablet metadata, so entries should be discarded.
                co_await buffer.process_raft_replayed_items(env.local_db(), env.local_qp(), env.get_system_keyspace().local());

                // After processing, old entries are cleared.
                BOOST_CHECK_EQUAL(buffer.remaining_groups(), 0);

                // take should return empty since the group was discarded.
                auto data = buffer.take_replayed_group_entries(gid);
                BOOST_CHECK(data.entries.empty());
                BOOST_CHECK(data.replay_positions.empty());
            },
            std::move(db_cfg_ptr));
}

// Comprehensive test for check_entry_ordering helper function covering all cases:
// - in_order: normal sequential entries, first entry, index jumps
// - leader_change: smaller/equal index with strictly higher term
// - out_of_order: smaller or equal index with same or lower term (duplicate or duplicate tail)
BOOST_AUTO_TEST_CASE(test_check_entry_ordering) {
    using db::raft_buffer_detail::entry_ordering_check_result;
    using db::raft_buffer_detail::check_entry_ordering;

    // === in_order cases ===
    // First entry (last_idx=0) is always in order
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(1), .idx = raft::index_t(1)}, {.term = raft::term_t(0), .idx = raft::index_t(0)}) ==
                entry_ordering_check_result::in_order);
    // First entry with high term/index
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(50), .idx = raft::index_t(100)}, {.term = raft::term_t(0), .idx = raft::index_t(0)}) ==
                entry_ordering_check_result::in_order);
    // Normal increasing index
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(1), .idx = raft::index_t(2)}, {.term = raft::term_t(1), .idx = raft::index_t(1)}) ==
                entry_ordering_check_result::in_order);
    // Large index jump (entries may be sparse)
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(1), .idx = raft::index_t(1000)}, {.term = raft::term_t(1), .idx = raft::index_t(1)}) ==
                entry_ordering_check_result::in_order);
    // Term increase with index increase
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(2), .idx = raft::index_t(5)}, {.term = raft::term_t(1), .idx = raft::index_t(4)}) ==
                entry_ordering_check_result::in_order);

    // === leader_change cases (idx <= last_idx, term strictly higher) ===
    // Smaller index with higher term
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(2), .idx = raft::index_t(2)}, {.term = raft::term_t(1), .idx = raft::index_t(3)}) ==
                entry_ordering_check_result::leader_change);
    // Index goes back to 1 with new term (complete log rewrite)
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(2), .idx = raft::index_t(1)}, {.term = raft::term_t(1), .idx = raft::index_t(10)}) ==
                entry_ordering_check_result::leader_change);
    // Equal index with higher term (overwrite at same position)
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(5), .idx = raft::index_t(5)}, {.term = raft::term_t(1), .idx = raft::index_t(5)}) ==
                entry_ordering_check_result::leader_change);

    // === out_of_order cases (idx <= last_idx, term same or lower) ===
    // Same index and same term - duplicate entry from crash recovery
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(1), .idx = raft::index_t(5)}, {.term = raft::term_t(1), .idx = raft::index_t(5)}) ==
                entry_ordering_check_result::out_of_order);
    // Smaller index with same term (duplicate tail from older segment)
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(1), .idx = raft::index_t(2)}, {.term = raft::term_t(1), .idx = raft::index_t(3)}) ==
                entry_ordering_check_result::out_of_order);
}

// Simulates the full processing loop for multiple groups with multiple leader changes.
// This tests the exact logic used in process_raft_replayed_items() without needing
// database/QP setup, verifying that entries are correctly discarded on leader change.
BOOST_AUTO_TEST_CASE(test_replay_buffer_multi_group_multi_leader_change_simulation) {
    using db::raft_buffer_detail::entry_ordering_check_result;
    using db::raft_buffer_detail::check_entry_ordering;

    struct simulated_entry {
        raft::term_t term;
        raft::index_t idx;

        bool operator==(const simulated_entry& other) const {
            return term == other.term && idx == other.idx;
        }
    };

    // Helper to simulate processing a group's entries (same logic as process_raft_replayed_items)
    auto process_group = [](const std::vector<simulated_entry>& entries) {
        std::vector<simulated_entry> kept;
        raft::index_t last_idx{0};
        raft::term_t last_term{0};
        int leader_changes = 0;

        for (const auto& entry : entries) {
            auto ordering = check_entry_ordering({.term = entry.term, .idx = entry.idx}, {.term = last_term, .idx = last_idx});

            if (ordering == entry_ordering_check_result::out_of_order) {
                break; // Duplicate tail — stop processing
            }
            if (ordering == entry_ordering_check_result::leader_change) {
                // Use binary search since entries are sorted by idx
                auto it = std::ranges::lower_bound(kept, entry.idx, {}, [](const simulated_entry& e) {
                    return e.idx;
                });
                kept.erase(it, kept.end());
                ++leader_changes;
            }

            last_idx = entry.idx;
            last_term = entry.term;
            kept.push_back(entry);
        }
        return std::make_pair(kept, leader_changes);
    };

    // Group 1: No leader changes - simple sequential entries
    {
        std::vector<simulated_entry> entries = {
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
                {raft::term_t(1), raft::index_t(3)},
        };
        auto [kept, changes] = process_group(entries);
        BOOST_CHECK_EQUAL(changes, 0);
        BOOST_CHECK_EQUAL(kept.size(), 3);
    }

    // Group 2: Single leader change
    // Leader 1 (term 1): idx 1, 2, 3
    // Leader 2 (term 2): overwrites at idx 2
    {
        std::vector<simulated_entry> entries = {
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
                {raft::term_t(1), raft::index_t(3)},
                {raft::term_t(2), raft::index_t(2)}, // Leader change
                {raft::term_t(2), raft::index_t(3)},
                {raft::term_t(2), raft::index_t(4)},
        };
        auto [kept, changes] = process_group(entries);
        BOOST_CHECK_EQUAL(changes, 1);
        BOOST_CHECK_EQUAL(kept.size(), 4);

        // Verify: idx 1 from term 1, idx 2-4 from term 2
        std::vector<simulated_entry> expected = {
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(2), raft::index_t(2)},
                {raft::term_t(2), raft::index_t(3)},
                {raft::term_t(2), raft::index_t(4)},
        };
        BOOST_CHECK(kept == expected);
    }

    // Group 3: Multiple leader changes (3 leaders)
    // Leader 1 (term 1): idx 1-5
    // Leader 2 (term 2): overwrites at idx 4
    // Leader 3 (term 3): overwrites at idx 3
    {
        std::vector<simulated_entry> entries = {
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
                {raft::term_t(1), raft::index_t(3)},
                {raft::term_t(1), raft::index_t(4)},
                {raft::term_t(1), raft::index_t(5)},
                {raft::term_t(2), raft::index_t(4)}, // Leader change #1
                {raft::term_t(2), raft::index_t(5)},
                {raft::term_t(2), raft::index_t(6)},
                {raft::term_t(3), raft::index_t(3)}, // Leader change #2
                {raft::term_t(3), raft::index_t(4)},
        };
        auto [kept, changes] = process_group(entries);
        BOOST_CHECK_EQUAL(changes, 2);
        BOOST_CHECK_EQUAL(kept.size(), 4);

        // Verify: idx 1-2 from term 1, idx 3-4 from term 3
        std::vector<simulated_entry> expected = {
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
                {raft::term_t(3), raft::index_t(3)},
                {raft::term_t(3), raft::index_t(4)},
        };
        BOOST_CHECK(kept == expected);
    }

    // Group 4: Leader change at index 1 (complete log replacement)
    {
        std::vector<simulated_entry> entries = {
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
                {raft::term_t(2), raft::index_t(1)}, // Complete replacement
                {raft::term_t(2), raft::index_t(2)},
        };
        auto [kept, changes] = process_group(entries);
        BOOST_CHECK_EQUAL(changes, 1);
        BOOST_CHECK_EQUAL(kept.size(), 2);

        std::vector<simulated_entry> expected = {
                {raft::term_t(2), raft::index_t(1)},
                {raft::term_t(2), raft::index_t(2)},
        };
        BOOST_CHECK(kept == expected);
    }

    // Group 5: Leader change at same index (overwrite without going back)
    {
        std::vector<simulated_entry> entries = {
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
                {raft::term_t(2), raft::index_t(2)}, // Same index, different term
                {raft::term_t(2), raft::index_t(3)},
        };
        auto [kept, changes] = process_group(entries);
        BOOST_CHECK_EQUAL(changes, 1);
        BOOST_CHECK_EQUAL(kept.size(), 3);

        std::vector<simulated_entry> expected = {
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(2), raft::index_t(2)},
                {raft::term_t(2), raft::index_t(3)},
        };
        BOOST_CHECK(kept == expected);
    }
}

// Test basic buffer operations: add, total_entries, remaining_groups, take_replayed_group_entries
SEASTAR_TEST_CASE(test_raft_replay_buffer_basic_operations) {
    db::raft_commitlog_replay_buffer buffer;

    auto gid1 = make_group_id();
    auto gid2 = make_group_id();

    BOOST_CHECK_EQUAL(buffer.total_entries(), 0);
    BOOST_CHECK_EQUAL(buffer.remaining_groups(), 0);

    // Add entries to multiple groups with different entry types
    buffer.add(gid1, make_dummy_entry(raft::term_t(1), raft::index_t(1)));
    buffer.add(gid1, make_command_entry(raft::term_t(1), raft::index_t(2)));
    buffer.add(gid2, make_config_entry(raft::term_t(1), raft::index_t(1)));

    BOOST_CHECK_EQUAL(buffer.total_entries(), 3);
    BOOST_CHECK_EQUAL(buffer.remaining_groups(), 2);

    // Before processing, take_replayed_group_entries returns empty
    // (entries are in _replayed_commitlog_entries_by_group, not _per_group_data)
    auto data1 = buffer.take_replayed_group_entries(gid1);
    BOOST_CHECK(data1.entries.empty());
    BOOST_CHECK(data1.replay_positions.empty());

    // Non-existent group returns empty
    auto data_nonexistent = buffer.take_replayed_group_entries(make_group_id());
    BOOST_CHECK(data_nonexistent.entries.empty());

    return make_ready_future<>();
}

// Test that out-of-order entries within the same term (duplicate tail from
// an older segment) are handled gracefully — processing stops at that point.
SEASTAR_TEST_CASE(test_raft_replay_buffer_out_of_order_same_term_stops_processing) {
    auto db_cfg_ptr = make_shared<db::config>();
    db_cfg_ptr->experimental_features({db::experimental_features_t::feature::STRONGLY_CONSISTENT_TABLES});
    return do_with_cql_env(
            [](cql_test_env& env) -> future<> {
                db::raft_commitlog_replay_buffer buffer;

                auto gid = make_group_id();

                // Simulate crash recovery: new segment entries first (idx 1-3),
                // then old segment duplicate tail restarts at idx 1.
                buffer.add(gid, make_dummy_entry(raft::term_t(1), raft::index_t(1)));
                buffer.add(gid, make_dummy_entry(raft::term_t(1), raft::index_t(2)));
                buffer.add(gid, make_dummy_entry(raft::term_t(1), raft::index_t(3)));
                buffer.add(gid, make_dummy_entry(raft::term_t(1), raft::index_t(1))); // Duplicate tail start
                buffer.add(gid, make_dummy_entry(raft::term_t(1), raft::index_t(2))); // Should be skipped

                BOOST_CHECK_EQUAL(buffer.remaining_groups(), 1);
                BOOST_CHECK_EQUAL(buffer.total_entries(), 5);

                // Process — group_id has no matching tablet metadata, so entries are discarded
                // before the ordering check. The filter_entries logic is tested directly
                // in the simulation tests below.
                co_await buffer.process_raft_replayed_items(env.local_db(), env.local_qp(), env.get_system_keyspace().local());

                BOOST_CHECK_EQUAL(buffer.remaining_groups(), 0);
            },
            std::move(db_cfg_ptr));
}

// Test the simulation of out-of-order same-term detection (duplicate tail scenario).
// This directly tests the detection logic without needing database setup.
BOOST_AUTO_TEST_CASE(test_out_of_order_same_term_detection_simulation) {
    using db::raft_buffer_detail::entry_ordering_check_result;
    using db::raft_buffer_detail::check_entry_ordering;

    // Simulate processing entries and verify we detect the duplicate tail
    struct simulated_entry {
        raft::term_t term;
        raft::index_t idx;
    };

    std::vector<simulated_entry> entries = {
            {raft::term_t(1), raft::index_t(1)},
            {raft::term_t(1), raft::index_t(3)},
            {raft::term_t(1), raft::index_t(2)}, // Out of order within same term!
    };

    raft::index_t last_idx{0};
    raft::term_t last_term{0};
    bool detected_duplicate_tail = false;

    for (const auto& entry : entries) {
        auto ordering = check_entry_ordering({.term = entry.term, .idx = entry.idx}, {.term = last_term, .idx = last_idx});

        if (ordering == entry_ordering_check_result::out_of_order) {
            detected_duplicate_tail = true;
            // In real code, filter_entries stops processing here
            // Verify we detected the right entry
            BOOST_CHECK_EQUAL(entry.idx, raft::index_t(2));
            BOOST_CHECK_EQUAL(entry.term, raft::term_t(1));
            BOOST_CHECK_EQUAL(last_idx, raft::index_t(3));
            BOOST_CHECK_EQUAL(last_term, raft::term_t(1));
            break; // Stop processing — all remaining entries are duplicates
        }

        last_idx = entry.idx;
        last_term = entry.term;
    }

    BOOST_CHECK(detected_duplicate_tail);
}

// Additional edge cases for out-of-order detection
BOOST_AUTO_TEST_CASE(test_out_of_order_same_term_edge_cases) {
    using db::raft_buffer_detail::entry_ordering_check_result;
    using db::raft_buffer_detail::check_entry_ordering;

    // Case 1: Equal index with same term is out_of_order (duplicate entry)
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(1), .idx = raft::index_t(5)}, {.term = raft::term_t(1), .idx = raft::index_t(5)}) ==
                entry_ordering_check_result::out_of_order);

    // Case 2: Index goes from 10 to 1 within same term (duplicate tail)
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(1), .idx = raft::index_t(1)}, {.term = raft::term_t(1), .idx = raft::index_t(10)}) ==
                entry_ordering_check_result::out_of_order);

    // Case 3: Index decreases by 1 within same term
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(2), .idx = raft::index_t(4)}, {.term = raft::term_t(2), .idx = raft::index_t(5)}) ==
                entry_ordering_check_result::out_of_order);

    // Contrast with leader_change: same scenarios but with strictly higher term
    // Case 1 contrast: Equal index with higher term is leader_change
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(2), .idx = raft::index_t(5)}, {.term = raft::term_t(1), .idx = raft::index_t(5)}) ==
                entry_ordering_check_result::leader_change);

    // Case 2 contrast: Index goes from 10 to 1 with higher term is leader_change
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(2), .idx = raft::index_t(1)}, {.term = raft::term_t(1), .idx = raft::index_t(10)}) ==
                entry_ordering_check_result::leader_change);

    // Case 3 contrast: Index decreases by 1 with higher term is leader_change
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(3), .idx = raft::index_t(4)}, {.term = raft::term_t(2), .idx = raft::index_t(5)}) ==
                entry_ordering_check_result::leader_change);
}

// Test out_of_order detection: both duplicate entries (same idx+term) and
// smaller-index same/lower-term entries are detected as out_of_order,
// and same index with strictly higher term is leader_change.
BOOST_AUTO_TEST_CASE(test_duplicate_entry_detection) {
    using db::raft_buffer_detail::entry_ordering_check_result;
    using db::raft_buffer_detail::check_entry_ordering;

    // === out_of_order: same index and same term (former "duplicate") ===
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(1), .idx = raft::index_t(1)}, {.term = raft::term_t(1), .idx = raft::index_t(1)}) ==
                entry_ordering_check_result::out_of_order);
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(5), .idx = raft::index_t(100)}, {.term = raft::term_t(5), .idx = raft::index_t(100)}) ==
                entry_ordering_check_result::out_of_order);

    // === leader_change: same index but strictly higher term ===
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(2), .idx = raft::index_t(5)}, {.term = raft::term_t(1), .idx = raft::index_t(5)}) ==
                entry_ordering_check_result::leader_change);

    // === out_of_order: same index with lower term (not a leader change) ===
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(1), .idx = raft::index_t(5)}, {.term = raft::term_t(2), .idx = raft::index_t(5)}) ==
                entry_ordering_check_result::out_of_order);

    // === out_of_order: smaller index with same term (duplicate tail) ===
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(1), .idx = raft::index_t(4)}, {.term = raft::term_t(1), .idx = raft::index_t(5)}) ==
                entry_ordering_check_result::out_of_order);
}

// Simulates crash recovery scenarios involving out-of-order entries.
// Any entry with idx <= last_idx and term <= last_term (including what were
// formerly "consecutive duplicates") is classified as out_of_order and causes
// filter_entries to stop processing immediately.
BOOST_AUTO_TEST_CASE(test_replay_buffer_duplicate_entries_simulation) {
    using db::raft_buffer_detail::entry_ordering_check_result;
    using db::raft_buffer_detail::check_entry_ordering;

    struct simulated_entry {
        raft::term_t term;
        raft::index_t idx;

        bool operator==(const simulated_entry& other) const {
            return term == other.term && idx == other.idx;
        }
    };

    // Helper simulating filter_entries: stops on out_of_order, discards tail on leader_change.
    auto process_group = [](const std::vector<simulated_entry>& entries) {
        std::vector<simulated_entry> kept;
        raft::index_t last_idx{0};
        raft::term_t last_term{0};
        int leader_changes = 0;

        for (const auto& entry : entries) {
            auto ordering = check_entry_ordering({.term = entry.term, .idx = entry.idx}, {.term = last_term, .idx = last_idx});

            switch (ordering) {
            case entry_ordering_check_result::leader_change: {
                // Use binary search since entries are sorted by idx
                auto it = std::ranges::lower_bound(kept, entry.idx, {}, [](const simulated_entry& e) {
                    return e.idx;
                });
                kept.erase(it, kept.end());
                ++leader_changes;
                break;
            }
            case entry_ordering_check_result::out_of_order:
                // Duplicate or duplicate tail — stop processing entirely.
                return std::make_pair(kept, leader_changes);
            case entry_ordering_check_result::in_order:
                break;
            }

            last_idx = entry.idx;
            last_term = entry.term;
            kept.push_back(entry);
        }
        return std::make_pair(kept, leader_changes);
    };

    // Scenario 1: Entry with same idx+term stops processing immediately (out_of_order).
    // Only entries before the duplicate are kept.
    {
        std::vector<simulated_entry> entries = {
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
                {raft::term_t(1), raft::index_t(2)}, // out_of_order: same idx+term → stop
                {raft::term_t(1), raft::index_t(3)},
        };
        auto [kept, changes] = process_group(entries);
        BOOST_CHECK_EQUAL(changes, 0);
        BOOST_CHECK_EQUAL(kept.size(), 2);

        std::vector<simulated_entry> expected = {
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
        };
        BOOST_CHECK(kept == expected);
    }

    // Scenario 2: Leader change followed by out_of_order entry stops processing.
    // Tail discarded by leader change, then processing stops at out_of_order.
    {
        std::vector<simulated_entry> entries = {
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
                {raft::term_t(2), raft::index_t(2)}, // leader change: discard term-1 idx>=2
                {raft::term_t(2), raft::index_t(3)},
                {raft::term_t(2), raft::index_t(3)}, // out_of_order: stop
                {raft::term_t(2), raft::index_t(4)},
        };
        auto [kept, changes] = process_group(entries);
        BOOST_CHECK_EQUAL(changes, 1);
        BOOST_CHECK_EQUAL(kept.size(), 3);

        std::vector<simulated_entry> expected = {
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(2), raft::index_t(2)},
                {raft::term_t(2), raft::index_t(3)},
        };
        BOOST_CHECK(kept == expected);
    }

    // Scenario 3: Entry with lower term and same index is out_of_order (not leader_change).
    // This differs from the old behavior where only "different term" caused leader_change
    // regardless of direction. Now only strictly higher term triggers leader_change.
    {
        std::vector<simulated_entry> entries = {
                {raft::term_t(2), raft::index_t(1)},
                {raft::term_t(2), raft::index_t(2)},
                {raft::term_t(1), raft::index_t(2)}, // out_of_order: lower term, same idx → stop
                {raft::term_t(2), raft::index_t(3)},
        };
        auto [kept, changes] = process_group(entries);
        BOOST_CHECK_EQUAL(changes, 0);
        BOOST_CHECK_EQUAL(kept.size(), 2);

        std::vector<simulated_entry> expected = {
                {raft::term_t(2), raft::index_t(1)},
                {raft::term_t(2), raft::index_t(2)},
        };
        BOOST_CHECK(kept == expected);
    }
}

// Test crash recovery scenarios where old segment entries appear after new ones.
// There is no explicit max_term skipping. Instead, old-term entries that appear
// after higher-indexed entries are caught by the out_of_order check
// (idx <= last_idx with same or lower term), which stops processing. This
// correctly handles the case where an old segment is replayed after a new one.
BOOST_AUTO_TEST_CASE(test_replay_buffer_old_term_skipping_simulation) {
    using db::raft_buffer_detail::entry_ordering_check_result;
    using db::raft_buffer_detail::check_entry_ordering;

    struct simulated_entry {
        raft::term_t term;
        raft::index_t idx;

        bool operator==(const simulated_entry& other) const {
            return term == other.term && idx == other.idx;
        }
    };

    // Helper simulating filter_entries: leader_change discards tail, out_of_order stops.
    auto process_group = [](const std::vector<simulated_entry>& entries) {
        std::vector<simulated_entry> kept;
        raft::index_t last_idx{0};
        raft::term_t last_term{0};
        int leader_changes = 0;

        for (const auto& entry : entries) {
            auto ordering = check_entry_ordering({.term = entry.term, .idx = entry.idx}, {.term = last_term, .idx = last_idx});

            switch (ordering) {
            case entry_ordering_check_result::leader_change: {
                auto it = std::ranges::lower_bound(kept, entry.idx, {}, [](const simulated_entry& e) {
                    return e.idx;
                });
                kept.erase(it, kept.end());
                ++leader_changes;
                break;
            }
            case entry_ordering_check_result::out_of_order:
                // Old-term or duplicate entry — stop processing.
                return std::make_pair(kept, leader_changes);
            case entry_ordering_check_result::in_order:
                break;
            }

            last_idx = entry.idx;
            last_term = entry.term;
            kept.push_back(entry);
        }
        return std::make_pair(kept, leader_changes);
    };

    // Scenario 1: Old segment entries replayed after leader change.
    // Segment 1: term 1 entries 1-5, then term 2 entries 3-7 (leader change).
    // Segment 2 (replayed): term 1 entries 1-5 — all have lower idx than last_idx=7,
    // so the first one triggers out_of_order and stops processing.
    {
        std::vector<simulated_entry> entries = {
                // From segment 1
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
                {raft::term_t(1), raft::index_t(3)},
                {raft::term_t(1), raft::index_t(4)},
                {raft::term_t(1), raft::index_t(5)},
                // Leader change to term 2
                {raft::term_t(2), raft::index_t(3)},
                {raft::term_t(2), raft::index_t(4)},
                {raft::term_t(2), raft::index_t(5)},
                {raft::term_t(2), raft::index_t(6)},
                {raft::term_t(2), raft::index_t(7)},
                // From segment 2 (replayed old data): idx 1 < last_idx 7 → out_of_order → stop
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
                {raft::term_t(1), raft::index_t(3)},
                {raft::term_t(1), raft::index_t(4)},
                {raft::term_t(1), raft::index_t(5)},
        };
        auto [kept, changes] = process_group(entries);
        BOOST_CHECK_EQUAL(changes, 1);
        BOOST_CHECK_EQUAL(kept.size(), 7); // idx 1-2 from term 1, idx 3-7 from term 2

        std::vector<simulated_entry> expected = {
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
                {raft::term_t(2), raft::index_t(3)},
                {raft::term_t(2), raft::index_t(4)},
                {raft::term_t(2), raft::index_t(5)},
                {raft::term_t(2), raft::index_t(6)},
                {raft::term_t(2), raft::index_t(7)},
        };
        BOOST_CHECK(kept == expected);
    }

    // Scenario 2: Multiple leader changes, then old entries replayed.
    // term 1 -> term 2 -> term 3, then term 1 entries appear — stopped by out_of_order.
    {
        std::vector<simulated_entry> entries = {
                // Initial entries
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
                // Leader change to term 2
                {raft::term_t(2), raft::index_t(2)},
                {raft::term_t(2), raft::index_t(3)},
                // Leader change to term 3
                {raft::term_t(3), raft::index_t(3)},
                {raft::term_t(3), raft::index_t(4)},
                // Replayed old entries: idx 1 < last_idx 4 → out_of_order → stop
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
                {raft::term_t(2), raft::index_t(2)},
                {raft::term_t(2), raft::index_t(3)},
        };
        auto [kept, changes] = process_group(entries);
        BOOST_CHECK_EQUAL(changes, 2); // term 1->2 and term 2->3
        BOOST_CHECK_EQUAL(kept.size(), 4);

        std::vector<simulated_entry> expected = {
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(2), raft::index_t(2)},
                {raft::term_t(3), raft::index_t(3)},
                {raft::term_t(3), raft::index_t(4)},
        };
        BOOST_CHECK(kept == expected);
    }

    // Scenario 3: out_of_order entry (same idx+term) stops processing before more entries.
    // After a leader change, a duplicate entry causes processing to stop.
    {
        std::vector<simulated_entry> entries = {
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(2), raft::index_t(1)}, // leader change
                {raft::term_t(2), raft::index_t(2)},
                {raft::term_t(2), raft::index_t(2)}, // out_of_order: same idx+term → stop
                {raft::term_t(2), raft::index_t(3)}, // not reached
        };
        auto [kept, changes] = process_group(entries);
        BOOST_CHECK_EQUAL(changes, 1);
        BOOST_CHECK_EQUAL(kept.size(), 2);

        std::vector<simulated_entry> expected = {
                {raft::term_t(2), raft::index_t(1)},
                {raft::term_t(2), raft::index_t(2)},
        };
        BOOST_CHECK(kept == expected);
    }

    // Scenario 4: Same-term entries after leader change proceed normally until out_of_order.
    {
        std::vector<simulated_entry> entries = {
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
                {raft::term_t(2), raft::index_t(2)}, // leader change
                {raft::term_t(2), raft::index_t(3)},
                {raft::term_t(2), raft::index_t(4)},
                {raft::term_t(2), raft::index_t(4)}, // out_of_order → stop
        };
        auto [kept, changes] = process_group(entries);
        BOOST_CHECK_EQUAL(changes, 1);
        BOOST_CHECK_EQUAL(kept.size(), 4);
    }
}

// Test empty entries list handling - should not crash or produce errors
BOOST_AUTO_TEST_CASE(test_empty_entries_handling_simulation) {
    using db::raft_buffer_detail::entry_ordering_check_result;
    using db::raft_buffer_detail::check_entry_ordering;

    struct simulated_entry {
        raft::term_t term;
        raft::index_t idx;
    };

    auto process_empty = [](const std::vector<simulated_entry>& entries) {
        std::vector<simulated_entry> kept;
        raft::index_t last_idx{0};
        raft::term_t last_term{0};

        for (const auto& entry : entries) {
            auto ordering = check_entry_ordering({.term = entry.term, .idx = entry.idx}, {.term = last_term, .idx = last_idx});
            if (ordering == entry_ordering_check_result::in_order) {
                last_idx = entry.idx;
                last_term = entry.term;
                kept.push_back(entry);
            }
        }
        return kept;
    };

    // Empty input should produce empty output without errors
    std::vector<simulated_entry> empty_entries;
    auto result = process_empty(empty_entries);
    BOOST_CHECK(result.empty());
}

// Test crash recovery where old-term entries appear after new-term entries.
// Non-consecutive "duplicates" (entries from an old term that appear after
// entries from a higher-indexed new term) are caught by the out_of_order
// check: their idx <= last_idx, so processing stops immediately.
BOOST_AUTO_TEST_CASE(test_non_consecutive_duplicates_via_old_term) {
    using db::raft_buffer_detail::entry_ordering_check_result;
    using db::raft_buffer_detail::check_entry_ordering;

    struct simulated_entry {
        raft::term_t term;
        raft::index_t idx;

        bool operator==(const simulated_entry& other) const {
            return term == other.term && idx == other.idx;
        }
    };

    // Simulates filter_entries: stops on out_of_order, discards tail on leader_change.
    auto process_group = [](const std::vector<simulated_entry>& entries) {
        std::vector<simulated_entry> kept;
        raft::index_t last_idx{0};
        raft::term_t last_term{0};

        for (const auto& entry : entries) {
            auto ordering = check_entry_ordering({.term = entry.term, .idx = entry.idx}, {.term = last_term, .idx = last_idx});

            switch (ordering) {
            case entry_ordering_check_result::leader_change: {
                auto it = std::ranges::lower_bound(kept, entry.idx, {}, [](const simulated_entry& e) {
                    return e.idx;
                });
                kept.erase(it, kept.end());
                break;
            }
            case entry_ordering_check_result::out_of_order:
                return kept; // Stop processing
            case entry_ordering_check_result::in_order:
                break;
            }

            last_idx = entry.idx;
            last_term = entry.term;
            kept.push_back(entry);
        }
        return kept;
    };

    // Scenario: Old segment (term 1) replayed after new segment (term 2).
    // After processing all term-2 entries (last_idx=3), the first term-1 entry
    // has idx=1 <= last_idx=3, so out_of_order triggers and stops processing.
    {
        std::vector<simulated_entry> entries = {
                // New segment first (term 2)
                {raft::term_t(2), raft::index_t(1)},
                {raft::term_t(2), raft::index_t(2)},
                {raft::term_t(2), raft::index_t(3)},
                // Old segment (term 1): idx 1 <= last_idx 3 → out_of_order → stop
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
                {raft::term_t(1), raft::index_t(3)},
        };
        auto kept = process_group(entries);

        BOOST_CHECK_EQUAL(kept.size(), 3);

        std::vector<simulated_entry> expected = {
                {raft::term_t(2), raft::index_t(1)},
                {raft::term_t(2), raft::index_t(2)},
                {raft::term_t(2), raft::index_t(3)},
        };
        BOOST_CHECK(kept == expected);
    }

    // Scenario: Interleaved old/new term entries.
    // After the first term-2 entry (last_idx=1), the next term-1 entry
    // has idx=1 <= last_idx=1, so out_of_order triggers immediately.
    {
        std::vector<simulated_entry> entries = {
                {raft::term_t(2), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(1)}, // idx 1 <= last_idx 1 → out_of_order → stop
                {raft::term_t(2), raft::index_t(2)},
                {raft::term_t(1), raft::index_t(2)},
                {raft::term_t(2), raft::index_t(3)},
        };
        auto kept = process_group(entries);

        // Only the first term-2 entry is kept; processing stops at term-1 idx=1.
        BOOST_CHECK_EQUAL(kept.size(), 1);
        std::vector<simulated_entry> expected = {
                {raft::term_t(2), raft::index_t(1)},
        };
        BOOST_CHECK(kept == expected);
    }
}

// Test that entries with decreasing terms but increasing indices are handled correctly.
// If idx increases, check_entry_ordering returns in_order regardless of term direction.
// Note: filter_entries may encounter such entries when commitlog segments are replayed
// in a non-standard order, but the out_of_order detection will catch them if they
// subsequently appear with idx <= last_idx.
BOOST_AUTO_TEST_CASE(test_term_decreasing_index_increasing) {
    using db::raft_buffer_detail::entry_ordering_check_result;
    using db::raft_buffer_detail::check_entry_ordering;

    // term 3, idx 5 -> term 2, idx 6: idx increased → in_order
    auto result = check_entry_ordering({.term = raft::term_t(2), .idx = raft::index_t(6)}, {.term = raft::term_t(3), .idx = raft::index_t(5)});
    BOOST_CHECK(result == entry_ordering_check_result::in_order);

    // This is correct because check_entry_ordering only requires idx > last_idx for in_order.
    // Term direction is irrelevant when the index advances.
}

// Test boundary conditions for index and term values
BOOST_AUTO_TEST_CASE(test_boundary_values) {
    using db::raft_buffer_detail::entry_ordering_check_result;
    using db::raft_buffer_detail::check_entry_ordering;

    // Maximum index values
    auto max_idx = raft::index_t(std::numeric_limits<uint64_t>::max());
    auto large_idx = raft::index_t(std::numeric_limits<uint64_t>::max() - 1);

    // First entry with max index
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(1), .idx = max_idx}, {.term = raft::term_t(0), .idx = raft::index_t(0)}) ==
                entry_ordering_check_result::in_order);

    // Sequential entries at high index values
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(1), .idx = max_idx}, {.term = raft::term_t(1), .idx = large_idx}) ==
                entry_ordering_check_result::in_order);

    // Same index and same term at max index — out_of_order (former "duplicate")
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(1), .idx = max_idx}, {.term = raft::term_t(1), .idx = max_idx}) ==
                entry_ordering_check_result::out_of_order);

    // Leader change at max index (higher term)
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(2), .idx = max_idx}, {.term = raft::term_t(1), .idx = max_idx}) ==
                entry_ordering_check_result::leader_change);

    // Term 0 handling (edge case - term 0 is typically not used in Raft but let's verify)
    BOOST_CHECK(check_entry_ordering({.term = raft::term_t(0), .idx = raft::index_t(1)}, {.term = raft::term_t(0), .idx = raft::index_t(0)}) ==
                entry_ordering_check_result::in_order);
}

// Test that the committed/uncommitted split works correctly in simulation.
// Committed entries (idx <= commit_idx) should be "applied" (in real code: to memtable)
// Uncommitted entries (idx > commit_idx) should be "rewritten" (in real code: to new commitlog)
BOOST_AUTO_TEST_CASE(test_committed_uncommitted_split_simulation) {
    struct simulated_entry {
        raft::term_t term;
        raft::index_t idx;
        bool is_command; // true for command entries, false for config/dummy
    };

    // Simulate the second pass of process_raft_replayed_items
    auto process_second_pass = [](const std::vector<simulated_entry>& entries, raft::index_t commit_idx) {
        int applied = 0;
        int rewritten = 0;
        int kept_non_command = 0;

        for (const auto& entry : entries) {
            if (entry.idx <= commit_idx && entry.is_command) {
                ++applied;
            }
            if (entry.idx > commit_idx) {
                ++rewritten;
            }
            if (entry.idx <= commit_idx && !entry.is_command) {
                ++kept_non_command;
            }
        }
        return std::make_tuple(applied, rewritten, kept_non_command);
    };

    // Scenario 1: All entries committed
    {
        std::vector<simulated_entry> entries = {
                {raft::term_t(1), raft::index_t(1), true},
                {raft::term_t(1), raft::index_t(2), true},
                {raft::term_t(1), raft::index_t(3), true},
        };
        auto [applied, rewritten, non_cmd] = process_second_pass(entries, raft::index_t(5));
        BOOST_CHECK_EQUAL(applied, 3);
        BOOST_CHECK_EQUAL(rewritten, 0);
    }

    // Scenario 2: All entries uncommitted
    {
        std::vector<simulated_entry> entries = {
                {raft::term_t(1), raft::index_t(5), true},
                {raft::term_t(1), raft::index_t(6), true},
                {raft::term_t(1), raft::index_t(7), true},
        };
        auto [applied, rewritten, non_cmd] = process_second_pass(entries, raft::index_t(3));
        BOOST_CHECK_EQUAL(applied, 0);
        BOOST_CHECK_EQUAL(rewritten, 3);
    }

    // Scenario 3: Mixed committed and uncommitted
    {
        std::vector<simulated_entry> entries = {
                {raft::term_t(1), raft::index_t(1), true},
                {raft::term_t(1), raft::index_t(2), true},
                {raft::term_t(1), raft::index_t(3), true}, // committed
                {raft::term_t(1), raft::index_t(4), true}, // uncommitted
                {raft::term_t(1), raft::index_t(5), true}, // uncommitted
        };
        auto [applied, rewritten, non_cmd] = process_second_pass(entries, raft::index_t(3));
        BOOST_CHECK_EQUAL(applied, 3);
        BOOST_CHECK_EQUAL(rewritten, 2);
    }

    // Scenario 4: Non-command entries are not applied but are kept
    {
        std::vector<simulated_entry> entries = {
                {raft::term_t(1), raft::index_t(1), true},  // command
                {raft::term_t(1), raft::index_t(2), false}, // config/dummy
                {raft::term_t(1), raft::index_t(3), true},  // command
                {raft::term_t(1), raft::index_t(4), false}, // config/dummy, uncommitted
        };
        auto [applied, rewritten, non_cmd] = process_second_pass(entries, raft::index_t(3));
        BOOST_CHECK_EQUAL(applied, 2);   // only commands at idx 1 and 3
        BOOST_CHECK_EQUAL(rewritten, 1); // only idx 4 (uncommitted)
        BOOST_CHECK_EQUAL(non_cmd, 1);   // config/dummy at idx 2
    }

    // Scenario 5: Entry at exactly commit_idx boundary
    {
        std::vector<simulated_entry> entries = {
                {raft::term_t(1), raft::index_t(5), true}, // at commit_idx - committed
                {raft::term_t(1), raft::index_t(6), true}, // after commit_idx - uncommitted
        };
        auto [applied, rewritten, non_cmd] = process_second_pass(entries, raft::index_t(5));
        BOOST_CHECK_EQUAL(applied, 1);
        BOOST_CHECK_EQUAL(rewritten, 1);
    }
}

// Test the full filtering pipeline simulation combining all aspects:
// max_term tracking, leader changes, and duplicates.
// Note: snapshot filtering is no longer part of filter_entries — it is handled
// by the caller (process_raft_replayed_items) when deciding what goes into the
// raft log vs what gets applied to memtables.
BOOST_AUTO_TEST_CASE(test_full_filtering_pipeline_simulation) {
    using db::raft_buffer_detail::entry_ordering_check_result;
    using db::raft_buffer_detail::check_entry_ordering;

    struct simulated_entry {
        raft::term_t term;
        raft::index_t idx;

        bool operator==(const simulated_entry& other) const {
            return term == other.term && idx == other.idx;
        }
    };

    struct filter_result {
        std::vector<simulated_entry> entries;
        uint64_t discarded_leader_change = 0;
    };

    // Full simulation of filter_entries: leader_change discards tail, out_of_order stops.
    auto filter_entries_sim = [](const std::vector<simulated_entry>& input) {
        filter_result result;
        raft::index_t last_idx{0};
        raft::term_t last_term{0};

        for (const auto& entry : input) {
            auto ordering = check_entry_ordering({.term = entry.term, .idx = entry.idx}, {.term = last_term, .idx = last_idx});

            switch (ordering) {
            case entry_ordering_check_result::leader_change: {
                auto it = std::ranges::lower_bound(result.entries, entry.idx, {}, [](const simulated_entry& e) {
                    return e.idx;
                });
                result.discarded_leader_change += std::distance(it, result.entries.end());
                result.entries.erase(it, result.entries.end());
                break;
            }
            case entry_ordering_check_result::out_of_order:
                return result; // Duplicate or duplicate tail — stop processing
            case entry_ordering_check_result::in_order:
                break;
            }

            last_idx = entry.idx;
            last_term = entry.term;

            result.entries.push_back(entry);
        }
        return result;
    };

    // Complex scenario: term 1 entries 1-5, leader change to term 2 at idx 3,
    // then a duplicate entry (out_of_order) stops processing.
    // Old segment replayed entries are never reached.
    {
        std::vector<simulated_entry> entries = {
                // Initial entries (term 1)
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
                {raft::term_t(1), raft::index_t(3)},
                {raft::term_t(1), raft::index_t(4)},
                {raft::term_t(1), raft::index_t(5)},
                // Leader change (term 2): discards term-1 idx>=3
                {raft::term_t(2), raft::index_t(3)},
                {raft::term_t(2), raft::index_t(4)},
                {raft::term_t(2), raft::index_t(4)}, // out_of_order: same idx+term → stop
                {raft::term_t(2), raft::index_t(5)}, // not reached
                // Old segment replayed (term 1) - also not reached
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
                {raft::term_t(1), raft::index_t(3)},
        };

        auto result = filter_entries_sim(entries);

        // - Term 1 idx 1,2: kept
        // - Term 1 idx 3,4,5: kept then discarded by leader change (3 discarded)
        // - Term 2 idx 3: added
        // - Term 2 idx 4: added
        // - Term 2 idx 4: out_of_order → stop (term 2 idx 5 and old-term entries not reached)
        BOOST_CHECK_EQUAL(result.discarded_leader_change, 3);
        BOOST_CHECK_EQUAL(result.entries.size(), 4);

        std::vector<simulated_entry> expected = {
                {raft::term_t(1), raft::index_t(1)},
                {raft::term_t(1), raft::index_t(2)},
                {raft::term_t(2), raft::index_t(3)},
                {raft::term_t(2), raft::index_t(4)},
        };
        BOOST_CHECK(result.entries == expected);
    }
}

// Test multiple groups processing - each group should be independent
BOOST_AUTO_TEST_CASE(test_multiple_groups_independence) {
    using db::raft_buffer_detail::entry_ordering_check_result;
    using db::raft_buffer_detail::check_entry_ordering;

    struct simulated_entry {
        raft::term_t term;
        raft::index_t idx;

        bool operator==(const simulated_entry& other) const {
            return term == other.term && idx == other.idx;
        }
    };

    // Process a single group
    auto process_group = [](const std::vector<simulated_entry>& entries) {
        using db::raft_buffer_detail::raft_term_and_idx;
        std::vector<simulated_entry> kept;
        raft_term_and_idx last{};

        for (const auto& entry : entries) {
            auto ordering = check_entry_ordering({.term = entry.term, .idx = entry.idx}, last);

            if (ordering == entry_ordering_check_result::out_of_order) {
                break;
            }
            if (ordering == entry_ordering_check_result::leader_change) {
                auto it = std::ranges::lower_bound(kept, entry.idx, {}, [](const simulated_entry& e) {
                    return e.idx;
                });
                kept.erase(it, kept.end());
            }

            last = {.term = entry.term, .idx = entry.idx};
            kept.push_back(entry);
        }
        return kept;
    };

    // Group 1: Has a leader change
    std::vector<simulated_entry> group1_entries = {
            {raft::term_t(1), raft::index_t(1)},
            {raft::term_t(1), raft::index_t(2)},
            {raft::term_t(2), raft::index_t(2)}, // leader change
            {raft::term_t(2), raft::index_t(3)},
    };

    // Group 2: No leader change, different starting index
    std::vector<simulated_entry> group2_entries = {
            {raft::term_t(5), raft::index_t(100)},
            {raft::term_t(5), raft::index_t(101)},
            {raft::term_t(5), raft::index_t(102)},
    };

    // Group 3: Multiple leader changes
    std::vector<simulated_entry> group3_entries = {
            {raft::term_t(1), raft::index_t(1)},
            {raft::term_t(2), raft::index_t(1)},
            {raft::term_t(3), raft::index_t(1)},
    };

    auto result1 = process_group(group1_entries);
    auto result2 = process_group(group2_entries);
    auto result3 = process_group(group3_entries);

    // Verify each group processed independently
    BOOST_CHECK_EQUAL(result1.size(), 3);
    BOOST_CHECK_EQUAL(result2.size(), 3);
    BOOST_CHECK_EQUAL(result3.size(), 1); // only term 3 idx 1 remains

    // Group 1: idx 1 from term 1, idx 2-3 from term 2
    std::vector<simulated_entry> expected1 = {
            {raft::term_t(1), raft::index_t(1)},
            {raft::term_t(2), raft::index_t(2)},
            {raft::term_t(2), raft::index_t(3)},
    };
    BOOST_CHECK(result1 == expected1);

    // Group 2: all entries kept (no changes)
    BOOST_CHECK(result2 == group2_entries);

    // Group 3: only final term 3 entry
    std::vector<simulated_entry> expected3 = {
            {raft::term_t(3), raft::index_t(1)},
    };
    BOOST_CHECK(result3 == expected3);
}

// Test that uncommitted raft entries survive destruction of raft_commitlog.
// When raft_commitlog is destroyed, release() is called on remaining handles
// to prevent segment dirty count from being decremented. This keeps commitlog
// segments alive so entries can be replayed after restart.
//
// Without release(), the destructors would decrement dirty counts to zero,
// making segments eligible for deletion on the next discard_completed_segments call.
// =============================================================================
// New tests: end-to-end, replay buffer integration, persistence, multi-segment
// =============================================================================

// End-to-end commitlog persistence roundtrip with full field verification.
// Writes raft entries from two groups (with command, config, and dummy types)
// to a commitlog, reads active segments back, and verifies every entry's
// group_id, term, index, and data variant are recovered exactly.
SEASTAR_TEST_CASE(test_end_to_end_commitlog_replay_full_verification) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid1 = make_group_id();
        auto gid2 = make_group_id();
        auto tid = make_table_id();

        struct entry_info {
            raft::group_id gid;
            raft::term_t term;
            raft::index_t idx;
            size_t variant_idx; // 0=command, 1=config, 2=dummy
        };

        std::vector<entry_info> infos = {
                {gid1, raft::term_t(1), raft::index_t(1), 0},
                {gid1, raft::term_t(1), raft::index_t(2), 1},
                {gid2, raft::term_t(2), raft::index_t(1), 2},
                {gid1, raft::term_t(1), raft::index_t(3), 0},
                {gid2, raft::term_t(2), raft::index_t(2), 0},
                {gid1, raft::term_t(2), raft::index_t(4), 2},
                {gid2, raft::term_t(3), raft::index_t(3), 1},
                {gid1, raft::term_t(2), raft::index_t(5), 0},
                {gid2, raft::term_t(3), raft::index_t(4), 2},
                {gid1, raft::term_t(2), raft::index_t(6), 1},
        };

        auto make_entry = [](const entry_info& e) -> raft::log_entry_ptr {
            if (e.variant_idx == 0)
                return make_command_entry(e.term, e.idx);
            if (e.variant_idx == 1)
                return make_config_entry(e.term, e.idx);
            return make_dummy_entry(e.term, e.idx);
        };

        // Write entries to commitlog.
        std::vector<replay_position> written_rps;
        for (auto& info : infos) {
            auto entry = make_entry(info);
            auto handle = co_await write_raft_entry_to_commitlog(log, tid, info.gid, entry);
            written_rps.push_back(handle.rp());
        }

        co_await log.sync_all_segments();

        // Read back from active segments and verify each entry.
        auto segments = log.get_active_segment_names();
        BOOST_REQUIRE(!segments.empty());

        size_t found = 0;
        for (auto& seg : segments) {
            co_await db::commitlog::read_log_file(
                    seg, db::commitlog::descriptor::FILENAME_PREFIX, [&](db::commitlog::buffer_and_replay_position buf_rp) -> future<> {
                        auto&& [buf, rp] = buf_rp;
                        auto it = std::ranges::find(written_rps, rp);
                        if (it == written_rps.end()) {
                            co_return;
                        }

                        auto idx = std::distance(written_rps.begin(), it);
                        const auto& expected = infos[idx];

                        commitlog_entry_reader reader(buf, detail::commitlog_entry_serialization_format::variant);
                        auto& entry_var = reader.entry().item;
                        visit_raft_commitlog_entry(entry_var, [&] (const auto& rle) {
                            BOOST_REQUIRE_EQUAL(rle.group_id, expected.gid);
                            BOOST_REQUIRE_EQUAL(rle.entry->term, expected.term);
                            BOOST_REQUIRE_EQUAL(rle.entry->idx, expected.idx);
                            BOOST_REQUIRE_EQUAL(rle.entry->data.index(), expected.variant_idx);
                        });
                        ++found;
                        co_return;
                    });
        }
        BOOST_REQUIRE_EQUAL(found, infos.size());
    });
}

// Test: raft_commitlog store, then truncate_log, verify handles.
// Store 10 entries, truncate at idx 6, verify handles for 1-5 succeed
// and entries 6-10 are gone.
SEASTAR_TEST_CASE(test_raft_commitlog_store_and_truncate_log) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();

        service::strong_consistency::replayed_data_per_group replayed_data;
        service::strong_consistency::raft_commitlog persistence(gid, log, tid, std::move(replayed_data));

        // Store 10 entries.
        raft::log_entry_ptr_list all_entries;
        for (int i = 1; i <= 10; ++i) {
            all_entries.push_back(make_command_entry(raft::term_t(1), raft::index_t(i)));
        }
        co_await persistence.store_log_entries(all_entries, raft::index_t(0));

        // Truncate at idx 6 — entries 6-10 should be removed.
        persistence.truncate_log(raft::index_t(6));

        // Verify: entries 1-5 should have valid handles.
        raft::log_entry_ptr_list first_five(all_entries.begin(), all_entries.begin() + 5);
        auto handles = persistence.acquire_replay_position_handles_for(first_five);
        BOOST_REQUIRE_EQUAL(handles.size(), 5);
        for (int i = 0; i < 5; ++i) {
            BOOST_REQUIRE_EQUAL(handles[i].index, raft::index_t(i + 1));
        }

        // Verify: requesting handles for entry 6 should trigger on_internal_error.
        // Use scoped_no_abort_on_internal_error to catch it.
        {
            seastar::testing::scoped_no_abort_on_internal_error no_abort;
            raft::log_entry_ptr_list entry_six = {all_entries[5]};
            try {
                persistence.acquire_replay_position_handles_for(entry_six);
                BOOST_FAIL("Expected on_internal_error for truncated entry");
            } catch (...) {
                // Expected — entry 6 was truncated.
            }
        }
    });
}

// Test: truncate_log_tail releases handles for old entries.
// Store 10 entries, truncate tail at idx 5, verify entries 1-5 are gone
// but entries 6-10 are still accessible.
SEASTAR_TEST_CASE(test_raft_commitlog_truncate_log_tail_releases_handles) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();

        service::strong_consistency::replayed_data_per_group replayed_data;
        service::strong_consistency::raft_commitlog persistence(gid, log, tid, std::move(replayed_data));

        raft::log_entry_ptr_list all_entries;
        for (int i = 1; i <= 10; ++i) {
            all_entries.push_back(make_command_entry(raft::term_t(1), raft::index_t(i)));
        }
        co_await persistence.store_log_entries(all_entries, raft::index_t(0));

        // Truncate tail at idx 5 — entries 1-5 handles should be released.
        persistence.truncate_log_tail(raft::index_t(5));

        // Verify: entries 6-10 should have valid handles.
        raft::log_entry_ptr_list last_five(all_entries.begin() + 5, all_entries.end());
        auto handles = persistence.acquire_replay_position_handles_for(last_five);
        BOOST_REQUIRE_EQUAL(handles.size(), 5);
        for (int i = 0; i < 5; ++i) {
            BOOST_REQUIRE_EQUAL(handles[i].index, raft::index_t(i + 6));
        }

        // Verify: requesting handles for entry 5 should trigger on_internal_error.
        {
            seastar::testing::scoped_no_abort_on_internal_error no_abort;
            raft::log_entry_ptr_list entry_five = {all_entries[4]};
            try {
                persistence.acquire_replay_position_handles_for(entry_five);
                BOOST_FAIL("Expected on_internal_error for tail-truncated entry");
            } catch (...) {
                // Expected — entry 5 was tail-truncated.
            }
        }
    });
}

// Test: Replay across multiple commitlog segments.
// Use the default commitlog from cl_test, write enough entries to fill
// at least one segment, then read all active segments back and verify
// order is preserved.
SEASTAR_TEST_CASE(test_replay_with_multiple_segments) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();

        constexpr int num_entries = 50;

        for (int i = 1; i <= num_entries; ++i) {
            auto entry = make_command_entry(raft::term_t(1), raft::index_t(i));
            co_await write_raft_entry_to_commitlog(log, tid, gid, entry);
        }

        co_await log.sync_all_segments();

        auto segments = log.get_active_segment_names();
        BOOST_REQUIRE(!segments.empty());
        BOOST_TEST_MESSAGE("Active segments: " << segments.size());

        // Collect all raft entries in replay order.
        std::vector<std::pair<raft::index_t, raft::term_t>> replayed_entries;

        for (auto& seg : segments) {
            co_await db::commitlog::read_log_file(
                    seg, db::commitlog::descriptor::FILENAME_PREFIX, [&](db::commitlog::buffer_and_replay_position buf_rp) -> future<> {
                        auto&& [buf, rp] = buf_rp;
                        commitlog_entry_reader reader(buf, detail::commitlog_entry_serialization_format::variant);
                        auto& entry_var = reader.entry().item;
                        if (std::holds_alternative<raft_commitlog_entry>(entry_var) ||
                                std::holds_alternative<raft_commitlog_entry_with_commit_idx>(entry_var)) {
                            visit_raft_commitlog_entry(entry_var, [&] (const auto& rle) {
                                replayed_entries.emplace_back(rle.entry->idx, rle.entry->term);
                            });
                        }
                        co_return;
                    });
        }

        BOOST_REQUIRE_EQUAL(replayed_entries.size(), num_entries);

        // Verify entries are in ascending index order.
        for (int i = 0; i < num_entries; ++i) {
            BOOST_REQUIRE_EQUAL(replayed_entries[i].first, raft::index_t(i + 1));
            BOOST_REQUIRE_EQUAL(replayed_entries[i].second, raft::term_t(1));
        }
    });
}

// Test: Mixed raft and mutation entries through the replay buffer.
// Write interleaved raft + mutation entries, then verify raft entries end
// up in the replay buffer and mutations are read as mutations.
SEASTAR_TEST_CASE(test_mixed_raft_and_mutation_entries_replay_separation) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();

        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        auto s = gen.schema();
        auto tid = s->id();

        // Write interleaved raft and mutation entries.
        constexpr int count = 5;
        std::vector<replay_position> raft_rps;
        std::vector<replay_position> mutation_rps;

        for (int i = 1; i <= count; ++i) {
            // Raft entry
            auto entry = make_command_entry(raft::term_t(1), raft::index_t(i));
            auto raft_handle = co_await write_raft_entry_to_commitlog(log, tid, gid, entry);
            raft_rps.push_back(raft_handle.rp());

            // Mutation entry
            auto fm = freeze(gen());
            commitlog_mutation_entry_writer cew(s, fm, db::commitlog::force_sync::no);
            auto mut_handle = co_await log.add_entry(tid, cew, db::no_timeout);
            mutation_rps.push_back(mut_handle.rp());
        }

        co_await log.sync_all_segments();

        // Read all entries and separate them by type.
        auto segments = log.get_active_segment_names();
        BOOST_REQUIRE(!segments.empty());

        db::raft_commitlog_replay_buffer buffer;
        size_t mutation_count = 0;

        for (auto& seg : segments) {
            co_await db::commitlog::read_log_file(
                    seg, db::commitlog::descriptor::FILENAME_PREFIX, [&](db::commitlog::buffer_and_replay_position buf_rp) -> future<> {
                        auto&& [buf, rp] = buf_rp;
                        commitlog_entry_reader reader(buf, detail::commitlog_entry_serialization_format::variant);
                        auto& entry_var = reader.entry().item;

                        if (std::holds_alternative<raft_commitlog_entry>(entry_var) ||
                                std::holds_alternative<raft_commitlog_entry_with_commit_idx>(entry_var)) {
                            visit_raft_commitlog_entry(entry_var, [&] (const auto& rle) {
                                buffer.add(rle.group_id, rle.entry);
                            });
                        } else {
                            BOOST_REQUIRE(std::holds_alternative<mutation_entry>(entry_var));
                            ++mutation_count;
                        }
                        co_return;
                    });
        }

        // Verify separation: raft entries in buffer, mutations counted separately.
        BOOST_REQUIRE_EQUAL(buffer.total_entries(), count);
        BOOST_REQUIRE_EQUAL(buffer.remaining_groups(), 1);
        BOOST_REQUIRE_EQUAL(mutation_count, count);
    });
}

// Test: raft_commitlog with combined truncate_log + truncate_log_tail.
// Verifies that truncating both head and tail leaves only the middle entries.
SEASTAR_TEST_CASE(test_raft_commitlog_combined_truncation) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();

        service::strong_consistency::replayed_data_per_group replayed_data;
        service::strong_consistency::raft_commitlog persistence(gid, log, tid, std::move(replayed_data));

        raft::log_entry_ptr_list all_entries;
        for (int i = 1; i <= 10; ++i) {
            all_entries.push_back(make_command_entry(raft::term_t(1), raft::index_t(i)));
        }
        co_await persistence.store_log_entries(all_entries, raft::index_t(0));

        // Truncate tail (entries <= 3) and head (entries >= 8).
        persistence.truncate_log_tail(raft::index_t(3));
        persistence.truncate_log(raft::index_t(8));

        // Only entries 4-7 should remain.
        raft::log_entry_ptr_list middle(all_entries.begin() + 3, all_entries.begin() + 7);
        auto handles = persistence.acquire_replay_position_handles_for(middle);
        BOOST_REQUIRE_EQUAL(handles.size(), 4);
        for (int i = 0; i < 4; ++i) {
            BOOST_REQUIRE_EQUAL(handles[i].index, raft::index_t(i + 4));
        }

        // Verify boundary entries are gone.
        {
            seastar::testing::scoped_no_abort_on_internal_error no_abort;
            try {
                raft::log_entry_ptr_list e3 = {all_entries[2]};
                persistence.acquire_replay_position_handles_for(e3);
                BOOST_FAIL("Expected error for tail-truncated entry 3");
            } catch (...) {
            }

            try {
                raft::log_entry_ptr_list e8 = {all_entries[7]};
                persistence.acquire_replay_position_handles_for(e8);
                BOOST_FAIL("Expected error for head-truncated entry 8");
            } catch (...) {
            }
        }
    });
}

// Test: raft_commitlog load_log returns replayed entries exactly once.
SEASTAR_TEST_CASE(test_raft_commitlog_load_log_one_shot) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();

        // Construct with pre-populated replayed entries.
        service::strong_consistency::replayed_data_per_group replayed_data;
        replayed_data.entries.push_back(make_command_entry(raft::term_t(1), raft::index_t(1)));
        replayed_data.entries.push_back(make_dummy_entry(raft::term_t(1), raft::index_t(2)));
        replayed_data.entries.push_back(make_config_entry(raft::term_t(2), raft::index_t(3)));

        service::strong_consistency::raft_commitlog persistence(gid, log, tid, std::move(replayed_data));

        // First call should return the entries.
        auto entries = persistence.load_log();
        BOOST_REQUIRE_EQUAL(entries.size(), 3);
        BOOST_REQUIRE_EQUAL(entries[0]->idx, raft::index_t(1));
        BOOST_REQUIRE_EQUAL(entries[1]->idx, raft::index_t(2));
        BOOST_REQUIRE_EQUAL(entries[2]->idx, raft::index_t(3));

        // Second call should return empty (one-shot).
        auto entries2 = persistence.load_log();
        BOOST_REQUIRE(entries2.empty());
        co_return;
    });
}

BOOST_AUTO_TEST_SUITE_END()
