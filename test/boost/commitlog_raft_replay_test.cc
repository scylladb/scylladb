/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <algorithm>
#include <unordered_set>
#include <boost/test/unit_test.hpp>
#include <fmt/ranges.h>

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
#include "service/strong_consistency/raft_groups_storage.hh"
#include "db/system_keyspace.hh"
#include "locator/tablets.hh"
#include "locator/token_metadata.hh"
#include "dht/token.hh"
#include "idl/raft_storage.dist.hh"
#include "idl/raft_storage.dist.impl.hh"
#include "mutation/mutation.hh"
#include "service/strong_consistency/state_machine.hh"
#include "idl/strong_consistency/state_machine.dist.hh"
#include "idl/strong_consistency/state_machine.dist.impl.hh"
#include "test/lib/cql_assertions.hh"

// A seam into raft_commitlog_replay_buffer's parked records: finish_replay()
// would need a database, a query processor and tablet metadata.
class raft_replay_buffer_tester {
public:
    static void seed(db::raft_commitlog_replay_buffer& buffer, raft::group_id gid,
            service::strong_consistency::replayed_data_per_group data) {
        buffer._per_group_data[gid] = std::move(data);
    }
};

BOOST_AUTO_TEST_SUITE(commitlog_raft_replay_test)

using namespace db;

namespace {

raft::log_entry_ptr make_dummy_entry(raft::term_t term, raft::index_t idx) {
    return make_lw_shared<raft::log_entry>(raft::log_entry{.term = term, .idx = idx, .data = raft::log_entry::dummy{}});
}

raft::log_entry_ptr make_command_entry_sized(raft::term_t term, raft::index_t idx, size_t payload_size) {
    raft::command cmd;
    ser::serialize(cmd, bytes(payload_size, 'x'));
    return make_lw_shared<raft::log_entry>(raft::log_entry{.term = term, .idx = idx, .data = std::move(cmd)});
}

raft::log_entry_ptr make_command_entry(raft::term_t term, raft::index_t idx) {
    raft::command cmd;
    ser::serialize(cmd, 123);
    return make_lw_shared<raft::log_entry>(raft::log_entry{.term = term, .idx = idx, .data = std::move(cmd)});
}

// A LeaseGuard-stamped entry. The bounds are deliberately not whole
// microseconds: raft::lease_clock fixes the wire unit at nanoseconds, so if that
// ever coarsened these digits would be lost rather than the change passing
// unnoticed.
constexpr int64_t lease_earliest_ns = 1'234'567'891;
constexpr int64_t lease_latest_ns = 1'234'567'893;

raft::log_entry_ptr make_lease_entry(raft::term_t term, raft::index_t idx) {
    return make_lw_shared<raft::log_entry>(raft::log_entry{.term = term, .idx = idx,
            .data = raft::log_entry::dummy{},
            .lease_time = raft::time_bounds{
                    raft::lease_clock::time_point(std::chrono::nanoseconds(lease_earliest_ns)),
                    raft::lease_clock::time_point(std::chrono::nanoseconds(lease_latest_ns))}});
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

future<> cl_test(commitlog::config cfg, noncopyable_function<future<>(commitlog&)> f) {
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

future<> cl_test(noncopyable_function<future<>(commitlog&)> f) {
    return cl_test(commitlog::config{}, std::move(f));
}

// Write a raft log entry to the commitlog and return the rp_handle.
future<rp_handle> write_raft_entry_to_commitlog(commitlog& cl, table_id tid, raft::group_id gid, raft::log_entry_ptr entry) {
    const std::vector<raft::log_entry_ptr> entries{entry};
    commitlog_raft_batch_writer writer(gid, raft::index_t{0}, entries);
    const auto target_size = writer.size();
    co_return co_await cl.add(tid, target_size, db::no_timeout, db::commitlog_force_sync::yes, [entries, gid](auto& out) {
        commitlog_raft_batch_writer w(gid, raft::index_t{0}, entries);
        w.write(out);
    });
}

} // anonymous namespace

// Test commitlog_raft_batch_writer: size computation is consistent with
// the serialized output, and a write/read roundtrip preserves all fields
// for every entry type (command, configuration, dummy, LeaseGuard-stamped).
//
// This is the *persisted* encoding, which is not the same byte format as the
// plain ser::serialize/ser::deserialize pair -- see the SCYLLADB-1029 note in
// db/commitlog/commitlog_entry.cc: the writer path
// (ser::writer_of_commitlog_entry) and the "manual" deserializer are not binary
// compatible, which is why replay reads through the view deserializer. So a
// field surviving ser::serialize says nothing about it surviving here, and
// log_entry::lease_time has to be asserted on this path too. The last entry
// below carries an interval and the others do not, covering both cases.
SEASTAR_TEST_CASE(test_commitlog_raft_batch_writer) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();

        std::vector<raft::log_entry_ptr> entries = {
                make_command_entry(raft::term_t(1), raft::index_t(1)),
                make_config_entry(raft::term_t(2), raft::index_t(2)),
                make_dummy_entry(raft::term_t(3), raft::index_t(3)),
                make_lease_entry(raft::term_t(4), raft::index_t(4)),
        };

        // Verify size() and accessor for each entry type, then write to commitlog.
        std::vector<replay_position> rps;
        for (const auto& entry : entries) {
            const std::vector<raft::log_entry_ptr> batch{entry};
            commitlog_raft_batch_writer writer(gid, raft::index_t{0}, batch);
            // size() must exceed the bare raft::log_entry serialization because
            // the writer wraps it in a commitlog_entry + raft_commitlog_batch envelope.
            BOOST_REQUIRE_GT(writer.size(), 0u);
            BOOST_REQUIRE_GT(writer.size(), ser::get_sizeof(*entry));
            BOOST_REQUIRE_EQUAL(writer.group_id(), gid);

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
                        BOOST_REQUIRE(std::holds_alternative<raft_commitlog_batch>(entry_var));

                        auto& rle = std::get<raft_commitlog_batch>(entry_var);
                        BOOST_REQUIRE_EQUAL(rle.group_id, gid);
                        BOOST_REQUIRE_EQUAL(rle.entries.at(0)->term, expected->term);
                        BOOST_REQUIRE_EQUAL(rle.entries.at(0)->idx, expected->idx);
                        BOOST_REQUIRE_EQUAL(rle.entries.at(0)->data.index(), expected->data.index());
                        // A LeaseGuard interval must survive the envelope intact,
                        // and an entry written without one must not gain one.
                        BOOST_REQUIRE_EQUAL(rle.entries.at(0)->lease_time.has_value(),
                                expected->lease_time.has_value());
                        if (expected->lease_time) {
                            BOOST_REQUIRE_EQUAL(
                                    rle.entries.at(0)->lease_time->earliest.time_since_epoch().count(),
                                    lease_earliest_ns);
                            BOOST_REQUIRE_EQUAL(
                                    rle.entries.at(0)->lease_time->latest.time_since_epoch().count(),
                                    lease_latest_ns);
                        }
                        ++found;
                        co_return;
                    });
        }
        BOOST_REQUIRE_EQUAL(found, entries.size());
    });
}

// A batch of several entries survives the round trip in order.
//
// The writer serializes entries one at a time into the batch's sequence, so a
// fault in that loop — a dropped entry, a miscounted length, entries reordered —
// only shows up with more than one entry in a batch, which every other writer
// test here has exactly one of.
SEASTAR_TEST_CASE(test_commitlog_raft_batch_writer_multiple_entries) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();
        const raft::index_t commit_idx{7};

        const std::vector<raft::log_entry_ptr> batch = {
                make_command_entry(raft::term_t(1), raft::index_t(11)),
                make_config_entry(raft::term_t(2), raft::index_t(12)),
                make_dummy_entry(raft::term_t(3), raft::index_t(13)),
                make_lease_entry(raft::term_t(4), raft::index_t(14)),
        };

        commitlog_raft_batch_writer writer(gid, commit_idx, batch);
        const auto handle = co_await log.add(tid, writer.size(), db::no_timeout,
                db::commitlog_force_sync::yes, [&writer](auto& out) { writer.write(out); });
        const auto written_at = handle.rp();
        co_await log.sync_all_segments();

        bool seen = false;
        for (const auto& name : log.get_active_segment_names()) {
            co_await commitlog::read_log_file(name, commitlog::descriptor::FILENAME_PREFIX,
                    [&](commitlog::buffer_and_replay_position buf_rp) -> future<> {
                if (buf_rp.position != written_at) {
                    co_return;
                }
                seen = true;
                commitlog_entry_reader reader(buf_rp.buffer,
                        detail::commitlog_entry_serialization_format::variant);
                auto& item = reader.entry().item;
                BOOST_REQUIRE(std::holds_alternative<raft_commitlog_batch>(item));
                auto& read = std::get<raft_commitlog_batch>(item);

                BOOST_REQUIRE_EQUAL(read.group_id, gid);
                BOOST_REQUIRE_EQUAL(read.commit_idx, commit_idx);
                BOOST_REQUIRE_EQUAL(read.entries.size(), batch.size());
                for (size_t i = 0; i < batch.size(); ++i) {
                    BOOST_REQUIRE_EQUAL(read.entries[i]->idx, batch[i]->idx);
                    BOOST_REQUIRE_EQUAL(read.entries[i]->term, batch[i]->term);
                    BOOST_REQUIRE_EQUAL(read.entries[i]->data.index(), batch[i]->data.index());
                }
            });
        }
        BOOST_REQUIRE(seen);
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
                        BOOST_REQUIRE(std::holds_alternative<raft_commitlog_batch>(entry_var));

                        auto& rle = std::get<raft_commitlog_batch>(entry_var);
                        BOOST_REQUIRE_EQUAL(rle.group_id, gid);

                        auto idx = std::distance(rps.begin(), it);
                        BOOST_REQUIRE_EQUAL(rle.entries.at(0)->idx, entries[idx]->idx);
                        BOOST_REQUIRE_EQUAL(rle.entries.at(0)->term, entries[idx]->term);
                        BOOST_REQUIRE(std::holds_alternative<raft::log_entry::dummy>(rle.entries.at(0)->data));

                        ++raft_entries_found;
                        co_return;
                    });
        }

        BOOST_REQUIRE_EQUAL(raft_entries_found, n);
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
                            BOOST_REQUIRE(std::holds_alternative<raft_commitlog_batch>(entry_var));
                            auto it = std::ranges::find(raft_rps, rp);
                            auto idx = std::distance(raft_rps.begin(), it);
                            const auto& expected = raft_entries[idx];

                            auto& rle = std::get<raft_commitlog_batch>(entry_var);
                            BOOST_REQUIRE_EQUAL(rle.group_id, gid);
                            BOOST_REQUIRE_EQUAL(rle.entries.at(0)->term, expected->term);
                            BOOST_REQUIRE_EQUAL(rle.entries.at(0)->idx, expected->idx);
                            BOOST_REQUIRE_EQUAL(rle.entries.at(0)->data.index(), expected->data.index());
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
                        BOOST_REQUIRE(std::holds_alternative<raft_commitlog_batch>(entry_var));

                        auto& rle = std::get<raft_commitlog_batch>(entry_var);
                        BOOST_REQUIRE_EQUAL(rle.group_id, expected.gid);
                        BOOST_REQUIRE_EQUAL(rle.entries.at(0)->term, expected.term);
                        BOOST_REQUIRE_EQUAL(rle.entries.at(0)->idx, expected.idx);
                        BOOST_REQUIRE_EQUAL(rle.entries.at(0)->data.index(), expected.variant_idx);
                        ++found;
                        co_return;
                    });
        }
        BOOST_REQUIRE_EQUAL(found, infos.size());
    });
}

// Test: one batch becomes one record, and truncate_log() clamps that record. The
// entries stay on disk, since the commitlog is append-only, so the truncation
// record is the only thing that tells replay they were superseded.
SEASTAR_TEST_CASE(test_raft_batch_record_and_truncation) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();
        auto rg_tid = make_table_id();

        raft::log_entry_ptr_list all_entries;
        for (int i = 1; i <= 10; ++i) {
            all_entries.push_back(make_command_entry(raft::term_t(1), raft::index_t(i)));
        }

        std::deque<service::strong_consistency::segment_record> segment_queue;
        // The whole batch is one commitlog entry, so one record.
        auto handle = co_await service::strong_consistency::write_raft_batch(
                log, tid, gid, raft::index_t(0), all_entries);
        service::strong_consistency::account_batch(segment_queue, rg_tid, std::move(handle), all_entries);
        BOOST_REQUIRE_EQUAL(segment_queue.size(), 1);
        BOOST_REQUIRE_EQUAL(segment_queue.front().first, raft::index_t(1));
        BOOST_REQUIRE_EQUAL(segment_queue.front().max, raft::index_t(10));
        BOOST_REQUIRE_EQUAL(segment_queue.front().max_term(), raft::term_t(1));
        BOOST_REQUIRE(segment_queue.front().last_cmd().has_value());
        BOOST_REQUIRE_EQUAL(*segment_queue.front().last_cmd(), raft::index_t(10));
        // Two references at the batch's position: the group's own and the one
        // under system.raft_groups.
        BOOST_REQUIRE(bool(segment_queue.front().pin_user_table));
        BOOST_REQUIRE(bool(segment_queue.front().pin_raft_groups));
        BOOST_REQUIRE(segment_queue.front().pin_user_table.rp() == segment_queue.front().pin_raft_groups.rp());

        // A leader change discards 6..10: max is clamped, the reference stays for 1..5.
        segment_queue.back().trim_from(raft::index_t(6));
        BOOST_REQUIRE_EQUAL(segment_queue.front().max, raft::index_t(5));
        BOOST_REQUIRE_EQUAL(*segment_queue.front().last_cmd(), raft::index_t(5));
        BOOST_REQUIRE(bool(segment_queue.front().pin_user_table));
    });
}

// Test: the release gate is the record's last *command*. Dummy and configuration
// entries never reach apply(), so gating on them would hold the record forever.
SEASTAR_TEST_CASE(test_raft_batch_record_release_gate) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();
        auto rg_tid = make_table_id();

        raft::log_entry_ptr_list mixed = {
            make_command_entry(raft::term_t(1), raft::index_t(1)),
            make_command_entry(raft::term_t(1), raft::index_t(2)),
            make_config_entry(raft::term_t(1), raft::index_t(3)),
            make_dummy_entry(raft::term_t(1), raft::index_t(4)),
        };
        std::deque<service::strong_consistency::segment_record> segment_queue;
        service::strong_consistency::account_batch(segment_queue, rg_tid,
                co_await service::strong_consistency::write_raft_batch(
                        log, tid, gid, raft::index_t(0), mixed), mixed);
        BOOST_REQUIRE_EQUAL(segment_queue.size(), 1);
        auto& rec = segment_queue.front();
        BOOST_REQUIRE_EQUAL(rec.max, raft::index_t(4));
        // The gate is command 2, not the dummy at 4.
        BOOST_REQUIRE_EQUAL(*rec.last_cmd(), raft::index_t(2));
        BOOST_REQUIRE_EQUAL(rec.noncmd_indexes.size(), 2);
        // The configuration is remembered so releasing the record can persist it.
        BOOST_REQUIRE(rec.last_conf().has_value());
        BOOST_REQUIRE_EQUAL(rec.last_conf()->first, raft::index_t(3));

        // A record of non-commands only has no gate: nothing will ever apply.
        raft::log_entry_ptr_list only_noncmd = {
            make_dummy_entry(raft::term_t(2), raft::index_t(5)),
            make_config_entry(raft::term_t(2), raft::index_t(6)),
        };
        std::deque<service::strong_consistency::segment_record> queue2;
        service::strong_consistency::account_batch(queue2, rg_tid,
                co_await service::strong_consistency::write_raft_batch(
                        log, tid, gid, raft::index_t(4), only_noncmd), only_noncmd);
        BOOST_REQUIRE_EQUAL(queue2.size(), 1);
        BOOST_REQUIRE(!queue2.front().last_cmd().has_value());
        BOOST_REQUIRE_EQUAL(queue2.front().max_term(), raft::term_t(2));
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
                        if (std::holds_alternative<raft_commitlog_batch>(entry_var)) {
                            auto& rle = std::get<raft_commitlog_batch>(entry_var);
                            replayed_entries.emplace_back(rle.entries.at(0)->idx, rle.entries.at(0)->term);
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

        // The separation lives in the on-disk format, so count the alternatives here.
        size_t raft_batch_count = 0;
        size_t raft_entry_count = 0;
        std::unordered_set<raft::group_id> raft_groups;
        size_t mutation_count = 0;

        for (auto& seg : segments) {
            co_await db::commitlog::read_log_file(
                    seg, db::commitlog::descriptor::FILENAME_PREFIX, [&](db::commitlog::buffer_and_replay_position buf_rp) -> future<> {
                        auto&& [buf, rp] = buf_rp;
                        commitlog_entry_reader reader(buf, detail::commitlog_entry_serialization_format::variant);
                        auto& entry_var = reader.entry().item;

                        if (std::holds_alternative<raft_commitlog_batch>(entry_var)) {
                            auto& rle = std::get<raft_commitlog_batch>(entry_var);
                            ++raft_batch_count;
                            raft_entry_count += rle.entries.size();
                            raft_groups.insert(rle.group_id);
                        } else {
                            BOOST_REQUIRE(std::holds_alternative<mutation_entry>(entry_var));
                            ++mutation_count;
                        }
                        co_return;
                    });
        }

        BOOST_REQUIRE_EQUAL(raft_batch_count, count);
        BOOST_REQUIRE_EQUAL(raft_entry_count, count);
        BOOST_REQUIRE_EQUAL(raft_groups.size(), 1);
        BOOST_REQUIRE_EQUAL(mutation_count, count);
    });
}

// Test: one record per segment, each with its own reference pair, since records
// are the unit of retention. A truncation pops the records it invalidates whole
// and clamps the one it lands inside.
SEASTAR_TEST_CASE(test_raft_batch_records_across_segments) {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(std::move(cfg), [](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();
        auto rg_tid = make_table_id();

        std::deque<service::strong_consistency::segment_record> segment_queue;
        raft::index_t next{1};
        // Each batch is one commitlog entry, so a handful of 4x64KB batches
        // fills a 1MB segment.
        while (segment_queue.size() < 3) {
            raft::log_entry_ptr_list batch;
            for (int i = 0; i < 4; ++i) {
                batch.push_back(make_command_entry_sized(raft::term_t(1), next, 64 * 1024));
                next = next + raft::index_t{1};
            }
            service::strong_consistency::account_batch(segment_queue, rg_tid,
                co_await service::strong_consistency::write_raft_batch(
                        log, tid, gid, raft::index_t(0), batch), batch);
        }

        for (size_t i = 0; i + 1 < segment_queue.size(); ++i) {
            BOOST_REQUIRE_LT(segment_queue[i].segment(), segment_queue[i + 1].segment());
            BOOST_REQUIRE_LT(segment_queue[i].max, segment_queue[i + 1].first);
            BOOST_REQUIRE(bool(segment_queue[i].pin_user_table));
            BOOST_REQUIRE(bool(segment_queue[i].pin_raft_groups));
        }

        // Truncate inside the middle record: records at or above the cut go away
        // whole, the one it lands in is clamped and keeps its references.
        const auto cut = segment_queue[1].first + raft::index_t{1};
        std::deque<service::strong_consistency::truncation_record> truncations;
        while (!segment_queue.empty() && segment_queue.back().first >= cut) {
            truncations.push_back(service::strong_consistency::truncation_record{
                    .segment = segment_queue.back().segment(), .from = segment_queue.back().first, .to = segment_queue.back().max});
            segment_queue.pop_back();
        }
        BOOST_REQUIRE(!segment_queue.empty());
        if (segment_queue.back().max >= cut) {
            truncations.push_back(service::strong_consistency::truncation_record{
                    .segment = segment_queue.back().segment(), .from = cut, .to = segment_queue.back().max});
            segment_queue.back().trim_from(cut);
        }
        BOOST_REQUIRE(!truncations.empty());
        BOOST_REQUIRE_LT(segment_queue.back().max, cut);
        BOOST_REQUIRE(bool(segment_queue.back().pin_user_table));
        for (const auto& truncation : truncations) {
            BOOST_REQUIRE_GT(truncation.segment, 0u);
            BOOST_REQUIRE_LE(truncation.from, truncation.to);
        }
    });
}

// Test: max_single_entry_batch_size() tightly bounds what write_raft_batch()
// measures for one entry. The check it feeds at startup is worthless once the
// number stops tracking the format.
SEASTAR_TEST_CASE(test_max_single_entry_batch_size_bounds_the_writer) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();

        for (const size_t payload : {size_t(0), size_t(4096), size_t(100 * 1024)}) {
            auto plain = make_command_entry_sized(raft::term_t(1), raft::index_t(1), payload);
            const auto command_size = std::get<raft::command>(plain->data).size();
            const auto bound = service::strong_consistency::max_single_entry_batch_size(command_size);

            const std::vector<raft::log_entry_ptr> plain_batch{plain};
            commitlog_raft_batch_writer plain_writer(gid, raft::index_t{0}, plain_batch);
            BOOST_REQUIRE_LE(plain_writer.size(), bound);

            // The bound is derived from the lease-stamped form, so here it must
            // be exact. A loose bound like SIZE_MAX would pass the check above.
            auto stamped = make_lw_shared<const raft::log_entry>(raft::log_entry{
                    .term = raft::term_t(1), .idx = raft::index_t(1),
                    .data = std::get<raft::command>(plain->data),
                    .lease_time = raft::time_bounds{
                            raft::lease_clock::time_point(std::chrono::nanoseconds(lease_earliest_ns)),
                            raft::lease_clock::time_point(std::chrono::nanoseconds(lease_latest_ns))}});
            const std::vector<raft::log_entry_ptr> stamped_batch{stamped};
            commitlog_raft_batch_writer stamped_writer(gid, raft::index_t{0}, stamped_batch);
            BOOST_REQUIRE_EQUAL(stamped_writer.size(), bound);
        }
        co_return;
    });
}

// Test: a batch too large for one commitlog entry raises an internal error.
// Fragmenting it would put one copy of an entry in two segments; the records and
// the truncation records need a copy to live in exactly one segment (see
// write_raft_batch()). allow_fragmented_entries is on, as in production: with it
// off commitlog::add() rejects the batch by itself, so the check under test would
// never run.
SEASTAR_TEST_CASE(test_raft_batch_too_large_is_an_internal_error) {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    cfg.allow_fragmented_entries = true;
    return cl_test(std::move(cfg), [](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();

        // A single commitlog entry is capped at half a segment, so 16 x 64KB is over.
        raft::log_entry_ptr_list big;
        for (int i = 1; i <= 16; ++i) {
            big.push_back(make_command_entry_sized(raft::term_t(1), raft::index_t(i), 64 * 1024));
        }
        BOOST_REQUIRE_GT(big.size() * 64 * 1024, log.max_record_size());

        seastar::testing::scoped_no_abort_on_internal_error no_abort;
        try {
            co_await service::strong_consistency::write_raft_batch(
                    log, tid, gid, raft::index_t(0), big);
            BOOST_FAIL("expected an oversized batch to be rejected");
        } catch (const std::runtime_error& e) {
            // on_internal_error's exception, not the commitlog's invalid_argument.
            BOOST_REQUIRE(sstring(e.what()).find("does not fit in one commitlog entry")
                    != sstring::npos);
        }
    });
}

// Test: records no group claimed are detached at stop(), not released, for the
// reason raft_commitlog_replay_buffer::stop() gives. The segments must stay dirty
// here, unlike in test_raft_commitlog_release_all_frees_the_segments.
SEASTAR_TEST_CASE(test_replay_buffer_stop_detaches_unclaimed_records) {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(std::move(cfg), [](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();
        auto rg_tid = make_table_id();

        // Build the records as finish_replay() does: write the tail as a batch
        // and account it. Fill past one segment: only sealed ones are dirty.
        service::strong_consistency::replayed_data_per_group data;
        raft::index_t next{1};
        while (log.get_num_dirty_segments() == 0) {
            raft::log_entry_ptr_list batch;
            for (int i = 0; i < 4; ++i) {
                batch.push_back(make_command_entry_sized(raft::term_t(1), next, 64 * 1024));
                next = next + raft::index_t{1};
            }
            auto handle = co_await service::strong_consistency::write_raft_batch(
                    log, tid, gid, raft::index_t(0), batch);
            service::strong_consistency::account_batch(data.records, rg_tid, std::move(handle), batch);
        }
        const auto dirty = log.get_num_dirty_segments();
        BOOST_REQUIRE_GT(dirty, 0);
        BOOST_REQUIRE(!data.records.empty());

        {
            db::raft_commitlog_replay_buffer buffer;
            raft_replay_buffer_tester::seed(buffer, gid, std::move(data));
            co_await buffer.stop();
        }
        // Asserted after the buffer is gone: a stop() that only cleared the map
        // would leave the destructor to decrement, and this would go clean.
        BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), dirty);
    });
}

// Test: the same holds when stop() never runs. No production path reaches this
// today (see the destructor's comment), but a destructor that decremented the
// pins would retire the segments holding a rewritten tail.
SEASTAR_TEST_CASE(test_replay_buffer_destructor_detaches_unclaimed_records) {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(std::move(cfg), [](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();
        auto rg_tid = make_table_id();

        service::strong_consistency::replayed_data_per_group data;
        raft::index_t next{1};
        while (log.get_num_dirty_segments() == 0) {
            raft::log_entry_ptr_list batch;
            for (int i = 0; i < 4; ++i) {
                batch.push_back(make_command_entry_sized(raft::term_t(1), next, 64 * 1024));
                next = next + raft::index_t{1};
            }
            auto handle = co_await service::strong_consistency::write_raft_batch(
                    log, tid, gid, raft::index_t(0), batch);
            service::strong_consistency::account_batch(data.records, rg_tid, std::move(handle), batch);
        }
        const auto dirty = log.get_num_dirty_segments();
        BOOST_REQUIRE_GT(dirty, 0);
        BOOST_REQUIRE(!data.records.empty());

        {
            db::raft_commitlog_replay_buffer buffer;
            raft_replay_buffer_tester::seed(buffer, gid, std::move(data));
            // Deliberately no stop().
        }
        BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), dirty);
    });
}

// Test: a group destroyed deliberately gives up its segment references instead
// of detaching them — see raft_commitlog::release_all() (SCYLLADB-3827).
SEASTAR_TEST_CASE(test_raft_commitlog_release_all_frees_the_segments) {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(std::move(cfg), [](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();
        auto rg_tid = make_table_id();

        {
            service::strong_consistency::raft_commitlog rc(gid, log, tid, rg_tid, {});

            // Fill past one segment: only sealed ones are dirty and reclaimable.
            raft::index_t next{1};
            while (log.get_num_dirty_segments() == 0) {
                raft::log_entry_ptr_list batch;
                for (int i = 0; i < 4; ++i) {
                    batch.push_back(make_command_entry_sized(raft::term_t(1), next, 64 * 1024));
                    next = next + raft::index_t{1};
                }
                co_await rc.store_log_entries(batch, raft::index_t(0));
            }
            BOOST_REQUIRE_GT(log.get_num_dirty_segments(), 0);
            BOOST_REQUIRE(bool(rc.pin_for_apply(raft::index_t(1))));

            rc.release_all();

            // No record holds anything any more...
            {
                seastar::testing::scoped_no_abort_on_internal_error no_abort;
                try {
                    rc.pin_for_apply(raft::index_t(1));
                    BOOST_FAIL("Expected the records to have been released");
                } catch (...) {
                    // Expected.
                }
            }
            // ...and the segments it kept dirty are clean, as detaching would not do.
            BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), 0);
        }
    });
}

// Test: destroying a group without release_all() detaches its references, so the
// segments stay dirty (the shutdown path, log_disposition::keep). Pairs with the
// test above: either alone passes if both paths behave the same.
SEASTAR_TEST_CASE(test_raft_commitlog_destructor_detaches_the_segments) {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(std::move(cfg), [](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();
        auto rg_tid = make_table_id();

        uint64_t dirty = 0;
        {
            service::strong_consistency::raft_commitlog rc(gid, log, tid, rg_tid, {});

            raft::index_t next{1};
            while (log.get_num_dirty_segments() == 0) {
                raft::log_entry_ptr_list batch;
                for (int i = 0; i < 4; ++i) {
                    batch.push_back(make_command_entry_sized(raft::term_t(1), next, 64 * 1024));
                    next = next + raft::index_t{1};
                }
                co_await rc.store_log_entries(batch, raft::index_t(0));
            }
            dirty = log.get_num_dirty_segments();
            BOOST_REQUIRE_GT(dirty, 0);
        }
        // Asserted after the group is gone: a destructor that dropped the
        // handles instead of detaching them would bring this to zero.
        BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), dirty);
    });
}

// The other encoding of lease_time: the plain ser::serialize/ser::deserialize
// pair, which is what idl/raft.idl.hh uses for append_request::entries. That is
// the replication path, and for LeaseGuard it is the primary one -- "the log is
// the lease" reaches a new leader over append_entries, not off disk. The
// persisted encoding is a different byte format and is covered by
// test_commitlog_raft_batch_writer above; neither test substitutes for the
// other.
//
// A misread here is silent and unsafe: a lease decoded as younger than it is
// lets a deposed leader serve a stale local read. So the encoding must be a
// fixed unit rather than whatever std::chrono::system_clock::period happens to
// be for the build (see raft::lease_clock). The bounds are deliberately not
// whole microseconds, so a coarsened unit drops digits here. Cross-build
// divergence is what the static_asserts in raft/bounded_clock.hh guard -- a
// round trip cannot see it, since both ends share a standard library.
BOOST_AUTO_TEST_CASE(test_log_entry_lease_time_round_trip) {
    constexpr int64_t earliest_ns = lease_earliest_ns;
    constexpr int64_t latest_ns = lease_latest_ns;

    raft::log_entry_ptr entry = make_lease_entry(raft::term_t(7), raft::index_t(11));

    bytes_ostream buf;
    ser::serialize(buf, entry);
    auto bv = buf.linearize();
    auto in = ser::as_input_stream(bv);
    auto decoded = ser::deserialize(in, std::type_identity<raft::log_entry_ptr>());

    BOOST_REQUIRE(decoded->lease_time);
    BOOST_REQUIRE_EQUAL(decoded->lease_time->earliest.time_since_epoch().count(), earliest_ns);
    BOOST_REQUIRE_EQUAL(decoded->lease_time->latest.time_since_epoch().count(), latest_ns);

    // An absent interval must stay absent (leases disabled, or an unsynchronized
    // clock at the time the entry was created).
    raft::log_entry_ptr no_lease = make_lw_shared<raft::log_entry>(raft::log_entry{
            .term = raft::term_t(7), .idx = raft::index_t(12), .data = raft::log_entry::dummy{}});
    bytes_ostream buf2;
    ser::serialize(buf2, no_lease);
    auto bv2 = buf2.linearize();
    auto in2 = ser::as_input_stream(bv2);
    BOOST_REQUIRE(!ser::deserialize(in2, std::type_identity<raft::log_entry_ptr>())->lease_time);
}

// Test: rp_handle::clone() takes an extra reference at a live handle's position,
// under the same or a different column family, any number of times. Asserted
// through the segment's dirty state: a clone that took no reference satisfies
// every rp() and bool() check, and mark_clean() no-ops once a cf's count is gone.
SEASTAR_TEST_CASE(test_rp_handle_clone) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();
        auto other_tid = make_table_id();

        auto entry = make_command_entry(raft::term_t(1), raft::index_t(1));
        std::optional handle = co_await write_raft_entry_to_commitlog(log, tid, gid, entry);
        BOOST_REQUIRE(bool(*handle));

        // A second reference under the entry's own cf, at the same position.
        std::optional dup = handle->clone(tid);
        BOOST_REQUIRE(bool(*dup));
        BOOST_REQUIRE(dup->rp() == handle->rp());

        // References under a cf the segment was never written for, as
        // account_batch() takes them for system.raft_groups; repeats are legal.
        std::optional pin1 = handle->clone(other_tid);
        std::optional pin2 = handle->clone(other_tid);
        BOOST_REQUIRE(bool(*pin1));
        BOOST_REQUIRE(pin1->rp() == handle->rp());
        BOOST_REQUIRE(bool(*pin2));

        // A reference cloned from a reference works the same.
        std::optional chained = pin1->clone(other_tid);
        BOOST_REQUIRE(chained->rp() == handle->rp());

        // Only a sealed segment reports as dirty, so the count shows up only now.
        co_await log.force_new_active_segment();
        BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), 1);

        // Every step but the last must leave the segment dirty, or clone() did not count.
        handle.reset();
        BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), 1);
        dup.reset();
        BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), 1);
        pin1.reset();
        BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), 1);
        pin2.reset();
        BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), 1);
        chained.reset();
        BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), 0);
    });
}

// Test: the copies a truncation superseded are dropped, and only those. A
// truncation removes a suffix of the raft log, so the discarded copies are the
// batch's tail; the cursor walks the record as the copies are read.
BOOST_AUTO_TEST_CASE(test_replay_drop_stale_copies) {
    using db::raft_buffer_detail::drop_stale_copies;
    using db::raft_buffer_detail::segment_cursors;
    using db::raft_buffer_detail::truncation_cursor;

    const auto batch = [] {
        std::vector<raft::log_entry_ptr> v;
        for (int i = 1; i <= 5; ++i) {
            v.push_back(make_command_entry(raft::term_t(1), raft::index_t(i)));
        }
        return v;
    };

    // "indexes 3..5 of this segment were truncated": 1 and 2 survive.
    {
        segment_cursors cursors{truncation_cursor{
                .from = raft::index_t(3), .to = raft::index_t(5), .next = raft::index_t(3)}};
        auto rest = drop_stale_copies(cursors, batch());
        BOOST_REQUIRE_EQUAL(rest.size(), 2);
        BOOST_REQUIRE_EQUAL(rest[0]->idx, raft::index_t(1));
        BOOST_REQUIRE_EQUAL(rest[1]->idx, raft::index_t(2));
        BOOST_REQUIRE(cursors.front().exhausted());
    }

    // A record for a range this batch does not reach leaves it untouched.
    {
        segment_cursors cursors{truncation_cursor{
                .from = raft::index_t(9), .to = raft::index_t(12), .next = raft::index_t(9)}};
        auto rest = drop_stale_copies(cursors, batch());
        BOOST_REQUIRE_EQUAL(rest.size(), 5);
        BOOST_REQUIRE(!cursors.front().exhausted());
    }

    // No records at all: nothing is stale.
    {
        segment_cursors cursors;
        BOOST_REQUIRE_EQUAL(drop_stale_copies(cursors, batch()).size(), 5);
    }
}

// Test: several truncations of one segment are matched oldest-first, so a
// segment that was truncated twice drops the right copy each time.
BOOST_AUTO_TEST_CASE(test_replay_drop_stale_copies_multiple_truncations) {
    using db::raft_buffer_detail::drop_stale_copies;
    using db::raft_buffer_detail::segment_cursors;
    using db::raft_buffer_detail::truncation_cursor;

    // The group wrote 4, 5 into this segment, was truncated from 4, wrote 4, 5
    // again, and was truncated from 4 once more. Two records, same range.
    segment_cursors cursors{
        truncation_cursor{.from = raft::index_t(4), .to = raft::index_t(5), .next = raft::index_t(4)},
        truncation_cursor{.from = raft::index_t(4), .to = raft::index_t(5), .next = raft::index_t(4)},
    };

    const auto pair = [] {
        std::vector<raft::log_entry_ptr> v;
        v.push_back(make_command_entry(raft::term_t(1), raft::index_t(4)));
        v.push_back(make_command_entry(raft::term_t(1), raft::index_t(5)));
        return v;
    };

    BOOST_REQUIRE(drop_stale_copies(cursors, pair()).empty());
    BOOST_REQUIRE(cursors.front().exhausted());
    BOOST_REQUIRE(drop_stale_copies(cursors, pair()).empty());
    BOOST_REQUIRE(cursors.back().exhausted());
    // Third copy: no record left, so this one is the current copy and survives.
    auto rest = drop_stale_copies(cursors, pair());
    BOOST_REQUIRE_EQUAL(rest.size(), 2);
    BOOST_REQUIRE_EQUAL(rest[0]->idx, raft::index_t(4));
}

// Test: truncations of one segment that reach back past each other. Several
// cursors can be live at one index, so matching only the oldest would keep a
// truncated copy and replay would apply an entry no leader ever committed.
BOOST_AUTO_TEST_CASE(test_replay_drop_stale_copies_overlapping_truncations) {
    using db::raft_buffer_detail::drop_stale_copies;
    using db::raft_buffer_detail::segment_cursors;
    using db::raft_buffer_detail::truncation_cursor;

    // One segment. A leader wrote 5..9; the next truncated from 7 (clamping the
    // record to 5..6 and recording 7..9) and wrote 7',8' into the same segment;
    // a third truncated from 5, popping the record whole and recording 5..8.
    segment_cursors cursors{
        truncation_cursor{.from = raft::index_t(7), .to = raft::index_t(9), .next = raft::index_t(7)},
        truncation_cursor{.from = raft::index_t(5), .to = raft::index_t(8), .next = raft::index_t(5)},
    };

    const auto batch = [](int from, int to, raft::term_t term) {
        std::vector<raft::log_entry_ptr> v;
        for (int i = from; i <= to; ++i) {
            v.push_back(make_command_entry(term, raft::index_t(i)));
        }
        return v;
    };

    // The first leader's batch is all stale: 5,6 to the second record, 7..9 to the first.
    BOOST_REQUIRE(drop_stale_copies(cursors, batch(5, 9, raft::term_t(1))).empty());
    // The second leader's 7',8' are stale against what is left of the second record.
    BOOST_REQUIRE(drop_stale_copies(cursors, batch(7, 8, raft::term_t(2))).empty());
    // Every cursor is used up, so the third leader's copies stand.
    auto current = drop_stale_copies(cursors, batch(5, 8, raft::term_t(3)));
    BOOST_REQUIRE_EQUAL(current.size(), 4);
    BOOST_REQUIRE_EQUAL(current.front()->idx, raft::index_t(5));
    BOOST_REQUIRE_EQUAL(current.front()->term, raft::term_t(3));
}

// Test: a later write at index N supersedes what is buffered at or above N.
// This is what makes a leader change that reuses indexes come out right without
// comparing terms: the copy written later is by definition the current one.
BOOST_AUTO_TEST_CASE(test_replay_superseded_by) {
    using db::raft_buffer_detail::superseded_by;

    std::deque<db::raft_buffer_detail::buffered_entry> buf;
    for (int i = 3; i <= 7; ++i) {
        buf.push_back(db::raft_buffer_detail::buffered_entry{
                .entry = make_command_entry(raft::term_t(1), raft::index_t(i)), .segment = 1});
    }

    BOOST_REQUIRE_EQUAL(superseded_by(buf, raft::index_t(8)), 0);
    BOOST_REQUIRE_EQUAL(superseded_by(buf, raft::index_t(6)), 2);
    BOOST_REQUIRE_EQUAL(superseded_by(buf, raft::index_t(3)), 5);
    BOOST_REQUIRE_EQUAL(superseded_by(buf, raft::index_t(1)), 5);
    BOOST_REQUIRE_EQUAL(superseded_by({}, raft::index_t(1)), 0);
}

namespace {

// The old segments hold three batches: 1..5 and 6..8 with nothing committed yet,
// then 9..10 whose header says 5 was. So the floor is 5 and the tail is 6..10.
// The floor must arrive only after the tail is buffered: in one batch,
// drain_committed() has nothing buffered to get wrong.
constexpr db::segment_id_type old_segment_id = 1;
constexpr db::segment_id_type rewrite_segment_id = 2;
const auto replay_floor = raft::index_t{5};
const auto replay_tail_end = raft::index_t{10};
const auto replay_term = raft::term_t{1};

// One raft batch as replay hands it over, with the commit index from its header.
struct replayed_batch {
    db::segment_id_type segment;
    raft::index_t commit_idx;
    std::vector<raft::log_entry_ptr> entries;
};

// Dummies: only which indexes come back matters, and a dummy is never applied as
// a mutation. Fresh objects per call, so two calls stand for two copies on disk.
std::vector<raft::log_entry_ptr> index_range(raft::index_t from, raft::index_t to) {
    std::vector<raft::log_entry_ptr> entries;
    for (auto i = from; i <= to; ++i) {
        entries.push_back(make_dummy_entry(replay_term, i));
    }
    return entries;
}

// term:index of every entry, as a string, so a mismatch prints both sides.
sstring log_shape(const raft::log_entries& entries) {
    std::vector<sstring> parts;
    for (const auto& entry : entries) {
        parts.push_back(fmt::format("{}:{}", entry->term, entry->idx));
    }
    return fmt::to_string(fmt::join(parts, ","));
}

cql_test_config sc_replay_config() {
    auto cfg = cql_test_config();
    // system.raft_groups exists only under this flag, and it is also what gives
    // the commitlog the descriptor tag a raft batch is written under.
    cfg.db_config->experimental_features(
            {db::experimental_features_t::feature::STRONGLY_CONSISTENT_TABLES},
            db::config::config_source::CommandLine);
    cfg.db_config->auto_snapshot.set(false);
    cfg.db_config->tablets_mode_for_new_keyspaces.set(db::tablets_mode_t::mode::enabled);
    cfg.initial_tablets = 1;
    return cfg;
}

// Point a group id at an ordinary tablet table: raft info naming the group, on a
// tablet with a replica here, is all resolve_group() asks for. A real strongly
// consistent table would bring a live raft group with it, writing into the same
// commitlog and owning the same system.raft_groups row this test reads back.
// Nothing starts for a fabricated id: only
// storage_service::commit_token_metadata_change reaches groups_manager::update().
void give_table_raft_info(cql_test_env& e, table_id table, raft::group_id gid) {
    locator::shared_token_metadata::mutate_on_all_shards(e.shared_token_metadata(),
            [table, gid] (locator::token_metadata& tm) -> future<> {
        // set_tablet_raft_info() rejects a map built without room for raft info,
        // so the map is replaced rather than patched.
        locator::tablet_map map(dht::get_uniform_tokens(1), /* with_raft_info = */ true);
        const auto tid = *map.tablet_ids().begin();
        map.set_tablet(tid, locator::tablet_info{locator::tablet_replica_set{
                locator::tablet_replica{.host = tm.get_my_id(), .shard = this_shard_id()}}});
        map.set_tablet_raft_info(tid, locator::tablet_raft_info{.group_id = gid});
        tm.tablets().set_tablet_map(table, std::move(map));
        co_return;
    }).get();
}

// A COMMAND entry carrying one real mutation, encoded as the coordinator does: a
// raft_command holding a frozen_mutation in raft::command. Dummies do not work
// for the stale-copy test below: dropped or applied, a dummy leaves no trace.
raft::log_entry_ptr make_mutation_entry(const schema_ptr& s, raft::term_t term, raft::index_t idx,
        int32_t pk, int32_t v, api::timestamp_type ts) {
    mutation m(s, partition_key::from_single_value(*s, int32_type->decompose(pk)));
    const auto ck = clustering_key::make_empty();
    m.set_clustered_cell(ck, to_bytes("v"), data_value(v), ts);
    // A row marker, as an INSERT writes one, so the row exists in its own right.
    m.partition().clustered_row(*s, ck).apply(row_marker(ts));
    raft::command cmd;
    ser::serialize(cmd, service::strong_consistency::raft_command{.mutation = freeze(m)});
    return make_lw_shared<raft::log_entry>(raft::log_entry{
            .term = term, .idx = idx, .data = std::move(cmd)});
}

} // anonymous namespace

// Test: a second replay of the same old segments recovers the same uncommitted
// tail, and does not mistake the floor the first one persisted for covering it.
//
// finish_replay() persists the floor before it rewrites the uncommitted tail, and
// main.cc deletes the old segments later still. In that window the floor is
// durable while the rewritten tail is pinned by nothing a group has claimed.
// Losing it is safe only because the old segments still hold the same entries.
//
// The invariant: the floor is the group's commit index, never the top of the
// tail, and add_batch() discards a copy only at or below the floor. Get either
// wrong and the second pass takes the tail for committed, hands the group a short
// log, and drops entries a leader counted toward a quorum. Rows cannot show that,
// so the assertions are on the recovered log and the floor.
SEASTAR_TEST_CASE(test_second_replay_recovers_the_rewritten_tail) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        // Everything runs on shard 0, which the fabricated replica and the row
        // finish_replay() writes both name, so the shard count does not matter.
        e.execute_cql("create table ks.cf (pk int primary key)").get();
        const auto table = e.local_db().find_schema("ks", "cf")->id();
        const auto gid = make_group_id();
        give_table_raft_info(e, table, gid);

        // One replay pass, returning what the group would have started with. A
        // fresh buffer each time, as a fresh startup has.
        const auto replay = [&] (const std::vector<replayed_batch>& batches) {
            db::raft_commitlog_replay_buffer buffer;
            for (const auto& batch : batches) {
                buffer.add_batch(e.local_db(), e.local_qp(), e.get_system_keyspace().local(),
                        gid, batch.segment, batch.commit_idx, batch.entries).get();
            }
            buffer.finish_replay(e.local_db(), e.local_qp()).get();
            auto data = buffer.take_replayed_group_entries(gid);
            buffer.stop().get();
            return data;
        };
        const auto persisted = [&] {
            return service::strong_consistency::raft_groups_storage::load_descriptor(
                    e.local_qp(), gid, this_shard_id()).get();
        };

        const auto tail = index_range(replay_floor + raft::index_t{1}, replay_tail_end);
        const std::vector<replayed_batch> old_segments = {
            {old_segment_id, raft::index_t{0}, index_range(raft::index_t{1}, replay_floor)},
            {old_segment_id, raft::index_t{0}, index_range(replay_floor + raft::index_t{1},
                    raft::index_t{8})},
            {old_segment_id, replay_floor, index_range(raft::index_t{9}, replay_tail_end)},
        };
        const auto expected_tail = log_shape(raft::log_entries(tail.begin(), tail.end()));

        // First pass: recovers the tail, rewrites it, and persists the floor.
        const auto first = replay(old_segments);
        BOOST_REQUIRE_EQUAL(log_shape(first.entries), expected_tail);
        // Without a rewrite there is nothing for a second pass to lose.
        BOOST_REQUIRE(!first.records.empty());
        BOOST_REQUIRE(persisted().exists);
        BOOST_REQUIRE_EQUAL(persisted().idx, replay_floor);
        BOOST_REQUIRE_EQUAL(persisted().term, replay_term);

        // Second pass with the floor durable and the rewrite gone.
        const auto second = replay(old_segments);
        BOOST_REQUIRE_EQUAL(log_shape(second.entries), expected_tail);
        BOOST_REQUIRE(!second.records.empty());
        BOOST_REQUIRE_EQUAL(persisted().idx, replay_floor);
        BOOST_REQUIRE_EQUAL(persisted().term, replay_term);

        // The same with the rewrite still on disk, which is what a crash in that
        // window leaves: it is force-synced into a segment main.cc never deletes.
        // Two copies of 6..8 are read and the later one wins.
        auto old_segments_and_rewrite = old_segments;
        old_segments_and_rewrite.push_back({rewrite_segment_id, replay_floor,
                index_range(replay_floor + raft::index_t{1}, replay_tail_end)});
        const auto third = replay(old_segments_and_rewrite);
        BOOST_REQUIRE_EQUAL(log_shape(third.entries), expected_tail);
        BOOST_REQUIRE(!third.records.empty());
        BOOST_REQUIRE_EQUAL(persisted().idx, replay_floor);
        BOOST_REQUIRE_EQUAL(persisted().term, replay_term);
    }, sc_replay_config());
}

// Test: a second replay drops the copies the first one superseded, instead of
// applying them as committed.
//
// The failure here is data reaching the tables that no leader committed, which
// leaves the recovered log and the floor exactly as they should be. So this test
// carries commands with real mutations and reads the rows back; a dummy leaves
// nothing behind whether it is dropped or applied.
//
// The setup is one leader change with reused indexes:
//   * the term-1 leader appended 1..8 into its segment and committed none of it;
//   * the term-2 leader reused 6..10 in a segment of its own, then wrote 11 with
//     a header saying 10 was committed.
// So the recovered floor is 10, the current copies of 6..8 are the term-2 ones,
// and the term-1 copies of 6..8 are entries no leader ever committed.
//
// The first pass needs no record: the term-1 copies are still buffered when the
// term-2 batch arrives, so they are popped as superseded before the floor reaches
// them, and it mints the truncation record from what it popped. The second pass
// starts with the floor at 10, so those copies arrive at or below it and the
// persisted record is the only thing that says they are stale.
//
// The timestamps are inverted on purpose. Applying a mutation reconciles per cell
// (compare_atomic_cell_for_merge), so a resurrected copy must be the one that
// wins: measured, with the timestamps equal the term-2 value 206 wins the
// tie-break and this test passes even with the drop deleted.
//
// The ordering is realistic: a new leader seeds last_timestamp from
// table::get_max_timestamp_for_tablet(), which covers applied data only, so an
// entry that was appended and never applied bounds nothing it does.
//
// Checked by deleting, in db/commitlog/raft_commitlog_replay_buffer.cc, the
// drop_stale_copies() call in add_batch() or the cursor seeding in
// resolve_group(): the second pass then returns v=106..108 at pk 6..8. Deleting
// the truncation_record loop in add_batch() fails the first pass's assertion.
SEASTAR_TEST_CASE(test_second_replay_drops_the_copies_the_first_one_superseded) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        // Two tables: `d` carries the mutations and is read back through the
        // ordinary path, `g` only makes the group id resolve and takes the
        // rewritten tablet map, leaving `d`'s metadata as CQL made it.
        e.execute_cql("create table ks.d (pk int primary key, v int)").get();
        e.execute_cql("create table ks.g (pk int primary key)").get();
        const auto s = e.local_db().find_schema("ks", "d");
        const auto gid = make_group_id();
        give_table_raft_info(e, e.local_db().find_schema("ks", "g")->id(), gid);

        // `d`'s tablet must have a replica on this shard: apply_committed() writes
        // to the memtable of the shard replay runs on. It does because `d` is the
        // first tablet table created here, which is why it is created first.
        // Asserted, so reordering the CREATEs fails here, not in table::apply().
        const auto token_metadata = e.local_db().get_shared_token_metadata().get();
        const auto& tablets = token_metadata->tablets().get_tablet_map(s->id());
        BOOST_REQUIRE(tablets.has_replica(*tablets.tablet_ids().begin(),
                locator::tablet_replica{.host = token_metadata->get_my_id(), .shard = this_shard_id()}));

        constexpr db::segment_id_type seg_term1 = 1;
        constexpr db::segment_id_type seg_term2 = 2;
        const auto term1 = raft::term_t{1};
        const auto term2 = raft::term_t{2};
        const auto recovered_floor = raft::index_t{10};
        // Inverted on purpose, see the note above.
        constexpr api::timestamp_type ts_term1 = 2000;
        constexpr api::timestamp_type ts_term2 = 1000;
        // Entry at index i writes pk=i, so a resurrected copy shows as a wrong value.
        constexpr int32_t term1_value_base = 100;
        constexpr int32_t term2_value_base = 200;

        const auto entries = [&] (raft::term_t term, int from, int to) {
            const auto [base, ts] = term == term1
                    ? std::pair(term1_value_base, ts_term1)
                    : std::pair(term2_value_base, ts_term2);
            std::vector<raft::log_entry_ptr> v;
            for (int i = from; i <= to; ++i) {
                v.push_back(make_mutation_entry(s, term, raft::index_t(i), i, base + i, ts));
            }
            return v;
        };

        // Fresh entry objects per call, so two calls stand for two passes.
        const auto old_segments = [&] {
            return std::vector<replayed_batch>{
                {seg_term1, raft::index_t{0}, entries(term1, 1, 8)},
                {seg_term2, raft::index_t{0}, entries(term2, 6, 10)},
                {seg_term2, recovered_floor, entries(term2, 11, 11)},
            };
        };

        const auto replay = [&] (const std::vector<replayed_batch>& batches) {
            db::raft_commitlog_replay_buffer buffer;
            for (const auto& batch : batches) {
                buffer.add_batch(e.local_db(), e.local_qp(), e.get_system_keyspace().local(),
                        gid, batch.segment, batch.commit_idx, batch.entries).get();
            }
            buffer.finish_replay(e.local_db(), e.local_qp()).get();
            auto data = buffer.take_replayed_group_entries(gid);
            buffer.stop().get();
            return data;
        };
        const auto persisted = [&] {
            return service::strong_consistency::raft_groups_storage::load_descriptor(
                    e.local_qp(), gid, this_shard_id()).get();
        };

        // 1..5 stay the term-1 copies, 6..10 the term-2 ones. 11 is uncommitted:
        // with_rows_ignore_order() rejects extra rows, so its absence is checked too.
        std::vector<std::vector<bytes_opt>> committed_rows;
        for (int i = 1; i <= 10; ++i) {
            committed_rows.push_back({int32_type->decompose(i),
                    int32_type->decompose(i <= 5 ? term1_value_base + i : term2_value_base + i)});
        }
        const auto require_committed_rows = [&] {
            assert_that(e.execute_cql("select pk, v from ks.d").get())
                    .is_rows().with_rows_ignore_order(committed_rows);
        };
        // The uncommitted tail, as the group must get it back from either pass.
        const auto expected_tail = sstring("2:11");
        // What the first pass must mint: the term-1 copies of 6..8, in their segment.
        const std::vector<service::strong_consistency::truncation_record> expected_truncations{
                {.segment = seg_term1, .from = raft::index_t{6}, .to = raft::index_t{8}}};

        // First pass. Correct either way; it is here for the record it leaves behind.
        const auto first = replay(old_segments());
        BOOST_REQUIRE_EQUAL(log_shape(first.entries), expected_tail);
        BOOST_REQUIRE_EQUAL(persisted().idx, recovered_floor);
        BOOST_REQUIRE_EQUAL(persisted().term, term2);
        BOOST_REQUIRE(persisted().truncations == expected_truncations);
        require_committed_rows();

        // Second pass, with that floor and record durable. Without the record the
        // term-1 copies of 6..8 apply as committed and their higher timestamp wins.
        const auto second = replay(old_segments());
        BOOST_REQUIRE_EQUAL(log_shape(second.entries), expected_tail);
        BOOST_REQUIRE_EQUAL(persisted().idx, recovered_floor);
        BOOST_REQUIRE_EQUAL(persisted().term, term2);
        // The row still holds the record, so a third pass would drop those copies
        // too. Weak: store_descriptor() returns early at an equal index.
        BOOST_REQUIRE(persisted().truncations == expected_truncations);
        require_committed_rows();
    }, sc_replay_config());
}

BOOST_AUTO_TEST_SUITE_END()
