/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <algorithm>
#include <unordered_set>
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

// A seam into raft_commitlog_replay_buffer's parked records. Reaching them
// through finish_replay() would mean standing up a database, a query processor
// and tablet metadata; what is under test is the shutdown behaviour, not how the
// records got there.
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
    commitlog_raft_batch_writer writer(raft_commitlog_batch{.group_id = gid, .commit_idx = raft::index_t{0}, .entries = {entry}});
    const auto target_size = writer.size();
    co_return co_await cl.add(tid, target_size, db::no_timeout, db::commitlog_force_sync::yes, [entry, gid](auto& out) {
        commitlog_raft_batch_writer w(raft_commitlog_batch{.group_id = gid, .commit_idx = raft::index_t{0}, .entries = {entry}});
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
            commitlog_raft_batch_writer writer(raft_commitlog_batch{.group_id = gid, .commit_idx = raft::index_t{0}, .entries = {entry}});
            // size() must exceed the bare raft::log_entry serialization because
            // the writer wraps it in a commitlog_entry + raft_commitlog_batch envelope.
            BOOST_REQUIRE_GT(writer.size(), 0u);
            BOOST_REQUIRE_GT(writer.size(), ser::get_sizeof(*entry));
            BOOST_REQUIRE_EQUAL(writer.get_batch().group_id, gid);
            BOOST_REQUIRE_EQUAL(writer.get_batch().entries.at(0)->idx, entry->idx);

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

// Test: raft_commitlog store, then truncate_log, verify handles.
// Test: one batch becomes one record, and truncate_log() clamps that record
// while writing a truncation record for what it discarded. The entries stay on
// disk — the commitlog is append-only — so the truncation record is what tells
// replay that those copies were superseded.
SEASTAR_TEST_CASE(test_raft_batch_record_and_truncation) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();
        auto rg_tid = make_table_id();

        raft::log_entry_ptr_list all_entries;
        for (int i = 1; i <= 10; ++i) {
            all_entries.push_back(make_command_entry(raft::term_t(1), raft::index_t(i)));
        }

        std::deque<service::strong_consistency::segment_record> queue;
        // The whole batch is one commitlog entry, so one record.
        auto handle = co_await service::strong_consistency::write_raft_batch(
                log, tid, gid, raft::index_t(0), all_entries);
        service::strong_consistency::account_batch(queue, rg_tid, std::move(handle), all_entries);
        BOOST_REQUIRE_EQUAL(queue.size(), 1);
        BOOST_REQUIRE_EQUAL(queue.front().first, raft::index_t(1));
        BOOST_REQUIRE_EQUAL(queue.front().max, raft::index_t(10));
        BOOST_REQUIRE_EQUAL(queue.front().max_term(), raft::term_t(1));
        // All commands, so the release gate is the last of them.
        BOOST_REQUIRE(queue.front().last_cmd().has_value());
        BOOST_REQUIRE_EQUAL(*queue.front().last_cmd(), raft::index_t(10));
        // Two references at the batch's position: the group's own and the one
        // under system.raft_groups.
        BOOST_REQUIRE(bool(queue.front().pin_table));
        BOOST_REQUIRE(bool(queue.front().pin_rg));
        BOOST_REQUIRE(queue.front().pin_table.rp() == queue.front().pin_rg.rp());

        // A leader change discards 6..10: the record keeps its reference (it
        // still holds live entries 1..5) with max clamped.
        queue.back().trim_from(raft::index_t(6));
        BOOST_REQUIRE_EQUAL(queue.front().max, raft::index_t(5));
        BOOST_REQUIRE_EQUAL(*queue.front().last_cmd(), raft::index_t(5));
        BOOST_REQUIRE(bool(queue.front().pin_table));
    });
}

// Test: the release gate is the record's last *command*. Dummy and
// configuration entries never reach apply(), so a record whose tail is one of
// those must not wait for its own max index, and a record holding nothing but
// those has no gate at all.
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
        std::deque<service::strong_consistency::segment_record> queue;
        service::strong_consistency::account_batch(queue, rg_tid,
                co_await service::strong_consistency::write_raft_batch(
                        log, tid, gid, raft::index_t(0), mixed), mixed);
        BOOST_REQUIRE_EQUAL(queue.size(), 1);
        auto& rec = queue.front();
        BOOST_REQUIRE_EQUAL(rec.max, raft::index_t(4));
        // The gate is command 2, not the dummy at 4.
        BOOST_REQUIRE_EQUAL(*rec.last_cmd(), raft::index_t(2));
        BOOST_REQUIRE_EQUAL(rec.noncmd.size(), 2);
        // The configuration is remembered so that releasing the record can
        // persist it.
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

        // The separation is a property of the on-disk format, so count the
        // alternatives directly rather than driving a replay buffer.
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

        // Verify separation: the raft batches carry only raft entries, for one
        // group, and the mutation entries are untouched by them.
        BOOST_REQUIRE_EQUAL(raft_batch_count, count);
        BOOST_REQUIRE_EQUAL(raft_entry_count, count);
        BOOST_REQUIRE_EQUAL(raft_groups.size(), 1);
        BOOST_REQUIRE_EQUAL(mutation_count, count);
    });
}

// Test: a group that writes into several segments gets one record per segment,
// and a truncation that spans them pops the records it invalidates whole while
// clamping the one it lands inside. Records are the unit of retention, so each
// segment must end up with its own pair of references.
SEASTAR_TEST_CASE(test_raft_batch_records_across_segments) {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(std::move(cfg), [](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();
        auto rg_tid = make_table_id();

        std::deque<service::strong_consistency::segment_record> queue;
        raft::index_t next{1};
        // Keep writing batches until the group has entries in three segments.
        // Each batch is one commitlog entry, so a 1MB segment takes a handful
        // of 4x64KB batches.
        while (queue.size() < 3) {
            raft::log_entry_ptr_list batch;
            for (int i = 0; i < 4; ++i) {
                batch.push_back(make_command_entry_sized(raft::term_t(1), next, 64 * 1024));
                next = next + raft::index_t{1};
            }
            service::strong_consistency::account_batch(queue, rg_tid,
                co_await service::strong_consistency::write_raft_batch(
                        log, tid, gid, raft::index_t(0), batch), batch);
        }

        // One record per segment, in segment order, with disjoint ascending
        // index ranges and a reference pair each.
        for (size_t i = 0; i + 1 < queue.size(); ++i) {
            BOOST_REQUIRE_LT(queue[i].segment(), queue[i + 1].segment());
            BOOST_REQUIRE_LT(queue[i].max, queue[i + 1].first);
            BOOST_REQUIRE(bool(queue[i].pin_table));
            BOOST_REQUIRE(bool(queue[i].pin_rg));
        }

        // Truncate inside the middle record: the newest records are entirely at
        // or above the truncation point and go away whole; the one the point
        // lands in is clamped and keeps its references, because it still holds
        // live entries.
        const auto cut = queue[1].first + raft::index_t{1};
        std::deque<service::strong_consistency::truncation_record> truncations;
        while (!queue.empty() && queue.back().first >= cut) {
            truncations.push_back(service::strong_consistency::truncation_record{
                    .segment = queue.back().segment(), .from = queue.back().first, .to = queue.back().max});
            queue.pop_back();
        }
        BOOST_REQUIRE(!queue.empty());
        if (queue.back().max >= cut) {
            truncations.push_back(service::strong_consistency::truncation_record{
                    .segment = queue.back().segment(), .from = cut, .to = queue.back().max});
            queue.back().trim_from(cut);
        }
        BOOST_REQUIRE(!truncations.empty());
        BOOST_REQUIRE_LT(queue.back().max, cut);
        BOOST_REQUIRE(bool(queue.back().pin_table));
        // Every truncation names a real segment and a non-empty range.
        for (const auto& t : truncations) {
            BOOST_REQUIRE_GT(t.segment, 0u);
            BOOST_REQUIRE_LE(t.from, t.to);
        }
    });
}

// Test: max_single_entry_batch_size() bounds what write_raft_batch() actually
// measures for a single entry, and bounds it tightly. The startup check it
// feeds is only worth anything if the number tracks the format, so this pins
// the two together — a bound for any entry, exact for the lease-stamped one it
// is derived from. Without the exact half, a bound of SIZE_MAX would pass.
SEASTAR_TEST_CASE(test_max_single_entry_batch_size_bounds_the_writer) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();

        for (const size_t payload : {size_t(0), size_t(4096), size_t(100 * 1024)}) {
            auto plain = make_command_entry_sized(raft::term_t(1), raft::index_t(1), payload);
            const auto command_size = std::get<raft::command>(plain->data).size();
            const auto bound = service::strong_consistency::max_single_entry_batch_size(command_size);

            commitlog_raft_batch_writer plain_writer(raft_commitlog_batch{
                    .group_id = gid, .commit_idx = raft::index_t{0}, .entries = {plain}});
            BOOST_REQUIRE_LE(plain_writer.size(), bound);

            // The lease-stamped form is the larger of the two and is what the
            // bound is measured from, so here it must be exact.
            auto stamped = make_lw_shared<const raft::log_entry>(raft::log_entry{
                    .term = raft::term_t(1), .idx = raft::index_t(1),
                    .data = std::get<raft::command>(plain->data),
                    .lease_time = raft::time_bounds{
                            raft::lease_clock::time_point(std::chrono::nanoseconds(lease_earliest_ns)),
                            raft::lease_clock::time_point(std::chrono::nanoseconds(lease_latest_ns))}});
            commitlog_raft_batch_writer stamped_writer(raft_commitlog_batch{
                    .group_id = gid, .commit_idx = raft::index_t{0}, .entries = {stamped}});
            BOOST_REQUIRE_EQUAL(stamped_writer.size(), bound);
        }
        co_return;
    });
}

// Test: a batch that does not fit in one commitlog entry is an internal error,
// not something to split or fragment.
//
// Fragmenting it across segments — which is what the commitlog would do with an
// oversized entry — breaks the rule that a copy of an entry lives in exactly one
// segment, and that rule is what the records and the truncation records are
// built on.
//
// write_raft_batch() owns the rule and the reachability argument.
//
// allow_fragmented_entries is on, as in production once the cluster feature is
// up, and that is the configuration the check exists for: with it off,
// commitlog::add() rejects the oversized entry by itself; with it on, removing
// the check raises nothing — the commitlog quietly fragments the entry, the one
// outcome the design forbids. Hence the flag, and an assertion on our own error
// rather than on any exception.
SEASTAR_TEST_CASE(test_raft_batch_too_large_is_an_internal_error) {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    cfg.allow_fragmented_entries = true;
    return cl_test(std::move(cfg), [](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();

        // Half a segment is the largest a single commitlog entry can be, so
        // 16 x 64KB is comfortably over it.
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
            // on_internal_error's own exception, not the commitlog's
            // invalid_argument: that one would not be raised at all here.
            BOOST_REQUIRE(sstring(e.what()).find("does not fit in one commitlog entry")
                    != sstring::npos);
        }
    });
}

// Test: records that no group claimed are detached at stop(), not released —
// raft_commitlog_replay_buffer::stop() says why. Direction matters: the
// segments must stay dirty here, while a deliberately destroyed group gives
// its up (test_raft_commitlog_release_all_frees_the_segments below).
SEASTAR_TEST_CASE(test_replay_buffer_stop_detaches_unclaimed_records) {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(std::move(cfg), [](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();
        auto rg_tid = make_table_id();

        // Build the records exactly as finish_replay() does: write the tail as a
        // batch and account it. Fill past one segment, since only sealed ones
        // count as dirty.
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
        // Asserted after the buffer is gone: a stop() that merely cleared the map
        // would leave the destructor to decrement, and the segments would go
        // clean here.
        BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), dirty);
    });
}

// Test: the same holds when stop() never runs at all.
//
// The buffer's implicit destructor would decrement, so skipping stop() — an
// exception between start() and its shutdown hook — would delete the segments
// holding a rewritten tail while the process is still live. This is the
// structural half of the rule: stop() being correct is not enough if the
// destructor beside it does the opposite.
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

            // Fill past one segment, so the group holds a sealed one: only those
            // count as dirty, and only they can be reclaimed.
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
            // ...and the segments it was keeping dirty are clean, which is
            // exactly what detaching them would not have done.
            BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), 0);
        }
    });
}

// Test: the other direction — destroying a group without release_all() detaches
// its references, so the segments stay dirty. This is the shutdown path
// (log_disposition::keep). The pair has to be tested together: either test
// alone passes with the destructor and release_all() doing the same thing.
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
        // Asserted after the group is gone, so the destructor has run: a
        // destructor that dropped the handles rather than detaching them would
        // have brought this to zero.
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

// Test: rp_handle::clone() takes an extra reference at a live handle's
// position, under the same or a different column family, any number of times.
//
// Asserted through the segment rather than through the handles: a clone that
// took no reference would satisfy every rp() and bool() check here, and
// mark_clean() no-ops silently once a cf's count is gone, so an under-counted
// release raises nothing either. Only the segment's dirty state tells them
// apart.
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

        // A reference taken from a reference is just as good a source as the original.
        std::optional chained = pin1->clone(other_tid);
        BOOST_REQUIRE(chained->rp() == handle->rp());

        // Seal the segment the references are in: only a sealed segment reports
        // as dirty, and only then does the count become observable.
        co_await log.force_new_active_segment();
        BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), 1);

        // Now drop them one at a time. Every step but the last must leave the
        // segment dirty — which is what fails if clone() did not count.
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

// Test: the copies a truncation superseded are dropped, and only those.
//
// A truncation removes a suffix of the raft log, so the copies it discarded are
// the batch's tail and the survivors its prefix. The record says which indexes
// of which segment went, and the cursor walks it as the copies are read.
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

    // First copy of 4,5: consumed by the first record.
    BOOST_REQUIRE(drop_stale_copies(cursors, pair()).empty());
    BOOST_REQUIRE(cursors.front().exhausted());
    // Second copy: consumed by the second record.
    BOOST_REQUIRE(drop_stale_copies(cursors, pair()).empty());
    BOOST_REQUIRE(cursors.back().exhausted());
    // Third copy: no record left, so this one is the current copy and survives.
    auto rest = drop_stale_copies(cursors, pair());
    BOOST_REQUIRE_EQUAL(rest.size(), 2);
    BOOST_REQUIRE_EQUAL(rest[0]->idx, raft::index_t(4));
}

// Test: truncations of one segment that reach back past each other. A
// truncation that clamps a record leaves a cursor for the tail it discarded, and
// a later truncation that pops the same record covers a range starting *below*
// that — so at a given index several cursors can be live with only a later one
// waiting for it. Matching against the oldest live cursor alone would keep a
// copy that was truncated, and replay would then apply an entry no leader ever
// committed.
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

    // The first leader's whole batch is stale: 5 and 6 belong to the second
    // record, 7..9 to the first.
    BOOST_REQUIRE(drop_stale_copies(cursors, batch(5, 9, raft::term_t(1))).empty());
    // The second leader's 7',8' are stale too, against what is left of the
    // second record.
    BOOST_REQUIRE(drop_stale_copies(cursors, batch(7, 8, raft::term_t(2))).empty());
    // Every cursor is now used up, so the third leader's copies stand — these are
    // the ones that actually committed.
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

    // A batch starting above the buffer supersedes nothing.
    BOOST_REQUIRE_EQUAL(superseded_by(buf, raft::index_t(8)), 0);
    // ...starting inside it, exactly the tail from there.
    BOOST_REQUIRE_EQUAL(superseded_by(buf, raft::index_t(6)), 2);
    BOOST_REQUIRE_EQUAL(superseded_by(buf, raft::index_t(3)), 5);
    // ...starting below it, all of it.
    BOOST_REQUIRE_EQUAL(superseded_by(buf, raft::index_t(1)), 5);
    // An empty buffer has nothing to supersede.
    BOOST_REQUIRE_EQUAL(superseded_by({}, raft::index_t(1)), 0);
}

BOOST_AUTO_TEST_SUITE_END()
