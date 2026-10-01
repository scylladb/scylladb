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
#include "service/strong_consistency/groups_manager.hh"
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
// needs a database, a query processor and tablet metadata.
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

// Every raft batch a group left on disk, ordered by first index.
future<std::vector<raft_commitlog_batch>> read_raft_batches(commitlog& log, raft::group_id gid) {
    co_await log.sync_all_segments();
    std::vector<raft_commitlog_batch> batches;
    for (const auto& name : log.get_active_segment_names()) {
        co_await commitlog::read_log_file(name, commitlog::descriptor::FILENAME_PREFIX,
                [&](commitlog::buffer_and_replay_position buf_rp) -> future<> {
            commitlog_entry_reader reader(buf_rp.buffer,
                    detail::commitlog_entry_serialization_format::variant);
            auto& item = reader.entry().item;
            if (std::holds_alternative<raft_commitlog_batch>(item)) {
                const auto& read = std::get<raft_commitlog_batch>(item);
                if (read.group_id == gid) {
                    batches.push_back(read);
                }
            }
            co_return;
        });
    }
    std::ranges::sort(batches, [](const raft_commitlog_batch& left, const raft_commitlog_batch& right) {
        return left.entries.front()->idx < right.entries.front()->idx;
    });
    co_return batches;
}

// Each batch links to the entry below its first index: the first to `floor_term`, every
// later one to the last entry of the batch before it.
void require_batches_chain(const std::vector<raft_commitlog_batch>& batches, raft::term_t floor_term) {
    BOOST_REQUIRE_GT(batches.size(), 1u);
    BOOST_REQUIRE_EQUAL(batches.front().prev_term, floor_term);
    for (size_t i = 1; i < batches.size(); ++i) {
        BOOST_REQUIRE_EQUAL(batches[i].entries.front()->idx,
                batches[i - 1].entries.back()->idx + raft::index_t{1});
        BOOST_REQUIRE_EQUAL(batches[i].prev_term, batches[i - 1].entries.back()->term);
    }
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
    commitlog_raft_batch_writer writer(gid, raft::index_t{0}, raft::term_t{0}, entries);
    const auto target_size = writer.size();
    co_return co_await cl.add(tid, target_size, db::no_timeout, db::commitlog_force_sync::yes, [entries, gid](auto& out) {
        commitlog_raft_batch_writer w(gid, raft::index_t{0}, raft::term_t{0}, entries);
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
            commitlog_raft_batch_writer writer(gid, raft::index_t{0}, raft::term_t{0}, batch);
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
// fault in that loop, such as a dropped entry, a miscounted length or entries
// reordered, only shows up with more than one entry in a batch, which every
// other writer test here has exactly one of.
SEASTAR_TEST_CASE(test_commitlog_raft_batch_writer_multiple_entries) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();
        const raft::index_t commit_idx{7};
        // Distinct from commit_idx and from every index and term in the batch, so a
        // field written or read in the wrong place shows up as a mismatch.
        const raft::term_t prev_term{6};

        const std::vector<raft::log_entry_ptr> batch = {
                make_command_entry_sized(raft::term_t(1), raft::index_t(11), 128),
                make_config_entry(raft::term_t(2), raft::index_t(12)),
                make_dummy_entry(raft::term_t(3), raft::index_t(13)),
                make_lease_entry(raft::term_t(4), raft::index_t(14)),
                make_command_entry_sized(raft::term_t(5), raft::index_t(15), 256),
        };

        commitlog_raft_batch_writer writer(gid, commit_idx, prev_term, batch);
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
                BOOST_REQUIRE_EQUAL(read.prev_term, prev_term);
                BOOST_REQUIRE_EQUAL(read.entries.size(), batch.size());
                for (size_t i = 0; i < batch.size(); ++i) {
                    BOOST_REQUIRE_EQUAL(read.entries[i]->idx, batch[i]->idx);
                    BOOST_REQUIRE_EQUAL(read.entries[i]->term, batch[i]->term);
                    BOOST_REQUIRE_EQUAL(read.entries[i]->data.index(), batch[i]->data.index());
                    // Compare the payloads. The persisted encoding differs from
                    // ser::serialize, so corrupted command bytes pass every other
                    // check in this test.
                    if (std::holds_alternative<raft::command>(batch[i]->data)) {
                        BOOST_REQUIRE(std::get<raft::command>(read.entries[i]->data)
                                == std::get<raft::command>(batch[i]->data));
                    }
                    if (std::holds_alternative<raft::configuration>(batch[i]->data)) {
                        BOOST_REQUIRE(std::get<raft::configuration>(read.entries[i]->data).current
                                == std::get<raft::configuration>(batch[i]->data).current);
                    }
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

// Installs a tablet map for `table` with a single tablet, one raft group and the given
// replica set, so that replay has tablet metadata to decide ownership from.
future<> set_sc_tablet_metadata(cql_test_env& env, table_id table, raft::group_id gid,
        locator::tablet_replica_set replicas) {
    co_await locator::shared_token_metadata::mutate_on_all_shards(env.shared_token_metadata(),
            [table, gid, replicas = std::move(replicas)] (locator::token_metadata& tm) -> future<> {
        locator::tablet_map tmap(1, true /* with_raft_info */);
        const auto tid = *tmap.tablet_ids().begin();
        tmap.set_tablet(tid, locator::tablet_info{replicas});
        tmap.set_tablet_raft_info(tid, locator::tablet_raft_info{gid});
        locator::tablet_metadata tmeta = co_await tm.tablets().copy();
        tmeta.set_tablet_map(table, std::move(tmap));
        tm.set_tablets(std::move(tmeta));
    });
}

// Drops the map set_sc_tablet_metadata() installed for `table`. The tablet load balancer
// raises an internal error on a replica whose host is not in topology, so a map with
// such a replica is dropped as soon as the step that needs it is done.
future<> drop_sc_tablet_metadata(cql_test_env& env, table_id table) {
    co_await locator::shared_token_metadata::mutate_on_all_shards(env.shared_token_metadata(),
            [table] (locator::token_metadata& tm) -> future<> {
        locator::tablet_metadata tmeta = co_await tm.tablets().copy();
        tmeta.drop_tablet_map(table);
        tm.set_tablets(std::move(tmeta));
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
                log, tid, gid, raft::index_t(0), raft::term_t(0), all_entries);
        service::strong_consistency::account_batch(segment_queue, rg_tid, std::move(handle), all_entries);
        BOOST_REQUIRE_EQUAL(segment_queue.size(), 1);
        BOOST_REQUIRE_EQUAL(segment_queue.front().first_index, raft::index_t(1));
        BOOST_REQUIRE_EQUAL(segment_queue.front().max_index, raft::index_t(10));
        BOOST_REQUIRE_EQUAL(segment_queue.front().max_term(), raft::term_t(1));
        BOOST_REQUIRE(segment_queue.front().last_cmd().has_value());
        BOOST_REQUIRE_EQUAL(*segment_queue.front().last_cmd(), raft::index_t(10));
        // Two references to the batch's segment: the group's own at the batch's
        // position and a clone under system.raft_groups.
        BOOST_REQUIRE(bool(segment_queue.front().pin_user_table));
        BOOST_REQUIRE(bool(segment_queue.front().pin_raft_groups));
        BOOST_REQUIRE(segment_queue.front().pin_raft_groups.rp()
                == db::replay_position(segment_queue.front().pin_user_table.rp().id, 0));

        // A leader change discards 6..10: max is clamped, the reference stays for 1..5.
        segment_queue.back().trim_from(raft::index_t(6));
        BOOST_REQUIRE_EQUAL(segment_queue.front().max_index, raft::index_t(5));
        BOOST_REQUIRE_EQUAL(*segment_queue.front().last_cmd(), raft::index_t(5));
        BOOST_REQUIRE(bool(segment_queue.front().pin_user_table));
    });
}

// Test: a truncation trims `max`, the term runs, the configurations and the
// non-command indexes. Entries a leader change discarded leave no trace in the
// record.
SEASTAR_TEST_CASE(test_trim_from_drops_terms_configs_and_noncmd_indexes) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();
        auto rg_tid = make_table_id();

        // Three term runs, two configurations, four non-commands.
        raft::log_entry_ptr_list entries = {
            make_command_entry(raft::term_t(1), raft::index_t(1)),
            make_config_entry(raft::term_t(1), raft::index_t(2)),
            make_command_entry(raft::term_t(2), raft::index_t(3)),
            make_dummy_entry(raft::term_t(2), raft::index_t(4)),
            make_command_entry(raft::term_t(3), raft::index_t(5)),
            make_config_entry(raft::term_t(3), raft::index_t(6)),
            make_command_entry(raft::term_t(3), raft::index_t(7)),
            make_dummy_entry(raft::term_t(3), raft::index_t(8)),
        };
        std::deque<service::strong_consistency::segment_record> segment_queue;
        service::strong_consistency::account_batch(segment_queue, rg_tid,
                co_await service::strong_consistency::write_raft_batch(
                        log, tid, gid, raft::index_t(0), raft::term_t(0), entries), entries);
        BOOST_REQUIRE_EQUAL(segment_queue.size(), 1);
        auto& record = segment_queue.front();
        BOOST_REQUIRE_EQUAL(record.max_index, raft::index_t(8));
        BOOST_REQUIRE_EQUAL(record.terms.size(), 3);
        BOOST_REQUIRE_EQUAL(record.configs.size(), 2);
        BOOST_REQUIRE_EQUAL(record.noncmd_indexes.size(), 4);

        // 6..8 go: the run that starts at 5 stays, the configuration at 6 goes.
        record.trim_from(raft::index_t(6));
        BOOST_REQUIRE_EQUAL(record.max_index, raft::index_t(5));
        BOOST_REQUIRE_EQUAL(record.terms.size(), 3);
        BOOST_REQUIRE_EQUAL(record.max_term(), raft::term_t(3));
        BOOST_REQUIRE_EQUAL(record.configs.size(), 1);
        BOOST_REQUIRE_EQUAL(record.last_conf()->first, raft::index_t(2));
        BOOST_REQUIRE_EQUAL(record.noncmd_indexes.size(), 2);
        BOOST_REQUIRE_EQUAL(*record.last_cmd(), raft::index_t(5));

        // 3..5 go: two term runs with them, so the reported term falls back to 1.
        record.trim_from(raft::index_t(3));
        BOOST_REQUIRE_EQUAL(record.max_index, raft::index_t(2));
        BOOST_REQUIRE_EQUAL(record.terms.size(), 1);
        BOOST_REQUIRE_EQUAL(record.max_term(), raft::term_t(1));
        BOOST_REQUIRE_EQUAL(record.configs.size(), 1);
        BOOST_REQUIRE_EQUAL(record.noncmd_indexes.size(), 1);
        BOOST_REQUIRE_EQUAL(*record.last_cmd(), raft::index_t(1));

        // Only index 1 is left: no configuration, no non-command, and the first
        // term run survives because a record always reports some term.
        record.trim_from(raft::index_t(2));
        BOOST_REQUIRE_EQUAL(record.max_index, raft::index_t(1));
        BOOST_REQUIRE_EQUAL(record.terms.size(), 1);
        BOOST_REQUIRE_EQUAL(record.max_term(), raft::term_t(1));
        BOOST_REQUIRE(record.configs.empty());
        BOOST_REQUIRE(record.noncmd_indexes.empty());
        BOOST_REQUIRE(!record.last_conf().has_value());
        BOOST_REQUIRE_EQUAL(*record.last_cmd(), raft::index_t(1));
    });
}

// Test: prev_term_for() names the term of the entry below `first` as the log
// now ends, across a truncation that clamps a record and a truncation that drops it.
// SCYLLADB-4893 covers a batch linking to the term a truncation discarded: replay
// would accept a superseded copy in place of the missing entry.
SEASTAR_TEST_CASE(test_prev_term_follows_the_log_through_a_truncation) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();
        auto rg_tid = make_table_id();

        service::strong_consistency::raft_commitlog rc(gid, log, tid, rg_tid, {});
        // The group starts above a persisted descriptor, as it does after a restart.
        rc.seed_floor(raft::index_t(3), raft::term_t(1));
        BOOST_REQUIRE_EQUAL(rc.prev_term_for(raft::index_t(4)), raft::term_t(1));

        // One record over two terms: 4..5 in term 2, 6 in term 4.
        co_await rc.store_log_entries({
                make_command_entry(raft::term_t(2), raft::index_t(4)),
                make_command_entry(raft::term_t(2), raft::index_t(5)),
                make_command_entry(raft::term_t(4), raft::index_t(6)),
        }, raft::index_t(0));
        BOOST_REQUIRE_EQUAL(rc.prev_term_for(raft::index_t(7)), raft::term_t(4));

        // A new leader discards index 6. The record keeps 4..5, so the replacement
        // at 6 links to term 2.
        rc.truncate_log(raft::index_t(6));
        BOOST_REQUIRE_EQUAL(rc.prev_term_for(raft::index_t(6)), raft::term_t(2));

        co_await rc.store_log_entries({
                make_command_entry(raft::term_t(5), raft::index_t(6)),
        }, raft::index_t(0));
        BOOST_REQUIRE_EQUAL(rc.prev_term_for(raft::index_t(7)), raft::term_t(5));

        // Read the link back from disk, as the write path stored it.
        co_await log.sync_all_segments();
        bool seen = false;
        for (const auto& name : log.get_active_segment_names()) {
            co_await commitlog::read_log_file(name, commitlog::descriptor::FILENAME_PREFIX,
                    [&](commitlog::buffer_and_replay_position buf_rp) -> future<> {
                commitlog_entry_reader reader(buf_rp.buffer,
                        detail::commitlog_entry_serialization_format::variant);
                auto& item = reader.entry().item;
                if (!std::holds_alternative<raft_commitlog_batch>(item)) {
                    co_return;
                }
                auto& read = std::get<raft_commitlog_batch>(item);
                if (read.group_id != gid || read.entries.front()->idx != raft::index_t(6)
                        || read.entries.front()->term != raft::term_t(5)) {
                    co_return;
                }
                BOOST_REQUIRE_EQUAL(read.prev_term, raft::term_t(2));
                seen = true;
                co_return;
            });
        }
        BOOST_REQUIRE(seen);

        // Truncating the whole log back to the floor empties the record queue, and
        // the floor answers again.
        rc.truncate_log(raft::index_t(4));
        BOOST_REQUIRE_EQUAL(rc.prev_term_for(raft::index_t(4)), raft::term_t(1));

        // A batch that follows neither the newest record nor the floor is a hole in
        // the log, which raft cannot produce and the writer refuses to guess a link for.
        {
            seastar::testing::scoped_no_abort_on_internal_error no_abort;
            try {
                rc.prev_term_for(raft::index_t(9));
                BOOST_FAIL("expected a batch over a hole to be rejected");
            } catch (const std::runtime_error& e) {
                BOOST_REQUIRE(sstring(e.what()).find("the log is not contiguous") != sstring::npos);
            }
        }
    });
}

// Test: the release gate is the record's last *command*. Dummy and configuration
// entries never reach apply(), so a gate on them holds the record forever.
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
                        log, tid, gid, raft::index_t(0), raft::term_t(0), mixed), mixed);
        BOOST_REQUIRE_EQUAL(segment_queue.size(), 1);
        auto& record = segment_queue.front();
        BOOST_REQUIRE_EQUAL(record.max_index, raft::index_t(4));
        // The gate is command 2, not the dummy at 4.
        BOOST_REQUIRE_EQUAL(*record.last_cmd(), raft::index_t(2));
        BOOST_REQUIRE_EQUAL(record.noncmd_indexes.size(), 2);
        // The configuration is remembered so releasing the record can persist it.
        BOOST_REQUIRE(record.last_conf().has_value());
        BOOST_REQUIRE_EQUAL(record.last_conf()->first, raft::index_t(3));

        // A record of non-commands only has no gate: nothing will ever apply.
        raft::log_entry_ptr_list only_noncmd = {
            make_dummy_entry(raft::term_t(2), raft::index_t(5)),
            make_config_entry(raft::term_t(2), raft::index_t(6)),
        };
        std::deque<service::strong_consistency::segment_record> queue2;
        service::strong_consistency::account_batch(queue2, rg_tid,
                co_await service::strong_consistency::write_raft_batch(
                        log, tid, gid, raft::index_t(4), raft::term_t(0), only_noncmd), only_noncmd);
        BOOST_REQUIRE_EQUAL(queue2.size(), 1);
        BOOST_REQUIRE(!queue2.front().last_cmd().has_value());
        BOOST_REQUIRE_EQUAL(queue2.front().max_term(), raft::term_t(2));
    });
}

// Test: Replay across multiple commitlog segments. Write enough entries to fill
// several segments, then read all active segments back and verify order is
// preserved.
SEASTAR_TEST_CASE(test_replay_with_multiple_segments) {
    // 1MB segments and 64KB entries, so the log spans several segments.
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(std::move(cfg), [](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();

        constexpr int num_entries = 50;

        // Hold the references. A segment with no references is recycled once
        // it is sealed.
        std::vector<rp_handle> handles;
        for (int i = 1; i <= num_entries; ++i) {
            auto entry = make_command_entry_sized(raft::term_t(1), raft::index_t(i), 64 * 1024);
            handles.push_back(co_await write_raft_entry_to_commitlog(log, tid, gid, entry));
        }

        co_await log.sync_all_segments();

        auto segments = log.get_active_segment_names();
        BOOST_REQUIRE_GT(segments.size(), 1u);
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
                        log, tid, gid, raft::index_t(0), raft::term_t(0), batch), batch);
        }

        for (size_t i = 0; i + 1 < segment_queue.size(); ++i) {
            BOOST_REQUIRE_LT(segment_queue[i].segment(), segment_queue[i + 1].segment());
            BOOST_REQUIRE_LT(segment_queue[i].max_index, segment_queue[i + 1].first_index);
            BOOST_REQUIRE(bool(segment_queue[i].pin_user_table));
            BOOST_REQUIRE(bool(segment_queue[i].pin_raft_groups));
        }

        // Truncate inside the middle record: records at or above the cut go away
        // whole, the one it lands in is clamped and keeps its references.
        const auto cut = segment_queue[1].first_index + raft::index_t{1};
        std::deque<service::strong_consistency::truncation_record> truncations;
        while (!segment_queue.empty() && segment_queue.back().first_index >= cut) {
            truncations.push_back(service::strong_consistency::truncation_record{
                    .segment = segment_queue.back().segment(), .from = segment_queue.back().first_index, .to = segment_queue.back().max_index});
            segment_queue.pop_back();
        }
        BOOST_REQUIRE(!segment_queue.empty());
        if (segment_queue.back().max_index >= cut) {
            truncations.push_back(service::strong_consistency::truncation_record{
                    .segment = segment_queue.back().segment(), .from = cut, .to = segment_queue.back().max_index});
            segment_queue.back().trim_from(cut);
        }
        BOOST_REQUIRE(!truncations.empty());
        BOOST_REQUIRE_LT(segment_queue.back().max_index, cut);
        BOOST_REQUIRE(bool(segment_queue.back().pin_user_table));
    });
}

// Test: the boot check accepts the smallest segment the commitlog allows.
// commitlog_segment_size_in_mb is clamped to 1 and max_record_size() is half a
// segment, so raft_max_command_size has 512KB of room on any configuration a node
// can be given.
SEASTAR_TEST_CASE(test_boot_check_accepts_the_smallest_segment) {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(std::move(cfg), [](commitlog& log) -> future<> {
        BOOST_REQUIRE_NO_THROW(
                service::strong_consistency::check_commitlog_can_hold_a_raft_entry(log));
        co_return;
    });
}

// Test: the boot check refuses a command the commitlog cannot hold, and the message
// carries the four numbers an operator needs. raft_max_command_size leaves 512KB of
// room on every real configuration, so the command size is a parameter to reach the
// refusal at all.
SEASTAR_TEST_CASE(test_boot_check_refuses_a_command_the_commitlog_cannot_hold) {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(std::move(cfg), [](commitlog& log) -> future<> {
        // One byte over what a commitlog entry holds, so the envelope pushes it past.
        const auto too_large = log.max_record_size();
        BOOST_REQUIRE_EXCEPTION(
                service::strong_consistency::check_commitlog_can_hold_a_raft_entry(log, too_large),
                std::runtime_error,
                [&](const std::runtime_error& e) {
                    const sstring what(e.what());
                    return what.find("commitlog_segment_size_in_mb=1") != sstring::npos
                            && what.find(format("({} bytes)", too_large)) != sstring::npos
                            && what.find(format("at most {}", log.max_record_size())) != sstring::npos
                            && what.find("Raise the segment size") != sstring::npos;
                });
        co_return;
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
            commitlog_raft_batch_writer plain_writer(gid, raft::index_t{0}, raft::term_t{0}, plain_batch);
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
            commitlog_raft_batch_writer stamped_writer(gid, raft::index_t{0}, raft::term_t{0}, stamped_batch);
            BOOST_REQUIRE_EQUAL(stamped_writer.size(), bound);
        }
        co_return;
    });
}

// Test: a tail larger than one commitlog entry is split into runs that each fit, so a
// recovered log that no single entry can hold is still rewritten. The tail is
// bounded by raft_max_log_size, so batches that each fitted when they were
// written can add up to a tail that does not, and an oversized rewrite aborts
// this replay and every later replay.
// Test: a batch too large for one commitlog entry is split, and every batch written
// links to the one below it.
//
// store_log_entries() re-reads prev_term_for() per batch, so a split run chains the same
// way an unsplit one does. A wrong link on any batch but the first refuses the next
// replay, and only a tail spanning two terms shows it.
SEASTAR_TEST_CASE(test_split_store_log_entries_chains_every_batch) {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(std::move(cfg), [](commitlog& log) -> future<> {
        const auto gid = make_group_id();
        const auto tid = make_table_id();
        const auto rg_tid = make_table_id();
        service::strong_consistency::raft_commitlog rc(gid, log, tid, rg_tid, {});
        // The group starts above a persisted descriptor, as it does after a restart.
        rc.seed_floor(raft::index_t(0), raft::term_t(1));

        // 16 x 64KB over two terms, more than one commitlog entry holds.
        raft::log_entry_ptr_list tail;
        for (int i = 1; i <= 16; ++i) {
            tail.push_back(make_command_entry_sized(
                    raft::term_t(i <= 8 ? 1 : 2), raft::index_t(i), 64 * 1024));
        }
        co_await rc.store_log_entries(tail, raft::index_t(0));

        const auto batches = co_await read_raft_batches(log, gid);
        require_batches_chain(batches, raft::term_t(1));
        BOOST_REQUIRE_EQUAL(batches.back().entries.back()->idx, raft::index_t(16));
    });
}

SEASTAR_TEST_CASE(test_split_raft_batch_keeps_every_batch_writable) {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(std::move(cfg), [](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();

        // 16 x 64KB is over the half-segment cap, so one batch cannot hold it.
        raft::log_entry_ptr_list tail;
        for (int i = 1; i <= 16; ++i) {
            tail.push_back(make_command_entry_sized(raft::term_t(1), raft::index_t(i), 64 * 1024));
        }
        BOOST_REQUIRE_GT(tail.size() * 64 * 1024, log.max_record_size());

        const auto batch_ends = service::strong_consistency::split_raft_batch(
                log, gid, raft::index_t(0), raft::term_t(0), tail);
        BOOST_REQUIRE_GT(batch_ends.size(), 1u);
        BOOST_REQUIRE_EQUAL(batch_ends.back(), tail.size());

        // Every batch is writable, covers its share in order, and none is empty.
        size_t batch_begin = 0;
        for (const auto batch_end : batch_ends) {
            BOOST_REQUIRE_LT(batch_begin, batch_end);
            const raft::log_entry_ptr_list batch(tail.begin() + batch_begin, tail.begin() + batch_end);
            auto handle = co_await service::strong_consistency::write_raft_batch(
                    log, tid, gid, raft::index_t(0), raft::term_t(0), batch);
            BOOST_REQUIRE(bool(handle));
            batch_begin = batch_end;
        }
        BOOST_REQUIRE_EQUAL(batch_begin, tail.size());
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
                    log, tid, gid, raft::index_t(0), raft::term_t(0), big);
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
                    log, tid, gid, raft::index_t(0), raft::term_t(0), batch);
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
        // leaves the destructor to decrement, and the segment goes clean.
        BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), dirty);
    });
}

// Test: the records are also detached when stop() never runs. No production path
// reaches the destructor with records (see its comment), but a destructor that
// decremented the pins retires the segments holding a rewritten tail.
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
                    log, tid, gid, raft::index_t(0), raft::term_t(0), batch);
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
// of detaching them: see raft_commitlog::release_all() (SCYLLADB-3827).
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
            // ...and the segments it kept dirty are clean; detaching leaves them dirty.
            BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), 0);
        }
    });
}

// Test: destroying a group without release_all() detaches its references, so the
// segments stay dirty (the shutdown path, log_disposition::keep). Pairs with the
// test above: each test alone passes if both paths behave the same.
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
        // handles instead of detaching them brings the count to zero.
        BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), dirty);
    });
}

// The other encoding of lease_time: the plain ser::serialize/ser::deserialize
// pair, which is what idl/raft.idl.hh uses for append_request::entries. That is
// the replication path, and for LeaseGuard it is the primary one: "the log is
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

// Test: rp_handle::clone() takes an extra reference on a live handle's segment,
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

        // A clone is at the segment start, which no entry occupies.
        const db::replay_position segment_start(handle->rp().id, 0);
        BOOST_REQUIRE(handle->rp() != segment_start);

        // A second reference under the entry's own cf, on the same segment.
        std::optional dup = handle->clone(tid);
        BOOST_REQUIRE(bool(*dup));
        BOOST_REQUIRE(dup->rp() == segment_start);

        // References under a cf the segment was never written for, such as
        // system.raft_groups; repeats are legal.
        std::optional pin1 = handle->clone(other_tid);
        std::optional pin2 = handle->clone(other_tid);
        BOOST_REQUIRE(bool(*pin1));
        BOOST_REQUIRE(pin1->rp() == segment_start);
        BOOST_REQUIRE(bool(*pin2));

        // A reference cloned from a reference works the same.
        std::optional chained = pin1->clone(other_tid);
        BOOST_REQUIRE(chained->rp() == segment_start);

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

// Test: a truncation record whose segment the commitlog has dropped is purged,
// and a record whose segment it still has survives. The history is ordered by
// time, so a stale record can sit behind a live one; a purge that stops at the
// first live record keeps the stale record.
SEASTAR_TEST_CASE(test_purge_stale_truncations_drops_only_the_dropped_segments) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();
        auto rg_tid = make_table_id();

        service::strong_consistency::raft_commitlog rc(gid, log, tid, rg_tid, {});

        // Everything below this segment is gone from the commitlog.
        const auto oldest = log.min_position().id;
        BOOST_REQUIRE_GT(oldest, 0);

        // Stale last, so stopping at the first live record would keep it.
        rc.seed_truncations({
            {.segment = oldest - 1, .from = raft::index_t(5), .to = raft::index_t(9)},
            {.segment = oldest, .from = raft::index_t(10), .to = raft::index_t(14)},
            {.segment = oldest + 3, .from = raft::index_t(20), .to = raft::index_t(24)},
            // oldest can be 1, so a lower id would wrap around and look live.
            {.segment = oldest - 1, .from = raft::index_t(1), .to = raft::index_t(4)},
        });
        BOOST_REQUIRE_EQUAL(rc.truncations().size(), 4);

        rc.purge_stale_truncations();

        // The two live records, in the order they were seeded.
        BOOST_REQUIRE_EQUAL(rc.truncations().size(), 2);
        BOOST_REQUIRE_EQUAL(rc.truncations()[0].segment, oldest);
        BOOST_REQUIRE_EQUAL(rc.truncations()[0].from, raft::index_t(10));
        BOOST_REQUIRE_EQUAL(rc.truncations()[1].segment, oldest + 3);
        BOOST_REQUIRE_EQUAL(rc.truncations()[1].from, raft::index_t(20));

        // The record at min_position itself stays: that segment is still there.
        rc.purge_stale_truncations();
        BOOST_REQUIRE_EQUAL(rc.truncations().size(), 2);
        co_return;
    });
}

// Test: clone(cf) charges the table it is given. Segment counts alone cannot
// tell the two tables apart, so drop one table's counts and see which reference
// still holds the segment.
SEASTAR_TEST_CASE(test_rp_handle_clone_charges_the_given_table) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();
        auto rg_tid = make_table_id();

        auto entry = make_command_entry(raft::term_t(1), raft::index_t(1));
        // Both references stay alive to the end. Dropping a table's counts frees
        // the segment here.
        auto handle = co_await write_raft_entry_to_commitlog(log, tid, gid, entry);
        auto cloned = handle.clone(rg_tid);

        // Only a sealed segment reports as dirty.
        co_await log.force_new_active_segment();
        BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), 1);

        // Everything charged to the entry's own table goes. A clone() that
        // charged tid frees the segment here.
        log.discard_completed_segments(tid);
        BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), 1);

        // Drop the clone's table. Nothing holds the segment now.
        log.discard_completed_segments(rg_tid);
        BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), 0);
    });
}

// Test: a clone given to a memtable is released by that memtable's flush. A
// memtable keeps its references in an rp_set keyed by segment id and hands the
// set to discard_completed_segments() on flush; the clone's (segment id, 0)
// position has to land in the right segment's count.
SEASTAR_TEST_CASE(test_rp_handle_clone_released_through_rp_set) {
    return cl_test([](commitlog& log) -> future<> {
        auto gid = make_group_id();
        auto tid = make_table_id();
        auto rg_tid = make_table_id();

        auto entry = make_command_entry(raft::term_t(1), raft::index_t(1));
        std::optional handle = co_await write_raft_entry_to_commitlog(log, tid, gid, entry);

        // What memtable::update() does with the handle it is given.
        db::rp_set memtable_set;
        memtable_set.put(handle->clone(rg_tid));
        // The source goes first: the memtable's count holds the segment alone.
        handle.reset();

        co_await log.force_new_active_segment();
        BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), 1);

        // What the memtable's flush does.
        log.discard_completed_segments(rg_tid, memtable_set);
        BOOST_REQUIRE_EQUAL(log.get_num_dirty_segments(), 0);
    });
}

// Test: the copies a truncation superseded are dropped, and only those. A
// truncation removes a suffix of the raft log, so the discarded copies are the
// batch's tail; the cursor walks the record as the copies are read.
BOOST_AUTO_TEST_CASE(test_replay_drop_truncated_copies) {
    using db::raft_buffer_detail::drop_truncated_copies;
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
        auto rest = drop_truncated_copies(cursors, batch());
        BOOST_REQUIRE_EQUAL(rest.size(), 2);
        BOOST_REQUIRE_EQUAL(rest[0]->idx, raft::index_t(1));
        BOOST_REQUIRE_EQUAL(rest[1]->idx, raft::index_t(2));
        BOOST_REQUIRE(cursors.front().exhausted());
    }

    // A record for a range this batch does not reach leaves it untouched.
    {
        segment_cursors cursors{truncation_cursor{
                .from = raft::index_t(9), .to = raft::index_t(12), .next = raft::index_t(9)}};
        auto rest = drop_truncated_copies(cursors, batch());
        BOOST_REQUIRE_EQUAL(rest.size(), 5);
        BOOST_REQUIRE(!cursors.front().exhausted());
    }

    // No records at all: nothing is stale.
    {
        segment_cursors cursors;
        BOOST_REQUIRE_EQUAL(drop_truncated_copies(cursors, batch()).size(), 5);
    }
}

// Test: several truncations of one segment are matched oldest-first, so a
// segment that was truncated twice drops the right copy each time.
BOOST_AUTO_TEST_CASE(test_replay_drop_truncated_copies_multiple_truncations) {
    using db::raft_buffer_detail::drop_truncated_copies;
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

    BOOST_REQUIRE(drop_truncated_copies(cursors, pair()).empty());
    BOOST_REQUIRE(cursors.front().exhausted());
    BOOST_REQUIRE(drop_truncated_copies(cursors, pair()).empty());
    BOOST_REQUIRE(cursors.back().exhausted());
    // Third copy: no record left, so this one is the current copy and survives.
    auto rest = drop_truncated_copies(cursors, pair());
    BOOST_REQUIRE_EQUAL(rest.size(), 2);
    BOOST_REQUIRE_EQUAL(rest[0]->idx, raft::index_t(4));
}

// Test: truncations of one segment that reach back past each other. Several
// cursors can be live at one index, so a match on only the oldest keeps a
// truncated copy and replay applies an entry no leader ever committed.
BOOST_AUTO_TEST_CASE(test_replay_drop_truncated_copies_overlapping_truncations) {
    using db::raft_buffer_detail::drop_truncated_copies;
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
    BOOST_REQUIRE(drop_truncated_copies(cursors, batch(5, 9, raft::term_t(1))).empty());
    // The second leader's 7',8' are stale against what is left of the second record.
    BOOST_REQUIRE(drop_truncated_copies(cursors, batch(7, 8, raft::term_t(2))).empty());
    // Every cursor is used up, so the third leader's copies stand.
    auto current = drop_truncated_copies(cursors, batch(5, 8, raft::term_t(3)));
    BOOST_REQUIRE_EQUAL(current.size(), 4);
    BOOST_REQUIRE_EQUAL(current.front()->idx, raft::index_t(5));
    BOOST_REQUIRE_EQUAL(current.front()->term, raft::term_t(3));
}

// Test: a later write at index N supersedes what is buffered at or above N.
// The supersede rule makes a leader change that reuses indexes come out right:
// the copy written later is the current copy.
BOOST_AUTO_TEST_CASE(test_replay_superseded_by) {
    using db::raft_buffer_detail::superseded_by;

    std::deque<db::raft_buffer_detail::buffered_entry> buf;
    for (int i = 3; i <= 7; ++i) {
        buf.push_back(db::raft_buffer_detail::buffered_entry{
                .entry = make_command_entry(raft::term_t(1), raft::index_t(i)), .segment = 1});
    }

    // A higher term at the overlap is a truncation: the copies below it go.
    const auto same = [](int idx) { return make_command_entry(raft::term_t(1), raft::index_t(idx)); };
    const auto newer = [](int idx) { return make_command_entry(raft::term_t(2), raft::index_t(idx)); };
    const auto no_commit = raft::index_t(0);
    BOOST_REQUIRE_EQUAL(superseded_by(buf, newer(8), no_commit), 0);
    BOOST_REQUIRE_EQUAL(superseded_by(buf, newer(6), no_commit), 2);
    BOOST_REQUIRE_EQUAL(superseded_by(buf, newer(3), no_commit), 5);
    BOOST_REQUIRE_EQUAL(superseded_by({}, newer(1), no_commit), 0);

    // Below the buffer at or under the commit index: copies of committed entries,
    // which are never replaced.
    BOOST_REQUIRE_EQUAL(superseded_by(buf, newer(1), raft::index_t(2)), 0);
    BOOST_REQUIRE_EQUAL(superseded_by(buf, newer(2), raft::index_t(2)), 0);

    // Below the buffer and above the commit index: raft got there by truncating at
    // that index, so the whole buffer is superseded whatever the terms say.
    BOOST_REQUIRE_EQUAL(superseded_by(buf, newer(2), raft::index_t(1)), buf.size());
    BOOST_REQUIRE_EQUAL(superseded_by(buf, same(2), raft::index_t(1)), buf.size());

    // The same term at the overlap is another copy of entries already buffered,
    // so nothing is superseded. A crash between the runs of a split rewrite
    // leaves such a duplicate, and dropping the tail above the run loses
    // entries this replica already acknowledged. same(7) overlaps the last
    // buffered entry only, which is the narrowest overlap the term check sees.
    BOOST_REQUIRE_EQUAL(superseded_by(buf, same(3), no_commit), 0);
    BOOST_REQUIRE_EQUAL(superseded_by(buf, same(6), no_commit), 0);
    BOOST_REQUIRE_EQUAL(superseded_by(buf, same(7), no_commit), 0);
    // The same overlap in a newer term supersedes that one entry.
    BOOST_REQUIRE_EQUAL(superseded_by(buf, newer(7), no_commit), 1);

    // A batch continuing the log overlaps nothing, whatever its term.
    BOOST_REQUIRE_EQUAL(superseded_by(buf, same(8), no_commit), 0);
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
    // system.raft_groups exists only under this flag, and the flag also gives
    // the commitlog the descriptor tag a raft batch is written under.
    cfg.db_config->experimental_features(
            {db::experimental_features_t::feature::STRONGLY_CONSISTENT_TABLES},
            db::config::config_source::CommandLine);
    cfg.db_config->auto_snapshot.set(false);
    cfg.db_config->tablets_mode_for_new_keyspaces.set(db::tablets_mode_t::mode::enabled);
    cfg.initial_tablets = 1;
    return cfg;
}

// The same, with the override that starts a node whose commitlog replay reported damage.
cql_test_config sc_replay_config_starting_on_damage() {
    auto cfg = sc_replay_config();
    cfg.db_config->strongly_consistent_tables_start_on_damaged_commitlog.set(true);
    return cfg;
}


// A COMMAND entry carrying one real mutation, encoded as the coordinator does: a
// raft_command holding a frozen_mutation in raft::command. Dummies do not work
// for the stale-copy test below: dropped or applied, a dummy leaves no trace.
raft::log_entry_ptr make_mutation_entry(const schema_ptr& schema, raft::term_t term, raft::index_t idx,
        int32_t pk, int32_t v, api::timestamp_type ts) {
    mutation m(schema, partition_key::from_single_value(*schema, int32_type->decompose(pk)));
    const auto ck = clustering_key::make_empty();
    m.set_clustered_cell(ck, to_bytes("v"), data_value(v), ts);
    // A row marker, as an INSERT writes one, so the row exists in its own right.
    m.partition().clustered_row(*schema, ck).apply(row_marker(ts));
    raft::command cmd;
    ser::serialize(cmd, service::strong_consistency::raft_command{.mutation = freeze(m)});
    return make_lw_shared<raft::log_entry>(raft::log_entry{
            .term = term, .idx = idx, .data = std::move(cmd)});
}

} // anonymous namespace

// Replay persists a floor of commit_idx, so every index at or below it has to have been
// read. A gap there refuses the boot. Above commit_idx no header read here claims the
// entries were committed, so replay lets that tail go. A gap refuses with nothing
// reported damaged, so the refusal does not hang off the damage report.
SEASTAR_TEST_CASE(test_replay_refuses_a_gap_below_the_commit_index) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        const auto table = table_id(utils::UUID_gen::get_time_UUID());
        const auto my_id = env.local_db().get_token_metadata().get_my_id();

        const auto seed_at = [&](raft::group_id gid, raft::index_t floor,
                std::vector<service::strong_consistency::truncation_record> truncations) {
            service::strong_consistency::raft_groups_storage::store_descriptor(
                    env.local_qp(), gid, this_shard_id(), floor, raft::term_t(1),
                    raft::configuration{}, truncations).get();
            set_sc_tablet_metadata(env, table, gid,
                    locator::tablet_replica_set{{my_id, this_shard_id()}}).get();
        };
        const auto seed = [&](raft::group_id gid,
                std::vector<service::strong_consistency::truncation_record> truncations) {
            seed_at(gid, raft::index_t(0), std::move(truncations));
        };
        const auto run = [&](raft::group_id gid, raft::index_t first, raft::index_t last,
                raft::index_t commit_idx, db::segment_id_type segment,
                db::raft_commitlog_replay_buffer& buffer) {
            std::vector<raft::log_entry_ptr> batch;
            for (auto i = first; i <= last; i = i + raft::index_t{1}) {
                batch.push_back(make_dummy_entry(raft::term_t(1), i));
            }
            buffer.add_batch(env.local_db(), env.local_qp(), env.get_system_keyspace().local(),
                    gid, segment, commit_idx, batch).get();
        };
        const auto run_in_term = [&](raft::group_id gid, raft::term_t term, raft::index_t first,
                raft::index_t last, raft::index_t commit_idx, db::segment_id_type segment,
                db::raft_commitlog_replay_buffer& buffer) {
            std::vector<raft::log_entry_ptr> batch;
            for (auto i = first; i <= last; i = i + raft::index_t{1}) {
                batch.push_back(make_dummy_entry(term, i));
            }
            buffer.add_batch(env.local_db(), env.local_qp(), env.get_system_keyspace().local(),
                    gid, segment, commit_idx, batch).get();
        };
        const auto refuses_with = [](db::raft_commitlog_replay_buffer& buffer, cql_test_env& env,
                const sstring& detail) {
            BOOST_REQUIRE_EXCEPTION(
                    buffer.finish_replay(env.local_db(), env.local_qp()).get(),
                    std::runtime_error,
                    [&](const std::runtime_error& e) {
                        const sstring what(e.what());
                        return what.find("committed entries are missing") != sstring::npos
                                && what.find(detail) != sstring::npos;
                    });
        };

        // Index 10 is committed while the run stopped at 3, so 4..10 are gone.
        {
            const auto gid = make_group_id();
            seed(gid, {});
            db::raft_commitlog_replay_buffer buffer;
            run(gid, raft::index_t(1), raft::index_t(3), raft::index_t(0), 1, buffer);
            run(gid, raft::index_t(11), raft::index_t(12), raft::index_t(10), 3, buffer);
            buffer.note_unreadable_segment(2);
            refuses_with(buffer, env, "read only to 3");
            buffer.stop().get();
        }

        // The same gap with nothing reported damaged. A segment file that is gone
        // leaves the same evidence and gets the same answer, so the refusal does not
        // hang off the damage report.
        {
            const auto gid = make_group_id();
            seed(gid, {});
            db::raft_commitlog_replay_buffer buffer;
            run(gid, raft::index_t(1), raft::index_t(3), raft::index_t(0), 1, buffer);
            run(gid, raft::index_t(11), raft::index_t(12), raft::index_t(10), 3, buffer);
            refuses_with(buffer, env, "No segment was reported damaged");
            buffer.stop().get();
        }

        // A superseded copy does not cover its index. The persisted truncation record
        // drops segment 1's copies of 2 and 3, the segment holding the current copies
        // is gone, so the run stops at 1 although a copy of 2 and 3 was read.
        {
            const auto gid = make_group_id();
            seed(gid, {service::strong_consistency::truncation_record{
                    .segment = 1, .from = raft::index_t(2), .to = raft::index_t(3)}});
            db::raft_commitlog_replay_buffer buffer;
            run(gid, raft::index_t(1), raft::index_t(3), raft::index_t(0), 1, buffer);
            run(gid, raft::index_t(5), raft::index_t(5), raft::index_t(4), 3, buffer);
            buffer.note_unreadable_segment(2);
            refuses_with(buffer, env, "read only to 1");
            buffer.stop().get();
        }

        // A batch under the buffered front, at one past the commit index. The truncation
        // that let it exist discarded everything at or above its index, so the buffer
        // goes and the batch stands. Nothing under it is missing, so the node starts.
        {
            const auto gid = make_group_id();
            seed(gid, {});
            db::raft_commitlog_replay_buffer buffer;
            run(gid, raft::index_t(3), raft::index_t(4), raft::index_t(0), 1, buffer);
            run(gid, raft::index_t(1), raft::index_t(4), raft::index_t(0), 2, buffer);
            run(gid, raft::index_t(5), raft::index_t(5), raft::index_t(2), 3, buffer);
            BOOST_REQUIRE_NO_THROW(buffer.finish_replay(env.local_db(), env.local_qp()).get());
            buffer.stop().get();
        }

        // The same shape one segment further along: truncate_log() popped the record for
        // 6 and the segment holding it was reclaimed, so replay meets 7 in the old term
        // first and the new leader's 6 second, both from the segment that survived.
        // A refusal here keeps a node down over a log that is whole.
        {
            const auto gid = make_group_id();
            seed_at(gid, raft::index_t(5), {});
            db::raft_commitlog_replay_buffer buffer;
            run_in_term(gid, raft::term_t(1), raft::index_t(7), raft::index_t(7),
                    raft::index_t(5), 1, buffer);
            run_in_term(gid, raft::term_t(2), raft::index_t(6), raft::index_t(6),
                    raft::index_t(5), 1, buffer);
            BOOST_REQUIRE_NO_THROW(buffer.finish_replay(env.local_db(), env.local_qp()).get());
            buffer.stop().get();
        }

        // A batch under the buffered front but above one past the commit index. The
        // truncation at 2 left index 1 alone, so 1 is still part of the log and replay
        // never read it.
        {
            const auto gid = make_group_id();
            seed(gid, {});
            db::raft_commitlog_replay_buffer buffer;
            run(gid, raft::index_t(3), raft::index_t(4), raft::index_t(0), 1, buffer);
            BOOST_REQUIRE_EXCEPTION(
                    run(gid, raft::index_t(2), raft::index_t(4), raft::index_t(0), 2, buffer),
                    std::runtime_error,
                    [](const std::runtime_error& e) {
                        const sstring what(e.what());
                        return what.find("is missing indexes 1 to 1") != sstring::npos;
                    });
            buffer.stop().get();
        }

        // A supersede takes back the indexes it pops. Segment 1's 1 and 2 are counted,
        // segment 2 replaces 1 in a newer term and pops both, and the segment holding
        // the new 2 is gone. The run has to drop to 1, so that the batch committing 2
        // finds it missing.
        {
            const auto gid = make_group_id();
            seed(gid, {});
            db::raft_commitlog_replay_buffer buffer;
            const auto batch = [&](raft::term_t term, raft::index_t first, raft::index_t last,
                    raft::index_t commit_idx, db::segment_id_type segment) {
                std::vector<raft::log_entry_ptr> entries;
                for (auto i = first; i <= last; i = i + raft::index_t{1}) {
                    entries.push_back(make_dummy_entry(term, i));
                }
                buffer.add_batch(env.local_db(), env.local_qp(), env.get_system_keyspace().local(),
                        gid, segment, commit_idx, entries).get();
            };
            batch(raft::term_t(1), raft::index_t(1), raft::index_t(2), raft::index_t(0), 1);
            batch(raft::term_t(2), raft::index_t(1), raft::index_t(1), raft::index_t(0), 2);
            batch(raft::term_t(2), raft::index_t(3), raft::index_t(3), raft::index_t(2), 4);
            refuses_with(buffer, env, "read only to 1");
            buffer.stop().get();
        }
    }, sc_replay_config());
}

// The damage override starts a node over an unreadable segment. A gap below the commit
// index is a separate refusal that the override must not reach: finish_replay() throws for
// the missing entries before it ever reads the flag.
SEASTAR_TEST_CASE(test_the_damage_override_does_not_start_a_node_over_a_gap) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        const auto table = table_id(utils::UUID_gen::get_time_UUID());
        const auto my_id = env.local_db().get_token_metadata().get_my_id();
        const auto gid = make_group_id();
        service::strong_consistency::raft_groups_storage::store_descriptor(
                env.local_qp(), gid, this_shard_id(), raft::index_t(0), raft::term_t(1),
                raft::configuration{}, {}).get();
        set_sc_tablet_metadata(env, table, gid,
                locator::tablet_replica_set{{my_id, this_shard_id()}}).get();

        const auto run = [&](raft::index_t first, raft::index_t last, raft::index_t commit_idx,
                db::segment_id_type segment, db::raft_commitlog_replay_buffer& buffer) {
            std::vector<raft::log_entry_ptr> batch;
            for (auto i = first; i <= last; i = i + raft::index_t{1}) {
                batch.push_back(make_dummy_entry(raft::term_t(1), i));
            }
            buffer.add_batch(env.local_db(), env.local_qp(), env.get_system_keyspace().local(),
                    gid, segment, commit_idx, batch).get();
        };

        // The gap of the first case in test_replay_refuses_a_gap_below_the_commit_index:
        // index 10 is committed while the run of read indexes stopped at 3.
        db::raft_commitlog_replay_buffer buffer;
        run(raft::index_t(1), raft::index_t(3), raft::index_t(0), 1, buffer);
        run(raft::index_t(11), raft::index_t(12), raft::index_t(10), 3, buffer);
        buffer.note_unreadable_segment(2);
        BOOST_REQUIRE_EXCEPTION(
                buffer.finish_replay(env.local_db(), env.local_qp()).get(),
                std::runtime_error,
                [](const std::runtime_error& e) {
                    const sstring what(e.what());
                    return what.find("committed entries are missing") != sstring::npos
                            && what.find("read only to 3") != sstring::npos;
                });
        buffer.stop().get();
    }, sc_replay_config_starting_on_damage());
}

// A refused boot has to leave system.raft_groups alone, so that the next replay of the
// same segments starts from the floor it started from this time. The checks that refuse
// therefore run before the first store_descriptor().
SEASTAR_TEST_CASE(test_a_refused_replay_leaves_the_row_untouched) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        const auto table = table_id(utils::UUID_gen::get_time_UUID());
        const auto my_id = env.local_db().get_token_metadata().get_my_id();

        const auto seed = [&](raft::group_id gid, raft::index_t floor) {
            service::strong_consistency::raft_groups_storage::store_descriptor(
                    env.local_qp(), gid, this_shard_id(), floor, raft::term_t(1),
                    raft::configuration{}, {}).get();
            set_sc_tablet_metadata(env, table, gid,
                    locator::tablet_replica_set{{my_id, this_shard_id()}}).get();
        };
        const auto run = [&](raft::group_id gid, raft::index_t first, raft::index_t last,
                raft::index_t commit_idx, db::segment_id_type segment,
                db::raft_commitlog_replay_buffer& buffer) {
            std::vector<raft::log_entry_ptr> batch;
            for (auto i = first; i <= last; i = i + raft::index_t{1}) {
                batch.push_back(make_dummy_entry(raft::term_t(1), i));
            }
            buffer.add_batch(env.local_db(), env.local_qp(), env.get_system_keyspace().local(),
                    gid, segment, commit_idx, batch).get();
        };
        const auto refuses_with = [&](db::raft_commitlog_replay_buffer& buffer,
                const sstring& missing, const sstring& detail) {
            BOOST_REQUIRE_EXCEPTION(
                    buffer.finish_replay(env.local_db(), env.local_qp()).get(),
                    std::runtime_error,
                    [&](const std::runtime_error& e) {
                        const sstring what(e.what());
                        return what.find(missing) != sstring::npos
                                && what.find(detail) != sstring::npos;
                    });
        };
        const auto still_seeded_at = [&](raft::group_id gid, raft::index_t floor) {
            const auto persisted = service::strong_consistency::raft_groups_storage::load_descriptor(
                    env.local_qp(), gid, this_shard_id()).get();
            BOOST_REQUIRE(persisted.exists);
            BOOST_REQUIRE_EQUAL(persisted.idx, floor);
        };

        // The tail starts above the floor. 6..8 are at or below the commit index, so the
        // buffer keeps none of them and the run reaches 8. The tail then starts at 10,
        // one over the 9 the floor of 8 asks for.
        {
            const auto gid = make_group_id();
            seed(gid, raft::index_t(5));
            db::raft_commitlog_replay_buffer buffer;
            run(gid, raft::index_t(6), raft::index_t(8), raft::index_t(8), 1, buffer);
            run(gid, raft::index_t(10), raft::index_t(11), raft::index_t(8), 2, buffer);
            refuses_with(buffer, "is missing indexes 9 to 9", "starts at 10 over a floor of 8");
            still_seeded_at(gid, raft::index_t(5));
            buffer.stop().get();
        }

        // A hole inside the tail. The tail starts where the floor asks, so the front
        // check passes and the pairwise check is the one that sees 7 missing.
        {
            const auto gid = make_group_id();
            seed(gid, raft::index_t(5));
            db::raft_commitlog_replay_buffer buffer;
            run(gid, raft::index_t(6), raft::index_t(6), raft::index_t(5), 1, buffer);
            run(gid, raft::index_t(8), raft::index_t(8), raft::index_t(5), 2, buffer);
            refuses_with(buffer, "is missing indexes 7 to 7", "jumps from 6 to 8");
            still_seeded_at(gid, raft::index_t(5));
            buffer.stop().get();
        }
    }, sc_replay_config());
}

namespace {

// One group with a floor of 5 and one batch at 6, so the log replay read is whole.
struct damaged_segment_fixture {
    cql_test_env& env;
    table_id table = table_id(utils::UUID_gen::get_time_UUID());
    raft::group_id gid = make_group_id();

    void seed(locator::tablet_replica_set replicas) {
        service::strong_consistency::raft_groups_storage::store_descriptor(
                env.local_qp(), gid, this_shard_id(), raft::index_t(5), raft::term_t(1),
                raft::configuration{}, {}).get();
        set_sc_tablet_metadata(env, table, gid, std::move(replicas)).get();
    }

    void read_one_batch(db::raft_commitlog_replay_buffer& buffer) {
        const auto batch = std::vector<raft::log_entry_ptr>{
            make_dummy_entry(raft::term_t(1), raft::index_t(6)),
        };
        buffer.add_batch(env.local_db(), env.local_qp(), env.get_system_keyspace().local(),
                gid, 2, raft::index_t(5), batch).get();
    }
};

} // anonymous namespace

// Test: a damaged segment refuses the boot once this shard hosts a strongly consistent
// group. The unread bytes may have held raft batches the node acknowledged, and a lost
// tail leaves nothing to find: the batch declaring the higher commit_idx went down
// with the tail, so the index checks pass over the shorter log.
SEASTAR_TEST_CASE(test_replay_refuses_a_damaged_segment) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        const auto my_id = env.local_db().get_token_metadata().get_my_id();

        // The group's only replica is on another host. This shard recovers no log, so
        // the unread bytes cost it nothing. This case runs first: the tablet metadata the
        // second block writes stays in the environment, and one hosted group is enough to refuse.
        {
            damaged_segment_fixture fixture{env};
            fixture.seed(locator::tablet_replica_set{
                    {locator::host_id{utils::UUID_gen::get_time_UUID()}, 0}});
            db::raft_commitlog_replay_buffer buffer;
            fixture.read_one_batch(buffer);
            buffer.note_unreadable_segment(3);
            BOOST_REQUIRE_NO_THROW(buffer.finish_replay(env.local_db(), env.local_qp()).get());
            drop_sc_tablet_metadata(env, fixture.table).get();
            buffer.stop().get();
        }

        // Nothing is missing between the floor and the tail, and the segment that could
        // not be read still refuses.
        {
            damaged_segment_fixture fixture{env};
            fixture.seed(locator::tablet_replica_set{{my_id, this_shard_id()}});
            db::raft_commitlog_replay_buffer buffer;
            fixture.read_one_batch(buffer);
            buffer.note_unreadable_segment(3);
            BOOST_REQUIRE_EXCEPTION(
                    buffer.finish_replay(env.local_db(), env.local_qp()).get(),
                    std::runtime_error,
                    [](const std::runtime_error& e) {
                        const sstring what(e.what());
                        return what.find("could not be read in full") != sstring::npos
                                && what.find("hosts 1 strongly consistent raft group") != sstring::npos;
                    });
            // The refusal runs before any descriptor is written.
            const auto persisted = service::strong_consistency::raft_groups_storage::load_descriptor(
                    env.local_qp(), fixture.gid, this_shard_id()).get();
            BOOST_REQUIRE(persisted.exists);
            BOOST_REQUIRE_EQUAL(persisted.idx, raft::index_t(5));
            buffer.stop().get();
        }
    }, sc_replay_config());
}

// Test: strongly_consistent_tables_start_on_damaged_commitlog starts the node the
// refusal above stops. The commitlog still reports an intact segment as damaged on some
// paths (SCYLLADB-4853), so an operator needs a way past the refusal.
SEASTAR_TEST_CASE(test_replay_starts_on_a_damaged_segment_when_the_override_is_set) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        const auto my_id = env.local_db().get_token_metadata().get_my_id();
        damaged_segment_fixture fixture{env};
        fixture.seed(locator::tablet_replica_set{{my_id, this_shard_id()}});
        db::raft_commitlog_replay_buffer buffer;
        fixture.read_one_batch(buffer);
        buffer.note_unreadable_segment(3);
        BOOST_REQUIRE_NO_THROW(buffer.finish_replay(env.local_db(), env.local_qp()).get());
        // The tail replay recovered is handed over, so the group starts with index 6.
        auto data = buffer.take_replayed_group_entries(fixture.gid);
        BOOST_REQUIRE_EQUAL(data.entries.size(), 1u);
        BOOST_REQUIRE_EQUAL(data.entries.front()->idx, raft::index_t(6));
        buffer.stop().get();
    }, sc_replay_config_starting_on_damage());
}

// hosts_raft_group() answers both "should this shard run the group" and "should replay
// recover its log", so a stage it gets wrong either tears down a group that is still a
// member or resurrects a group that is not. The rollback stages matter most: the
// pending replica may be the leader driving its own removal.
BOOST_AUTO_TEST_CASE(test_hosts_raft_group_per_stage) {
    using namespace locator;
    using service::strong_consistency::hosts_raft_group;

    const tablet_replica leaving{host_id{utils::UUID_gen::get_time_UUID()}, 0};
    const tablet_replica staying{host_id{utils::UUID_gen::get_time_UUID()}, 0};
    const tablet_replica pending{host_id{utils::UUID_gen::get_time_UUID()}, 0};

    tablet_info tinfo;
    tinfo.replicas = {leaving, staying};

    const auto at = [&](tablet_transition_stage stage) {
        return tablet_transition_info(stage, tablet_transition_kind::migration,
                tablet_replica_set{staying, pending}, pending);
    };

    // No transition: the replica set is the whole answer.
    BOOST_CHECK(hosts_raft_group(tinfo, nullptr, leaving));
    BOOST_CHECK(!hosts_raft_group(tinfo, nullptr, pending));

    // Before the removal is confirmed both replicas are members: the leaving replica's vote can
    // be needed to commit its own removal, and the pending one can be the leader.
    for (const auto stage : {tablet_transition_stage::start_migration,
                             tablet_transition_stage::sc_add_nonvoter,
                             tablet_transition_stage::sc_snapshot_transfer,
                             tablet_transition_stage::sc_become_voter,
                             tablet_transition_stage::sc_rollback}) {
        const auto trinfo = at(stage);
        BOOST_CHECK_MESSAGE(hosts_raft_group(tinfo, &trinfo, leaving), fmt::format("{}", stage));
        BOOST_CHECK_MESSAGE(hosts_raft_group(tinfo, &trinfo, pending), fmt::format("{}", stage));
    }

    // The leaving replica is out from use_new on. Replay must agree, since the
    // teardown has given up its segment references by then.
    for (const auto stage : {tablet_transition_stage::use_new,
                             tablet_transition_stage::cleanup,
                             tablet_transition_stage::end_migration}) {
        const auto trinfo = at(stage);
        BOOST_CHECK_MESSAGE(!hosts_raft_group(tinfo, &trinfo, leaving), fmt::format("{}", stage));
        BOOST_CHECK_MESSAGE(hosts_raft_group(tinfo, &trinfo, pending), fmt::format("{}", stage));
    }

    // The mirror case: a rolled-back migration drops the pending replica instead.
    for (const auto stage : {tablet_transition_stage::cleanup_target,
                             tablet_transition_stage::revert_migration}) {
        const auto trinfo = at(stage);
        BOOST_CHECK_MESSAGE(hosts_raft_group(tinfo, &trinfo, leaving), fmt::format("{}", stage));
        BOOST_CHECK_MESSAGE(!hosts_raft_group(tinfo, &trinfo, pending), fmt::format("{}", stage));
    }
}

// Test that replay discards a group whose tablet has no replica on this shard, even
// though the group is still present in tablet metadata because it lives on its other
// replicas. Applying its entries here resurrects data on a node that gave the
// range up.
SEASTAR_TEST_CASE(test_replay_discards_groups_without_local_replica) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        const auto gid = make_group_id();
        const auto table = table_id(utils::UUID_gen::get_time_UUID());
        const auto my_id = env.local_db().get_token_metadata().get_my_id();

        // The group has persisted state here - the replica used to be a member and its
        // cleanup hasn't erased it yet - so only the ownership test can discard these
        // entries. Index 0, so nothing the batch carries counts as committed.
        service::strong_consistency::raft_groups_storage::store_descriptor(
                env.local_qp(), gid, this_shard_id(), raft::index_t(0), raft::term_t(0),
                raft::configuration{}, {}).get();

        const auto batch = std::vector<raft::log_entry_ptr>{
            make_dummy_entry(raft::term_t(1), raft::index_t(1)),
            make_dummy_entry(raft::term_t(1), raft::index_t(2)),
        };

        // The tablet's only replica is on another host.
        set_sc_tablet_metadata(env, table, gid,
                locator::tablet_replica_set{{locator::host_id{utils::UUID_gen::get_time_UUID()}, 0}}).get();

        db::raft_commitlog_replay_buffer buffer;
        buffer.add_batch(env.local_db(), env.local_qp(), env.get_system_keyspace().local(),
                gid, 1, raft::index_t(0), batch).get();
        buffer.finish_replay(env.local_db(), env.local_qp()).get();
        drop_sc_tablet_metadata(env, table).get();

        auto data = buffer.take_replayed_group_entries(gid);
        BOOST_CHECK(data.entries.empty());
        BOOST_CHECK(data.records.empty());
        buffer.stop().get();

        // The same group, now with a replica on this shard, is not discarded.
        set_sc_tablet_metadata(env, table, gid,
                locator::tablet_replica_set{{my_id, this_shard_id()}}).get();

        db::raft_commitlog_replay_buffer owned_buffer;
        owned_buffer.add_batch(env.local_db(), env.local_qp(), env.get_system_keyspace().local(),
                gid, 1, raft::index_t(0), batch).get();
        owned_buffer.finish_replay(env.local_db(), env.local_qp()).get();

        // Nothing is committed - the floor is 0 - so both entries are kept for the
        // group's log, each held by a record of the rewrite.
        auto owned_data = owned_buffer.take_replayed_group_entries(gid);
        BOOST_CHECK_EQUAL(owned_data.entries.size(), 2u);
        BOOST_CHECK(!owned_data.records.empty());
        owned_buffer.stop().get();
    }, sc_replay_config());
}

// Test that replay discards a group this shard persists nothing about. Tablet cleanup
// leaves a group in that state if it crashes after erasing the raft state and before
// removing the tablet's storage: the tablet metadata still places a replica here, so only
// the persisted-state test can discard these entries.
SEASTAR_TEST_CASE(test_replay_discards_groups_without_persisted_state) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        const auto gid = make_group_id();
        const auto table = table_id(utils::UUID_gen::get_time_UUID());
        const auto my_id = env.local_db().get_token_metadata().get_my_id();

        set_sc_tablet_metadata(env, table, gid,
                locator::tablet_replica_set{{my_id, this_shard_id()}}).get();
        BOOST_REQUIRE(!service::strong_consistency::raft_groups_storage::load_descriptor(
                env.local_qp(), gid, this_shard_id()).get().exists);

        db::raft_commitlog_replay_buffer buffer;
        buffer.add_batch(env.local_db(), env.local_qp(), env.get_system_keyspace().local(),
                gid, 1, raft::index_t(0), std::vector<raft::log_entry_ptr>{
                    make_dummy_entry(raft::term_t(1), raft::index_t(1)),
                    make_dummy_entry(raft::term_t(1), raft::index_t(2)),
                }).get();
        buffer.finish_replay(env.local_db(), env.local_qp()).get();

        auto data = buffer.take_replayed_group_entries(gid);
        BOOST_CHECK(data.entries.empty());
        BOOST_CHECK(data.records.empty());
        buffer.stop().get();
    }, sc_replay_config());
}

// Test: a second replay of the same old segments recovers the same uncommitted
// tail, and does not take the floor the first pass persisted as covering the tail.
//
// finish_replay() persists the floor before it rewrites the uncommitted tail, and
// main.cc deletes the old segments later still. In that window the floor is
// durable while the rewritten tail is pinned by nothing a group has claimed.
// Losing the rewrite is safe only because the old segments still hold the same
// entries.
//
// The invariant: the floor is the group's commit index, and add_batch() discards
// a copy only at or below the floor. Get either rule wrong and the second pass
// takes the tail for committed, hands the group a short log, and drops entries a
// leader counted toward a quorum. Rows cannot show the loss, so the assertions
// are on the recovered log and the floor.
SEASTAR_TEST_CASE(test_second_replay_recovers_the_rewritten_tail) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        // Everything runs on shard 0, which the fabricated replica and the row
        // finish_replay() writes both name, so the shard count does not matter.
        env.execute_cql("create table ks.cf (pk int primary key)").get();
        const auto table = env.local_db().find_schema("ks", "cf")->id();
        const auto gid = make_group_id();
        set_sc_tablet_metadata(env, table, gid, locator::tablet_replica_set{
                {env.local_db().get_token_metadata().get_my_id(), this_shard_id()}}).get();

        // One replay pass, returning what a starting group gets. A
        // fresh buffer each time, as a fresh startup has.
        const auto replay = [&] (const std::vector<replayed_batch>& batches) {
            db::raft_commitlog_replay_buffer buffer;
            for (const auto& batch : batches) {
                buffer.add_batch(env.local_db(), env.local_qp(), env.get_system_keyspace().local(),
                        gid, batch.segment, batch.commit_idx, batch.entries).get();
            }
            buffer.finish_replay(env.local_db(), env.local_qp()).get();
            auto data = buffer.take_replayed_group_entries(gid);
            buffer.stop().get();
            return data;
        };
        const auto persisted = [&] {
            return service::strong_consistency::raft_groups_storage::load_descriptor(
                    env.local_qp(), gid, this_shard_id()).get();
        };

        const auto tail = index_range(replay_floor + raft::index_t{1}, replay_tail_end);
        const std::vector<replayed_batch> old_segments = {
            {old_segment_id, raft::index_t{0}, index_range(raft::index_t{1}, replay_floor)},
            {old_segment_id, raft::index_t{0}, index_range(replay_floor + raft::index_t{1},
                    raft::index_t{8})},
            {old_segment_id, replay_floor, index_range(raft::index_t{9}, replay_tail_end)},
        };
        const auto expected_tail = log_shape(raft::log_entries(tail.begin(), tail.end()));

        // The row bootstrap() writes before the group's first batch. Replay discards a
        // group this shard persists nothing about, so without it there is nothing to
        // recover here.
        service::strong_consistency::raft_groups_storage::store_descriptor(
                env.local_qp(), gid, this_shard_id(), raft::index_t(0), raft::term_t(0),
                raft::configuration{}, {}).get();

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

        // The same with the rewrite still on disk, which is what a crash in that window
        // leaves: the rewrite is force-synced into a segment main.cc never deletes. Both
        // the old segments and the rewrite carry 6..10, and the rewrite's copies arrive
        // last: superseded_by() reads the equal terms as a second copy and keeps the
        // buffer, then the duplicate check drops the rewrite's copies.
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
// drop_truncated_copies() call in add_batch() or the cursor seeding in
// resolve_group(): the second pass then returns v=106..108 at pk 6..8. Deleting
// the truncation_record loop in add_batch() fails the first pass's assertion.
SEASTAR_TEST_CASE(test_second_replay_drops_the_copies_the_first_one_superseded) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        // Two tables: `d` carries the mutations and is read back through the
        // ordinary path, `g` only makes the group id resolve and takes the
        // rewritten tablet map, leaving `d`'s metadata as CQL made it.
        env.execute_cql("create table ks.d (pk int primary key, v int)").get();
        env.execute_cql("create table ks.g (pk int primary key)").get();
        const auto schema = env.local_db().find_schema("ks", "d");
        const auto gid = make_group_id();
        set_sc_tablet_metadata(env, env.local_db().find_schema("ks", "g")->id(), gid,
                locator::tablet_replica_set{
                        {env.local_db().get_token_metadata().get_my_id(), this_shard_id()}}).get();

        // `d`'s tablet must have a replica on this shard: apply_committed() writes
        // to the memtable of the shard replay runs on. Only the first tablet table
        // created here is sure of a replica on this shard, so `d` is created first.
        // Asserted, so reordering the CREATEs fails here, not in table::apply().
        const auto token_metadata = env.local_db().get_shared_token_metadata().get();
        const auto& tablets = token_metadata->tablets().get_tablet_map(schema->id());
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
            const auto [value_base, timestamp] = term == term1
                    ? std::pair(term1_value_base, ts_term1)
                    : std::pair(term2_value_base, ts_term2);
            std::vector<raft::log_entry_ptr> batch;
            for (int i = from; i <= to; ++i) {
                batch.push_back(make_mutation_entry(schema, term, raft::index_t(i), i, value_base + i, timestamp));
            }
            return batch;
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
                buffer.add_batch(env.local_db(), env.local_qp(), env.get_system_keyspace().local(),
                        gid, batch.segment, batch.commit_idx, batch.entries).get();
            }
            buffer.finish_replay(env.local_db(), env.local_qp()).get();
            auto data = buffer.take_replayed_group_entries(gid);
            buffer.stop().get();
            return data;
        };
        const auto persisted = [&] {
            return service::strong_consistency::raft_groups_storage::load_descriptor(
                    env.local_qp(), gid, this_shard_id()).get();
        };

        // 1..5 stay the term-1 copies, 6..10 the term-2 ones. 11 is uncommitted:
        // with_rows_ignore_order() rejects extra rows, so its absence is checked too.
        std::vector<std::vector<bytes_opt>> committed_rows;
        for (int i = 1; i <= 10; ++i) {
            committed_rows.push_back({int32_type->decompose(i),
                    int32_type->decompose(i <= 5 ? term1_value_base + i : term2_value_base + i)});
        }
        const auto require_committed_rows = [&] {
            assert_that(env.execute_cql("select pk, v from ks.d").get())
                    .is_rows().with_rows_ignore_order(committed_rows);
        };
        // The uncommitted tail, as the group must get it back from either pass.
        const auto expected_tail = sstring("2:11");
        // What the first pass must mint: the term-1 copies of 6..8, in their segment.
        const std::vector<service::strong_consistency::truncation_record> expected_truncations{
                {.segment = seg_term1, .from = raft::index_t{6}, .to = raft::index_t{8}}};

        // The row bootstrap() writes before the group's first batch. Replay discards a
        // group this shard persists nothing about, so without it there is nothing to
        // recover here.
        service::strong_consistency::raft_groups_storage::store_descriptor(
                env.local_qp(), gid, this_shard_id(), raft::index_t(0), raft::term_t(0),
                raft::configuration{}, {}).get();

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

// Test: replay persists the configuration a committed entry carried (SCYLLADB-3842).
//
// The configuration reaches the row through note_committed(), so a replica that restarts
// after a membership change comes back on the membership raft committed, not the one the
// row was last written with. A configuration above the commit index is not committed, so
// it waits in the recovered tail.
SEASTAR_TEST_CASE(test_replay_persists_the_committed_configuration) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        const auto table = table_id(utils::UUID_gen::get_time_UUID());
        const auto my_id = env.local_db().get_token_metadata().get_my_id();
        const auto gid = make_group_id();
        service::strong_consistency::raft_groups_storage::store_descriptor(
                env.local_qp(), gid, this_shard_id(), raft::index_t(0), raft::term_t(0),
                raft::configuration{}, {}).get();
        set_sc_tablet_metadata(env, table, gid,
                locator::tablet_replica_set{{my_id, this_shard_id()}}).get();

        const auto committed_config = make_config_entry(raft::term_t(1), raft::index_t(2));
        const auto uncommitted_config = make_config_entry(raft::term_t(1), raft::index_t(5));

        db::raft_commitlog_replay_buffer buffer;
        // 1..3 are committed, so the configuration at 2 is the one raft agreed on.
        buffer.add_batch(env.local_db(), env.local_qp(), env.get_system_keyspace().local(),
                gid, 1, raft::index_t(3),
                std::vector<raft::log_entry_ptr>{
                        make_dummy_entry(raft::term_t(1), raft::index_t(1)),
                        committed_config,
                        make_dummy_entry(raft::term_t(1), raft::index_t(3))}).get();
        buffer.add_batch(env.local_db(), env.local_qp(), env.get_system_keyspace().local(),
                gid, 2, raft::index_t(3),
                std::vector<raft::log_entry_ptr>{
                        make_dummy_entry(raft::term_t(1), raft::index_t(4)),
                        uncommitted_config}).get();
        buffer.finish_replay(env.local_db(), env.local_qp()).get();

        const auto persisted = service::strong_consistency::raft_groups_storage::load_descriptor(
                env.local_qp(), gid, this_shard_id()).get();
        BOOST_REQUIRE_EQUAL(persisted.idx, raft::index_t(3));
        BOOST_REQUIRE(persisted.config.current
                == std::get<raft::configuration>(committed_config->data).current);
        BOOST_REQUIRE(persisted.config.current
                != std::get<raft::configuration>(uncommitted_config->data).current);
        // The uncommitted configuration is in the tail the group restarts on.
        auto data = buffer.take_replayed_group_entries(gid);
        BOOST_REQUIRE_EQUAL(log_shape(data.entries), "1:4,1:5");
        buffer.stop().get();
    }, sc_replay_config());
}

// Test: a committed configuration below the floor leaves the floor's own configuration
// in place. The floor carries the configuration raft had agreed on when the descriptor
// was written, so an older copy still on disk must not replace it.
SEASTAR_TEST_CASE(test_replay_keeps_the_floors_configuration_over_an_older_copy) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        const auto table = table_id(utils::UUID_gen::get_time_UUID());
        const auto my_id = env.local_db().get_token_metadata().get_my_id();
        const auto gid = make_group_id();
        const auto floor_config = raft::configuration{{raft::config_member{
                raft::server_address{raft::server_id::create_random_id(), {}}, raft::is_voter::yes}}};
        service::strong_consistency::raft_groups_storage::store_descriptor(
                env.local_qp(), gid, this_shard_id(), raft::index_t(10), raft::term_t(1),
                floor_config, {}).get();
        set_sc_tablet_metadata(env, table, gid,
                locator::tablet_replica_set{{my_id, this_shard_id()}}).get();

        const auto older_config = make_config_entry(raft::term_t(1), raft::index_t(5));
        const auto add = [&](db::segment_id_type segment, raft::index_t commit_idx,
                std::vector<raft::log_entry_ptr> entries, db::raft_commitlog_replay_buffer& buffer) {
            buffer.add_batch(env.local_db(), env.local_qp(), env.get_system_keyspace().local(),
                    gid, segment, commit_idx, entries).get();
        };

        db::raft_commitlog_replay_buffer buffer;
        // A segment older than the floor still holds the configuration at 5.
        add(1, raft::index_t(0), {older_config}, buffer);
        // 11 and 12 are committed by the header of the batch after them, which raises the
        // floor to 12 so that the descriptor is written at all.
        add(2, raft::index_t(10), {make_dummy_entry(raft::term_t(1), raft::index_t(11)),
                make_dummy_entry(raft::term_t(1), raft::index_t(12))}, buffer);
        add(3, raft::index_t(12), {make_dummy_entry(raft::term_t(1), raft::index_t(13)),
                make_dummy_entry(raft::term_t(1), raft::index_t(14))}, buffer);
        buffer.finish_replay(env.local_db(), env.local_qp()).get();

        const auto persisted = service::strong_consistency::raft_groups_storage::load_descriptor(
                env.local_qp(), gid, this_shard_id()).get();
        BOOST_REQUIRE_EQUAL(persisted.idx, raft::index_t(12));
        BOOST_REQUIRE(persisted.config.current == floor_config.current);
        BOOST_REQUIRE(persisted.config.current
                != std::get<raft::configuration>(older_config->data).current);
        buffer.stop().get();
    }, sc_replay_config());
}

// Test: commit_term is the term of the entry at the floor, even when a copy of a lower
// committed index is read after it. raft reads snapshot_term as the term at snapshot_idx,
// for the election check and for log matching at the snapshot boundary, so a lower index's
// term paired with the floor's index makes both answer on the wrong term.
SEASTAR_TEST_CASE(test_replay_takes_the_commit_term_from_the_entry_at_the_floor) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        const auto table = table_id(utils::UUID_gen::get_time_UUID());
        const auto my_id = env.local_db().get_token_metadata().get_my_id();
        const auto gid = make_group_id();
        const auto floor_config = raft::configuration{{raft::config_member{
                raft::server_address{raft::server_id::create_random_id(), {}}, raft::is_voter::yes}}};
        service::strong_consistency::raft_groups_storage::store_descriptor(
                env.local_qp(), gid, this_shard_id(), raft::index_t(5), raft::term_t(1),
                floor_config, {}).get();
        set_sc_tablet_metadata(env, table, gid,
                locator::tablet_replica_set{{my_id, this_shard_id()}}).get();

        const auto add = [&](db::segment_id_type segment, raft::index_t commit_idx,
                std::vector<raft::log_entry_ptr> entries, db::raft_commitlog_replay_buffer& buffer) {
            buffer.add_batch(env.local_db(), env.local_qp(), env.get_system_keyspace().local(),
                    gid, segment, commit_idx, entries).get();
        };

        db::raft_commitlog_replay_buffer buffer;
        // 6 to 10 in term 3 raise the floor from 5 to 10, so term 3 is the term at the floor.
        add(1, raft::index_t(10), {make_dummy_entry(raft::term_t(3), raft::index_t(6)),
                make_dummy_entry(raft::term_t(3), raft::index_t(7)),
                make_dummy_entry(raft::term_t(3), raft::index_t(8)),
                make_dummy_entry(raft::term_t(3), raft::index_t(9)),
                make_dummy_entry(raft::term_t(3), raft::index_t(10))}, buffer);
        // A later segment still holds the copy of 7 that term 2 wrote. Every entry of the
        // batch before it was committed and drained, so the buffer is empty and the dedupe
        // against the buffered tail does not run. The copy reaches note_committed().
        add(2, raft::index_t(10), {make_dummy_entry(raft::term_t(2), raft::index_t(7))}, buffer);
        buffer.finish_replay(env.local_db(), env.local_qp()).get();

        const auto persisted = service::strong_consistency::raft_groups_storage::load_descriptor(
                env.local_qp(), gid, this_shard_id()).get();
        BOOST_REQUIRE_EQUAL(persisted.idx, raft::index_t(10));
        BOOST_REQUIRE_EQUAL(persisted.term, raft::term_t(3));
        buffer.stop().get();
    }, sc_replay_config());
}

BOOST_AUTO_TEST_SUITE_END()
