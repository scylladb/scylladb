/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "service/strong_consistency/raft_groups_storage.hh"

#include "cql3/untyped_result_set.hh"
#include "db/system_keyspace.hh"
#include "raft/raft.hh"
#include "utils/UUID.hh"
#include "utils/log.hh"

#include "serializer.hh"
#include "idl/raft_storage.dist.hh"
#include "serializer_impl.hh"
#include "idl/raft_storage.dist.impl.hh"

#include "cql3/query_processor.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include "mutation/mutation.hh"
#include "mutation/timestamp.hh"
#include "replica/database.hh"
#include "service/storage_proxy.hh"
#include "types/types.hh"
#include "utils/assert.hh"

namespace service::strong_consistency {

logging::logger rgslog("raft_groups_storage");

static mutation make_commit_idx_mutation(shard_id shard, raft::group_id group_id, raft::index_t commit_idx, api::timestamp_type ts);

raft_groups_storage::raft_groups_storage(cql3::query_processor& qp, raft::group_id gid, raft::server_id server_id, shard_id shard, db::commitlog& commit_log,
        table_id target_table_id, replayed_data_per_group replayed_data)
    : _raft_commitlog(gid, commit_log, target_table_id, std::move(replayed_data))
    , _group_id(std::move(gid))
    , _server_id(std::move(server_id))
    , _qp(qp)
    , _pending_op_fut(make_ready_future<>())
{
    rgslog.trace("Creating raft_groups_storage for group_id={}, server_id={}, shard={}", _group_id, _server_id, _shard);
    if (shard > std::numeric_limits<int16_t>::max()) {
        // The shard should fit in int16_t since that's the column type (smallint) we use in the Raft tables
        on_internal_error(rgslog, fmt::format("Shard value {} exceeds maximum allowed {}", shard, std::numeric_limits<int16_t>::max()));
    }
    _shard = static_cast<uint16_t>(shard);
}

future<> raft_groups_storage::store_term_and_vote(raft::term_t term, raft::server_id vote) {
    return execute_with_linearization_point([this, term, vote] {
        static const auto store_cql = format("INSERT INTO system.{} (shard, group_id, vote_term, vote) VALUES (?, ?, ?, ?)",
            db::system_keyspace::RAFT_GROUPS);
        return _qp.execute_internal(
            store_cql,
            {int16_t(_shard), _group_id.id, int64_t(term.value()), vote.id}, cql3::query_processor::cache_internal::yes).discard_result();
    });
}

future<std::pair<raft::term_t, raft::server_id>> raft_groups_storage::load_term_and_vote() {
    static const auto load_cql = format("SELECT vote_term, vote FROM system.{} WHERE shard = ? AND group_id = ? LIMIT 1", db::system_keyspace::RAFT_GROUPS);
    ::shared_ptr<cql3::untyped_result_set> rs = co_await _qp.execute_internal(load_cql, {int16_t(_shard), _group_id.id}, cql3::query_processor::cache_internal::yes);
    if (rs->empty()) {
        co_return std::pair(raft::term_t(), raft::server_id());
    }
    const auto& static_row = rs->one();
    raft::term_t vote_term = raft::term_t(static_row.get_or<int64_t>("vote_term", raft::term_t{}.value()));
    raft::server_id vote{static_row.get_or<utils::UUID>("vote", raft::server_id{}.id)};
    co_return std::pair(vote_term, vote);
}

future<> raft_groups_storage::store_commit_idx(raft::index_t idx) {
    if (_batcher) {
        // The io_fiber co_awaits this before pushing committed entries to the
        // applier fiber, so resolving only after the round's forced sync keeps
        // raft's persist-before-apply ordering — batched across all groups on
        // the shard instead of one synced write per group per commit.
        return _batcher->submit_commit_idx(_group_id, idx).then([this, idx] {
            _raft_commitlog.release_dummy_rp_handles(idx);
        });
    }
    // No batcher wired (unit tests): persist through the same representation a
    // round would produce — one force-synced commit_idx entry whose rp_handle
    // is attached to a fake system.raft_groups mutation. Single format either
    // way; no CQL write path exists for commit_idx.
    return execute_with_linearization_point([this, idx] () -> future<> {
        utils::chunked_vector<commitlog_raft_log_entry_writer> writers;
        writers.emplace_back(db::system_keyspace::raft_groups()->id(),
                raft_commit_idx_entry{.group_id = _group_id, .commit_idx = idx});
        auto handles = co_await _raft_commitlog.commit_log().add_raft_entries(std::move(writers));
        SCYLLA_ASSERT(handles.size() == 1);
        auto m = make_commit_idx_mutation(_shard, _group_id, idx, api::new_timestamp());
        auto& db = _qp.proxy().local_db();
        auto& cf = db.find_column_family(m.schema()->id());
        co_await db.apply_in_memory(m, cf, std::move(handles[0]), db::no_timeout);
        // Dummy entries at or below commit_idx are committed and covered on
        // restart — raft won't replay them, and a dummy carries no state.
        // (Configuration entries are deliberately kept: see
        // release_dummy_rp_handles() and SCYLLADB-3842.)
        _raft_commitlog.release_dummy_rp_handles(idx);
    });
}

future<raft::index_t> raft_groups_storage::load_commit_idx() {
    return load_commit_idx(_qp, _group_id, _shard);
}

future<raft::index_t> raft_groups_storage::load_commit_idx(cql3::query_processor& qp, raft::group_id gid, shard_id shard) {
    static const auto load_cql = format("SELECT commit_idx FROM system.{} WHERE shard = ? AND group_id = ? LIMIT 1", db::system_keyspace::RAFT_GROUPS);
    ::shared_ptr<cql3::untyped_result_set> rs = co_await qp.execute_internal(load_cql, {int16_t(shard), gid.id}, cql3::query_processor::cache_internal::yes);
    if (rs->empty()) {
        co_return raft::index_t(0);
    }
    const auto& static_row = rs->one();
    co_return raft::index_t(static_row.get_or<int64_t>("commit_idx", raft::index_t{}.value()));
}

future<raft::log_entries> raft_groups_storage::load_log() {
    return make_ready_future<raft::log_entries>(_raft_commitlog.load_log());
}

future<raft::snapshot_descriptor> raft_groups_storage::load_snapshot_descriptor() {
    static const auto load_id_cql = format("SELECT snapshot_id FROM system.{} WHERE shard = ? AND group_id = ? LIMIT 1", db::system_keyspace::RAFT_GROUPS);
    ::shared_ptr<cql3::untyped_result_set> id_rs = co_await _qp.execute_internal(load_id_cql, {int16_t(_shard), _group_id.id}, cql3::query_processor::cache_internal::yes);
    if (id_rs->empty() || !id_rs->one().has("snapshot_id")) {
        co_return raft::snapshot_descriptor();
    }
    const auto& id_row = id_rs->one(); // should be only one row since snapshot_id column is static
    utils::UUID snapshot_id = id_row.get_as<utils::UUID>("snapshot_id");

    // Fetch raft log index and term for the latest snapshot descriptor
    static const auto load_snp_info_cql = format("SELECT idx, term FROM system.{} WHERE shard = ? AND group_id = ?",
        db::system_keyspace::RAFT_GROUPS_SNAPSHOTS);
    ::shared_ptr<cql3::untyped_result_set> snp_rs = co_await _qp.execute_internal(load_snp_info_cql, {int16_t(_shard), _group_id.id}, cql3::query_processor::cache_internal::yes);
    // Should be only one matching row, since each individual server can only
    // have a single snapshot installed at a time
    const auto& snp_row = snp_rs->one();
    // Fetch current and previous raft configurations for the snapshot
    static const auto load_cfg_cql = format("SELECT disposition, server_id, can_vote FROM system.{} WHERE shard = ? AND group_id = ?", db::system_keyspace::RAFT_GROUPS_SNAPSHOT_CONFIG);
    ::shared_ptr<cql3::untyped_result_set> cfg_rs = co_await _qp.execute_internal(load_cfg_cql, {int16_t(_shard), _group_id.id}, cql3::query_processor::cache_internal::yes);

    raft::configuration cfg;

    for (const cql3::untyped_result_set_row& row : *cfg_rs) {
        const auto disposition = row.get_as<sstring>("disposition");
        auto& cfg_part = disposition == "CURRENT" ? cfg.current : cfg.previous;
        cfg_part.insert(
            raft::config_member{
                raft::server_address{raft::server_id{row.get_as<utils::UUID>("server_id")}, {}},
                raft::is_voter(row.get_as<bool>("can_vote"))}
        );
    }

    raft::snapshot_descriptor s{
        .idx = raft::index_t(snp_row.get_as<int64_t>("idx")),
        .term = raft::term_t(snp_row.get_as<int64_t>("term")),
        .config = std::move(cfg),
        .id = raft::snapshot_id(snapshot_id)};
    co_return s;
}

future<> raft_groups_storage::store_snapshot_descriptor(const raft::snapshot_descriptor& snap, size_t preserve_log_entries) {
    // TODO: check that snap.idx refers to an already persisted entry
    return execute_with_linearization_point([this, &snap, preserve_log_entries] () -> future<> {
        static const auto store_snp_cql = format("INSERT INTO system.{} (shard, group_id, snapshot_id, idx, term) VALUES (?, ?, ?, ?, ?)",
            db::system_keyspace::RAFT_GROUPS_SNAPSHOTS);
        co_await _qp.execute_internal(
            store_snp_cql,
            {int16_t(_shard), _group_id.id, snap.id.id, int64_t(snap.idx.value()), int64_t(snap.term.value())},
            cql3::query_processor::cache_internal::yes
        );
        // remove old configs
        static const auto delete_raft_cfg_cql = format("DELETE FROM system.{} WHERE shard = ? AND group_id = ?", db::system_keyspace::RAFT_GROUPS_SNAPSHOT_CONFIG);
        co_await _qp.execute_internal(delete_raft_cfg_cql, {int16_t(_shard), _group_id.id}, cql3::query_processor::cache_internal::yes);
        // store current and previous raft configurations
        static const auto store_raft_cfg_cql = format("INSERT INTO system.{} (shard, group_id, disposition, server_id, can_vote) VALUES (?, ?, ?, ?, ?)",
            db::system_keyspace::RAFT_GROUPS_SNAPSHOT_CONFIG);
        for (const raft::config_member& srv : snap.config.current) {
            co_await _qp.execute_internal(store_raft_cfg_cql,
                {int16_t(_shard), _group_id.id, "CURRENT", srv.addr.id.id, srv.can_vote},
                    cql3::query_processor::cache_internal::yes);
        }
        for (const raft::config_member& srv : snap.config.previous) {
            co_await _qp.execute_internal(store_raft_cfg_cql,
                {int16_t(_shard), _group_id.id, "PREVIOUS", srv.addr.id.id, srv.can_vote},
                    cql3::query_processor::cache_internal::yes);
        }

        co_await update_snapshot(snap);
        // Release replay position handles for entries covered by the snapshot.
        // state_machine::apply() only acquires handles for command entries;
        // configuration and dummy entries retain their handles in the map
        // and are cleaned up here.
        raft::index_t log_tail_index(snap.idx.value() - preserve_log_entries);
        _raft_commitlog.truncate_log_tail(log_tail_index);
    });
}

future<> raft_groups_storage::store_log_entries(const std::vector<raft::log_entry_ptr>& entries) {
    return _raft_commitlog.store_log_entries(entries);
}

future<> raft_groups_storage::truncate_log(raft::index_t idx) {
    _raft_commitlog.truncate_log(idx);
    return make_ready_future<>();
}

future<> raft_groups_storage::abort() {
    // wait for pending write requests to complete.
    // TODO: should we wait for all kinds of requests?
    return std::move(_pending_op_fut);
}

future<> raft_groups_storage::update_snapshot(const raft::snapshot_descriptor &snap) {
    static const auto update_snapshot_cql = format(
        "INSERT INTO system.{} (shard, group_id, snapshot_id) VALUES (?, ?, ?)",
        db::system_keyspace::RAFT_GROUPS);
    return _qp.execute_internal(
        update_snapshot_cql,
        {int16_t(_shard), _group_id.id, snap.id.id},
        cql3::query_processor::cache_internal::yes
    ).discard_result();
}

future<> raft_groups_storage::store_snapshot_index(cql3::query_processor& qp, raft::group_id gid, shard_id shard, const raft::snapshot_descriptor& snap) {
    // Guard against repeated replays (e.g., crash after writing but before raft
    // groups start): only advance the snapshot index, never go backwards.
    static const auto load_snp_idx_cql = format("SELECT idx FROM system.{} WHERE shard = ? AND group_id = ?",
        db::system_keyspace::RAFT_GROUPS_SNAPSHOTS);
    auto rs = co_await qp.execute_internal(load_snp_idx_cql, {int16_t(shard), gid.id}, cql3::query_processor::cache_internal::yes);
    if (!rs->empty() && rs->one().has("idx")) {
        auto existing_idx = raft::index_t(static_cast<uint64_t>(rs->one().get_as<int64_t>("idx")));
        if (existing_idx >= snap.idx) {
            co_return;
        }
    }

    // Update both tables atomically so a crash between writes cannot leave
    // an inconsistent snapshot_id reference.
    static const auto store_snapshot_batch_cql = format(
        "BEGIN UNLOGGED BATCH"
        "   INSERT INTO system.{} (shard, group_id, snapshot_id, idx, term) VALUES (?, ?, ?, ?, ?);"
        "   INSERT INTO system.{} (shard, group_id, snapshot_id) VALUES (?, ?, ?);"
        "APPLY BATCH",
        db::system_keyspace::RAFT_GROUPS_SNAPSHOTS, db::system_keyspace::RAFT_GROUPS);
    co_await qp.execute_internal(
        store_snapshot_batch_cql,
        {int16_t(shard), gid.id, snap.id.id, int64_t(snap.idx.value()), int64_t(snap.term.value()),
         int16_t(shard), gid.id, snap.id.id},
        cql3::query_processor::cache_internal::yes
    );
}

future<> raft_groups_storage::execute_with_linearization_point(std::function<future<>()> f) {
    promise<> task_promise;
    auto pending_fut = std::exchange(_pending_op_fut, task_promise.get_future());
    co_await std::move(pending_fut);
    try {
        co_await f();
        task_promise.set_value();
    } catch (...) {
        task_promise.set_exception(std::current_exception());
        throw;
    }
}

future<> raft_groups_storage::bootstrap(raft::configuration initial_configuation, bool nontrivial_snapshot) {
    auto init_index = nontrivial_snapshot ? raft::index_t{1} : raft::index_t{0};
    raft::snapshot_descriptor snapshot{.idx{init_index}};
    snapshot.id = raft::snapshot_id::create_random_id();
    snapshot.config = std::move(initial_configuation);
    co_await store_snapshot_descriptor(snapshot, 0);
}

std::vector<index_and_replay_position> raft_groups_storage::acquire_replay_position_handles_for(const raft::log_entry_ptr_list& entries) {
    return _raft_commitlog.acquire_replay_position_handles_for(entries);
}


future<> raft_groups_storage::store_commit_idx_if_higher(cql3::query_processor& qp, raft::group_id gid, shard_id shard, raft::index_t commit_idx) {
    // Only advance, never regress: an earlier replay may already have
    // persisted a value at or beyond the recovered one.
    const auto persisted = co_await load_commit_idx(qp, gid, shard);
    if (commit_idx <= persisted) {
        co_return;
    }
    static const auto store_cql = format("INSERT INTO system.{} (shard, group_id, commit_idx) VALUES (?, ?, ?)",
        db::system_keyspace::RAFT_GROUPS);
    co_await qp.execute_internal(
        store_cql,
        {int16_t(shard), gid.id, int64_t(commit_idx.value())},
        cql3::query_processor::cache_internal::yes).discard_result();
}

future<std::pair<raft::index_t, raft::term_t>>
raft_groups_storage::load_snapshot_idx_and_term(cql3::query_processor& qp, raft::group_id gid, shard_id shard) {
    static const auto load_cql = format("SELECT idx, term FROM system.{} WHERE shard = ? AND group_id = ?",
        db::system_keyspace::RAFT_GROUPS_SNAPSHOTS);
    auto rs = co_await qp.execute_internal(load_cql, {int16_t(shard), gid.id}, cql3::query_processor::cache_internal::yes);
    if (rs->empty()) {
        co_return std::pair(raft::index_t(0), raft::term_t(0));
    }
    const auto& row = rs->one();
    co_return std::pair(
            raft::index_t(row.get_or<int64_t>("idx", 0)),
            raft::term_t(row.get_or<int64_t>("term", 0)));
}

// ============================ sc_io_batcher ================================

// Build a static-only system.raft_groups mutation that sets commit_idx for the
// (shard, group_id) partition. Applied in-memory with the round's commit_idx
// entry rp_handle attached, so the value rides the raft_groups memtable to an
// SSTable before the segment holding the entry can be recycled.
static mutation make_commit_idx_mutation(shard_id shard, raft::group_id group_id, raft::index_t commit_idx, api::timestamp_type ts) {
    auto schema = db::system_keyspace::raft_groups();
    auto pk = partition_key::from_exploded(*schema, {
        short_type->decompose(int16_t(shard)),
        timeuuid_type->decompose(group_id.id),
    });
    mutation m(schema, std::move(pk));
    m.set_static_cell("commit_idx", data_value(int64_t(commit_idx.value())), ts);
    return m;
}

sc_io_batcher::sc_io_batcher(cql3::query_processor& qp, db::commitlog& cl)
    : _qp(qp)
    , _commitlog(cl) {
}

void sc_io_batcher::start() {
    (void)with_gate(_gate, [this] { return run(); });
}

future<> sc_io_batcher::stop() {
    _stopping = true;
    _cv.signal();
    co_await _gate.close();
    rgslog.info("sc_io_batcher: {} rounds, {} append calls, {} entries, {} commit values"
            " (avg appends/round {:.2f}, avg entries/round {:.2f})",
            _rounds, _append_calls, _entries, _commit_values,
            _rounds ? double(_append_calls) / double(_rounds) : 0.0,
            _rounds ? double(_entries) / double(_rounds) : 0.0);
}

future<utils::chunked_vector<db::rp_handle>> sc_io_batcher::submit_append(utils::chunked_vector<commitlog_raft_log_entry_writer> writers) {
    if (_stopping) {
        return make_exception_future<utils::chunked_vector<db::rp_handle>>(gate_closed_exception());
    }
    _appends.emplace_back(append_item{.writers = std::move(writers)});
    auto f = _appends.back().done.get_future();
    _cv.signal();
    return f;
}

future<> sc_io_batcher::submit_commit_idx(raft::group_id gid, raft::index_t idx) {
    if (_stopping) {
        return make_exception_future<>(gate_closed_exception());
    }
    auto [it, inserted] = _pending_commit.try_emplace(gid, idx);
    if (!inserted) {
        // commit_idx is a watermark: superseded values carry no information.
        it->second = std::max(it->second, idx);
    }
    _cv.signal();
    return _round_done.get_shared_future();
}

future<> sc_io_batcher::run() {
    while (true) {
        co_await _cv.when([this] { return !_appends.empty() || !_pending_commit.empty() || _stopping; });
        if (_stopping) {
            break;
        }
        // Snapshot: everything accumulated so far is THIS round; arrivals during
        // the write go to the next one.
        auto appends = std::exchange(_appends, {});
        std::vector<std::pair<raft::group_id, raft::index_t>> commits;
        commits.reserve(_pending_commit.size());
        for (const auto& [gid, idx] : _pending_commit) {
            commits.emplace_back(gid, idx);
        }
        _pending_commit.clear();
        auto commit_done = std::exchange(_round_done, shared_promise<>());

        std::exception_ptr ex;
        try {
            co_await write_round(appends, commits);
        } catch (...) {
            ex = std::current_exception();
        }
        ++_rounds;
        _append_calls += appends.size();
        _commit_values += commits.size();
        if (ex) {
            // Waiters are group io_fibers; the error surfaces per group via
            // raft's background_error.
            for (auto& a : appends) {
                a.done.set_exception(ex);
            }
            commit_done.set_exception(std::move(ex));
        } else {
            commit_done.set_value();
        }
    }
    for (auto& a : _appends) {
        a.done.set_exception(std::make_exception_ptr(gate_closed_exception()));
    }
    _round_done.set_exception(std::make_exception_ptr(gate_closed_exception()));
}

future<> sc_io_batcher::write_round(std::deque<append_item>& appends,
        const std::vector<std::pair<raft::group_id, raft::index_t>>& commits) {
    const auto rg_cf = db::system_keyspace::raft_groups()->id();
    // ONE multi-entry, force-synced commitlog write for the whole shard: every
    // group's data batch plus a commit_idx entry per group whose commit
    // advanced. One sync regardless of how many groups took part.
    utils::chunked_vector<commitlog_raft_log_entry_writer> writers;
    size_t n_append_writers = 0;
    for (auto& a : appends) {
        n_append_writers += a.writers.size();
    }
    writers.reserve(n_append_writers + commits.size());
    for (auto& a : appends) {
        for (auto& w : a.writers) {
            writers.emplace_back(std::move(w));
        }
    }
    for (const auto& [gid, idx] : commits) {
        writers.emplace_back(rg_cf, raft_commit_idx_entry{.group_id = gid, .commit_idx = idx});
    }
    _entries += n_append_writers;

    auto handles = co_await _commitlog.add_raft_entries(std::move(writers));
    SCYLLA_ASSERT(handles.size() == n_append_writers + commits.size());

    // Hand each append item its slice, in submission order.
    size_t i = 0;
    for (auto& a : appends) {
        utils::chunked_vector<db::rp_handle> slice;
        slice.reserve(a.writers.size());
        for (size_t k = 0; k < a.writers.size(); ++k) {
            slice.emplace_back(std::move(handles[i++]));
        }
        a.done.set_value(std::move(slice));
    }

    // Commit values are durable. Attach each handle to a fake
    // system.raft_groups mutation so the value also reaches an SSTable before
    // its segment can be recycled (memory-only applies; nothing here allocates
    // commitlog space).
    auto& db = _qp.proxy().local_db();
    auto& cf = db.find_column_family(rg_cf);
    for (const auto& [gid, idx] : commits) {
        auto m = make_commit_idx_mutation(this_shard_id(), gid, idx, api::new_timestamp());
        co_await db.apply_in_memory(m, cf, std::move(handles[i++]), db::no_timeout);
    }
}

} // namespace service::strong_consistency
