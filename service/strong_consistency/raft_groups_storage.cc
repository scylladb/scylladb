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
#include "mutation/mutation.hh"
#include "mutation/timestamp.hh"
#include "replica/database.hh"
#include "service/storage_proxy.hh"
#include "types/types.hh"

#include <seastar/core/coroutine.hh>

#include <algorithm>

namespace service::strong_consistency {

logging::logger rgslog("raft_groups_storage");

raft_groups_storage::raft_groups_storage(cql3::query_processor& qp, raft::group_id gid, raft::server_id server_id, shard_id shard, db::commitlog& commit_log,
        table_id target_table_id, replayed_data_per_group replayed_data)
    : _raft_commitlog(gid, commit_log, target_table_id,
            db::system_keyspace::raft_groups()->id(), std::move(replayed_data))
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
    // Update in-memory tracking only. Persistence happens via the fake
    // mutation in store_log_entries (durable once the raft_groups memtable
    // flushes) and via persist_commit_idx() from the SC tablet flush hook
    // (groups_manager::save_commit_log_index). Keeping this path IO-free
    // avoids a per-committed-batch CQL write on the raft io_fiber.
    //
    // The io_fiber calls this *before* pushing entries to the applier_fiber,
    // so _last_known_commit_idx is always >= the raft index of any entry that
    // has been applied to a memtable.
    _last_known_commit_idx = idx;
    return make_ready_future<>();
}

future<> raft_groups_storage::persist_commit_idx() {
    if (_last_known_commit_idx <= _last_persisted_commit_idx) {
        // Nothing new to persist since the last write.
        return make_ready_future<>();
    }
    if (_aborted) {
        // The group is being torn down. abort() deliberately does not persist
        // commit_idx (shutdown may have closed the CQL / storage_proxy gates);
        // durability is instead provided by the fake system.raft_groups mutation
        // applied per batch and the raft_groups memtable flush. A flush hook
        // (groups_manager::save_commit_log_index) racing teardown can still reach
        // here with unpersisted commit_idx — that is expected and harmless, so
        // skip quietly rather than starting a CQL write.
        rgslog.debug("persist_commit_idx skipped after abort for group {}"
            " (unpersisted commit_idx {}, last persisted {})",
            _group_id, _last_known_commit_idx, _last_persisted_commit_idx);
        return make_ready_future<>();
    }
    auto idx = _last_known_commit_idx;
    return execute_with_linearization_point([this, idx] {
        static const auto store_cql = format("INSERT INTO system.{} (shard, group_id, commit_idx) VALUES (?, ?, ?)",
            db::system_keyspace::RAFT_GROUPS);
        return _qp.execute_internal(
            store_cql,
            {int16_t(_shard), _group_id.id, int64_t(idx.value())},
            cql3::query_processor::cache_internal::yes).discard_result().then([this, idx] {
                // store_log_entries()'s fake mutation may have advanced the
                // watermark past idx while our CQL write was in flight; keep it
                // monotonic.
                _last_persisted_commit_idx = std::max(_last_persisted_commit_idx, idx);
                // Non-command entries (configuration, dummy) at or below commit_idx
                // are committed and, now that commit_idx is persisted, are covered
                // by it on restart — raft won't replay them — so release their
                // rp_handles and let the commitlog segments be reclaimed. (Command
                // entries are handled by apply(), which hands their rp_handles to
                // the target memtable.)
                _raft_commitlog.release_noncommand_rp_handles(idx);
            });
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

// Build a static-only system.raft_groups mutation that sets commit_idx for the
// (shard, group_id) partition. Applied in-memory (see store_log_entries) with
// the commit_idx entry's rp_handle so it rides the raft_groups memtable flush.
static mutation make_commit_idx_mutation(shard_id shard, raft::group_id group_id, raft::index_t commit_idx) {
    auto schema = db::system_keyspace::raft_groups();
    auto pk = partition_key::from_exploded(*schema, {
        short_type->decompose(int16_t(shard)),
        timeuuid_type->decompose(group_id.id),
    });
    mutation m(schema, std::move(pk));
    m.set_static_cell("commit_idx",
            data_value(int64_t(commit_idx.value())),
            api::new_timestamp());
    return m;
}

future<> raft_groups_storage::store_log_entries(const std::vector<raft::log_entry_ptr>& entries) {
    const auto commit_idx = _last_known_commit_idx;
    auto commit_idx_entry_handle = co_await _raft_commitlog.store_log_entries(entries, commit_idx);

    if (commit_idx <= _last_persisted_commit_idx) {
        // commit_idx has not advanced since it was last recorded to
        // system.raft_groups — by a prior fake mutation here or by
        // persist_commit_idx() — or is 0 before any commit. Nothing new to
        // record; release the commit_idx entry's rp_handle normally — the batch's
        // raft entries are still pinned via _command/_noncommand_positions.
        co_return;
    }

    // raft_commitlog::store_log_entries wrote a small commit_idx entry to the
    // commitlog and returned its rp_handle. Build a fake system.raft_groups
    // mutation carrying commit_idx and apply it in-memory with that handle
    // attached: this pins the commitlog segment via the raft_groups memtable and
    // lets commit_idx eventually flush to an SSTable without a synchronous CQL
    // write on the raft io_fiber.
    auto m = make_commit_idx_mutation(_shard, _group_id, commit_idx);
    auto& db = _qp.proxy().local_db();
    auto& cf = db.find_column_family(m.schema()->id());
    co_await db.apply_in_memory(m, cf, std::move(commit_idx_entry_handle), db::no_timeout);
    // Both this path and persist_commit_idx() write commit_idx to the raft_groups
    // memtable; either may run between our co_awaits, so keep the watermark
    // monotonic rather than clobbering a larger value.
    _last_persisted_commit_idx = std::max(_last_persisted_commit_idx, commit_idx);

    // Non-command entries (configuration, dummy) are never applied to a memtable
    // — unlike command entries, whose rp_handles apply() hands to the target
    // memtable. We keep their rp_handles pinning the commitlog only until they
    // commit: non-command entries at or below commit_idx are committed and, now
    // that commit_idx is recorded, are covered by it on restart, so raft won't
    // replay them. Release their handles and let the commitlog segments be
    // reclaimed.
    _raft_commitlog.release_noncommand_rp_handles(commit_idx);
}

future<> raft_groups_storage::truncate_log(raft::index_t idx) {
    _raft_commitlog.truncate_log(idx);
    return make_ready_future<>();
}

future<> raft_groups_storage::abort() {
    // Mark aborted so a flush hook (save_commit_log_index -> persist_commit_idx)
    // racing teardown becomes a no-op instead of running against a group that is
    // being destroyed.
    _aborted = true;
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

} // namespace service::strong_consistency
