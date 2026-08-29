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

namespace service::strong_consistency {

logging::logger rgslog("raft_groups_storage");

namespace {

// Serialize a raft::configuration for the system.raft_groups snapshot_config
// cell, using the IDL serializer the log entries' data variant already uses
// (idl/raft_storage.idl.hh).
bytes serialize_config(const raft::configuration& config) {
    bytes_ostream out;
    ser::serialize(out, config);
    return bytes(out.linearize());
}

raft::configuration deserialize_config(const bytes& blob) {
    auto in = ser::as_input_stream(blob);
    return ser::deserialize(in, std::type_identity<raft::configuration>());
}

} // namespace

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
    return execute_with_linearization_point([this, idx] {
        static const auto store_cql = format("INSERT INTO system.{} (shard, group_id, commit_idx) VALUES (?, ?, ?)",
            db::system_keyspace::RAFT_GROUPS);
        return _qp.execute_internal(
            store_cql,
            {int16_t(_shard), _group_id.id, int64_t(idx.value())},
            cql3::query_processor::cache_internal::yes).discard_result();
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
    // The whole descriptor is in the group's own row: one read, and the index,
    // the term and the configuration are always the ones that were written
    // together.
    static const auto load_cql = format(
            "SELECT snapshot_id, snapshot_idx, snapshot_term, snapshot_config FROM system.{} "
            "WHERE shard = ? AND group_id = ? LIMIT 1",
            db::system_keyspace::RAFT_GROUPS);
    ::shared_ptr<cql3::untyped_result_set> rs = co_await _qp.execute_internal(load_cql,
            {int16_t(_shard), _group_id.id}, cql3::query_processor::cache_internal::yes);
    if (rs->empty() || !rs->one().has("snapshot_id")) {
        co_return raft::snapshot_descriptor();
    }
    const auto& row = rs->one();
    raft::snapshot_descriptor s{
        .idx = raft::index_t(row.get_or<int64_t>("snapshot_idx", 0)),
        .term = raft::term_t(row.get_or<int64_t>("snapshot_term", 0)),
        .id = raft::snapshot_id(row.get_as<utils::UUID>("snapshot_id"))};
    if (row.has("snapshot_config")) {
        s.config = deserialize_config(row.get_blob_unfragmented("snapshot_config"));
    }
    co_return s;
}

future<> raft_groups_storage::store_snapshot_descriptor(const raft::snapshot_descriptor& snap, size_t preserve_log_entries) {
    return execute_with_linearization_point([this, &snap, preserve_log_entries] () -> future<> {
        // One row, one statement: index, term, configuration and id are written
        // together, so a reader cannot see a mismatched pair.
        static const auto store_cql = format(
                "INSERT INTO system.{} (shard, group_id, snapshot_id, snapshot_idx, snapshot_term, snapshot_config) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                db::system_keyspace::RAFT_GROUPS);
        co_await _qp.execute_internal(store_cql,
                {int16_t(_shard), _group_id.id, snap.id.id, int64_t(snap.idx.value()), int64_t(snap.term.value()),
                 data_value(serialize_config(snap.config))},
                cql3::query_processor::cache_internal::yes);

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


future<> raft_groups_storage::store_snapshot_index(cql3::query_processor& qp, raft::group_id gid, shard_id shard, const raft::snapshot_descriptor& snap) {
    // Guard against repeated replays (e.g. a crash after writing but before the
    // raft groups start): only advance the index, never go backwards.
    static const auto load_cql = format("SELECT snapshot_idx FROM system.{} WHERE shard = ? AND group_id = ? LIMIT 1",
            db::system_keyspace::RAFT_GROUPS);
    auto rs = co_await qp.execute_internal(load_cql, {int16_t(shard), gid.id}, cql3::query_processor::cache_internal::yes);
    if (!rs->empty() && rs->one().has("snapshot_idx")) {
        if (raft::index_t(rs->one().get_as<int64_t>("snapshot_idx")) >= snap.idx) {
            co_return;
        }
    }
    static const auto store_cql = format(
            "INSERT INTO system.{} (shard, group_id, snapshot_id, snapshot_idx, snapshot_term) VALUES (?, ?, ?, ?, ?)",
            db::system_keyspace::RAFT_GROUPS);
    co_await qp.execute_internal(store_cql,
            {int16_t(shard), gid.id, snap.id.id, int64_t(snap.idx.value()), int64_t(snap.term.value())},
            cql3::query_processor::cache_internal::yes);
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
