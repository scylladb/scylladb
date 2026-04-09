/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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

#include "idl/commitlog.dist.hh"
#include "idl/commitlog.dist.impl.hh"

#include "cql3/query_processor.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "service/storage_proxy.hh"
#include "replica/database.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <algorithm>
#include <limits>
#include <utility>

namespace service::strong_consistency {

logging::logger rgslog("raft_groups_storage");

namespace {

using groups_replay_state_map = std::unordered_map<raft::group_id, raft_groups_storage::group_replay_state>;

void apply_group_replayed_entry(raft_groups_storage::group_replay_state& state, const raft::log_entry& entry) {
    state.log[entry.idx] = make_lw_shared<const raft::log_entry>(entry);
    if (state.replayed_truncate_idx && entry.idx.value() >= state.replayed_truncate_idx->value()) {
        state.replayed_truncate_idx.reset();
    }
    if (!state.replayed_seen_max_idx || state.replayed_seen_max_idx->value() < entry.idx.value()) {
        state.replayed_seen_max_idx = entry.idx;
    }
    if (!state.replayed_max_idx || state.replayed_max_idx->value() < entry.idx.value()) {
        state.replayed_max_idx = entry.idx;
    }
}

void apply_group_replayed_truncate(raft_groups_storage::group_replay_state& state, raft::index_t idx) {
    state.replayed_has_truncate = true;
    state.replayed_truncate_idx = idx;
    state.replayed_truncate_prefix_idx.reset();
    if (!state.replayed_seen_max_idx || state.replayed_seen_max_idx->value() < idx.value()) {
        state.replayed_seen_max_idx = idx;
    }
    auto it = state.log.lower_bound(idx);
    state.log.erase(it, state.log.end());
    if (state.log.empty()) {
        state.replayed_max_idx.reset();
    } else {
        state.replayed_max_idx = state.log.rbegin()->first;
    }
}

} // anonymous namespace

raft_groups_storage::raft_groups_storage(cql3::query_processor& qp, raft::group_id gid, raft::server_id server_id, shard_id shard, db::commitlog* commitlog)
    : _group_id(std::move(gid))
    , _server_id(std::move(server_id))
    , _qp(qp)
    , _dummy_query_state(service::client_state::for_internal_calls(), empty_service_permit())
    , _pending_op_fut(make_ready_future<>())
    , _commitlog(commitlog)
    , _table_id(_qp.db().find_schema("system", db::system_keyspace::RAFT_GROUPS)->id())
{
    rgslog.trace("Creating raft_groups_storage for group_id={}, server_id={}, shard={}", _group_id, _server_id, shard);
    if (shard > std::numeric_limits<int16_t>::max()) {
        on_internal_error(rgslog, fmt::format("Shard value {} exceeds maximum allowed {}", shard, std::numeric_limits<int16_t>::max()));
    }
    _shard = static_cast<uint16_t>(shard);
}

raft_groups_storage::raft_groups_storage(cql3::query_processor& qp, raft::group_id gid, raft::server_id server_id, shard_id shard)
    : raft_groups_storage(qp, std::move(gid), std::move(server_id), shard, qp.proxy().local_db().commitlog()) {
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
    static const auto load_cql = format("SELECT commit_idx FROM system.{} WHERE shard = ? AND group_id = ? LIMIT 1", db::system_keyspace::RAFT_GROUPS);
    ::shared_ptr<cql3::untyped_result_set> rs = co_await _qp.execute_internal(load_cql, {int16_t(_shard), _group_id.id}, cql3::query_processor::cache_internal::yes);
    if (rs->empty()) {
        co_return raft::index_t(0);
    }
    const auto& static_row = rs->one();
    co_return raft::index_t(static_row.get_or<int64_t>("commit_idx", raft::index_t{}.value()));
}

future<raft::log_entries> raft_groups_storage::load_log() {
    raft::log_entries log;
    for (const auto& [idx, entry] : _log) {
        (void) idx;
        log.push_back(entry);
        co_await coroutine::maybe_yield();
    }
    co_return log;
}

future<raft::snapshot_descriptor> raft_groups_storage::load_snapshot_descriptor() {
    static const auto load_id_cql = format("SELECT snapshot_id FROM system.{} WHERE shard = ? AND group_id = ? LIMIT 1", db::system_keyspace::RAFT_GROUPS);
    ::shared_ptr<cql3::untyped_result_set> id_rs = co_await _qp.execute_internal(load_id_cql, {int16_t(_shard), _group_id.id}, cql3::query_processor::cache_internal::yes);
    if (id_rs->empty() || !id_rs->one().has("snapshot_id")) {
        co_return raft::snapshot_descriptor();
    }
    const auto& id_row = id_rs->one();
    utils::UUID snapshot_id = id_row.get_as<utils::UUID>("snapshot_id");

    static const auto load_snp_info_cql = format("SELECT snapshot_id, idx, term FROM system.{} WHERE shard = ? AND group_id = ?",
        db::system_keyspace::RAFT_GROUPS_SNAPSHOTS);
    ::shared_ptr<cql3::untyped_result_set> snp_rs = co_await _qp.execute_internal(load_snp_info_cql, {int16_t(_shard), _group_id.id}, cql3::query_processor::cache_internal::yes);
    if (snp_rs->empty() || !snp_rs->one().has("snapshot_id")) {
        co_return raft::snapshot_descriptor();
    }
    const auto& snp_row = snp_rs->one();
    if (snp_row.get_as<utils::UUID>("snapshot_id") != snapshot_id) {
        co_return raft::snapshot_descriptor();
    }

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

future<db::rp_handle> raft_groups_storage::add_raft_entry_to_commitlog(const raft_commit_log_entry& e) {
    if (!_commitlog) {
        on_internal_error(rgslog, "raft commitlog is not initialized");
    }

    const auto size = sizeof(uint8_t) + ser::get_sizeof(e);
    auto timeout = db::timeout_clock::time_point::max();
    auto handle = co_await _commitlog->add_mutation(_table_id, size, timeout, db::commitlog::force_sync::no,
            [entry = e] (db::commitlog::output& out) {
        ser::serialize(out, uint8_t(1));
        ser::serialize(out, entry);
    });
    co_return handle;
}

future<> raft_groups_storage::store_log_entries_internal(const std::vector<raft::log_entry_ptr>& entries) {
    for (const auto& eptr : entries) {
        raft_commit_log_entry op(_group_id.id, raft::log_entry(*eptr));
        auto h = co_await add_raft_entry_to_commitlog(op);
        _entry_rp_handles.insert_or_assign(eptr->idx, std::move(h));
        _log[eptr->idx] = eptr;
        if (_replayed_truncate_idx && eptr->idx.value() >= _replayed_truncate_idx->value()) {
            _replayed_truncate_idx.reset();
        }
        if (!_replayed_seen_max_idx || _replayed_seen_max_idx->value() < eptr->idx.value()) {
            _replayed_seen_max_idx = eptr->idx;
        }
        if (!_replayed_max_idx || _replayed_max_idx->value() < eptr->idx.value()) {
            _replayed_max_idx = eptr->idx;
        }
        co_await coroutine::maybe_yield();
    }
}

future<> raft_groups_storage::store_log_entries(const std::vector<raft::log_entry_ptr>& entries) {
    return execute_with_linearization_point([this, &entries] {
        return store_log_entries_internal(entries);
    });
}

void raft_groups_storage::apply_replayed_entry(const raft::log_entry& entry) {
    _log[entry.idx] = make_lw_shared<const raft::log_entry>(entry);
    if (_replayed_truncate_idx && entry.idx.value() >= _replayed_truncate_idx->value()) {
        _replayed_truncate_idx.reset();
    }
    if (!_replayed_seen_max_idx || _replayed_seen_max_idx->value() < entry.idx.value()) {
        _replayed_seen_max_idx = entry.idx;
    }
    if (!_replayed_max_idx || _replayed_max_idx->value() < entry.idx.value()) {
        _replayed_max_idx = entry.idx;
    }
}

void raft_groups_storage::apply_replayed_truncate(raft::index_t idx) {
    _replayed_has_truncate = true;
    _replayed_truncate_idx = idx;
    _replayed_truncate_prefix_idx.reset();
    if (!_replayed_seen_max_idx || _replayed_seen_max_idx->value() < idx.value()) {
        _replayed_seen_max_idx = idx;
    }
    auto it = _log.lower_bound(idx);
    _log.erase(it, _log.end());
    auto rit = _entry_rp_handles.lower_bound(idx);
    _entry_rp_handles.erase(rit, _entry_rp_handles.end());
    if (_log.empty()) {
        _replayed_max_idx.reset();
    } else {
        _replayed_max_idx = _log.rbegin()->first;
    }
}

void raft_groups_storage::record_replayed_truncate_prefix(raft::index_t idx) {
    _replayed_has_truncate = true;
    _replayed_truncate_prefix_idx = idx;
    _replayed_truncate_idx.reset();
    if (!_replayed_seen_max_idx || _replayed_seen_max_idx->value() < idx.value()) {
        _replayed_seen_max_idx = idx;
    }
}

void raft_groups_storage::apply_replayed_truncate_prefix(raft::index_t idx) {
    record_replayed_truncate_prefix(idx);
    auto it = _log.upper_bound(idx);
    _log.erase(_log.begin(), it);
    auto rit = _entry_rp_handles.upper_bound(idx);
    _entry_rp_handles.erase(_entry_rp_handles.begin(), rit);
    if (_log.empty()) {
        _replayed_max_idx.reset();
    } else {
        _replayed_max_idx = _log.rbegin()->first;
    }
}

future<> raft_groups_storage::truncate_log(raft::index_t idx) {
    return execute_with_linearization_point([this, idx] () -> future<> {
        raft_commit_log_entry op(_group_id.id, idx, std::nullopt);
        auto h = co_await add_raft_entry_to_commitlog(op);
        _truncate_tail_rp_handle = std::move(h);
        apply_replayed_truncate(idx);
    });
}

future<> raft_groups_storage::truncate_prefix_log(raft::index_t idx) {
    return execute_with_linearization_point([this, idx] () -> future<> {
        co_await truncate_prefix_log_impl(idx);
    });
}

future<> raft_groups_storage::truncate_prefix_log_impl(raft::index_t idx) {
    raft_commit_log_entry op(_group_id.id, std::nullopt, idx);
    auto h = co_await add_raft_entry_to_commitlog(op);
    _truncate_prefix_rp_handle = std::move(h);
    apply_replayed_truncate_prefix(idx);
}

future<> raft_groups_storage::abort() {
    std::exception_ptr pending_error;
    try {
        co_await std::exchange(_pending_op_fut, make_ready_future<>());
    } catch (...) {
        pending_error = std::current_exception();
    }
    _entry_rp_handles.clear();
    _truncate_tail_rp_handle.reset();
    _truncate_prefix_rp_handle.reset();
    if (pending_error) {
        std::rethrow_exception(pending_error);
    }
}

future<> raft_groups_storage::update_snapshot_and_truncate_log_tail(const raft::snapshot_descriptor &snap, size_t preserve_log_entries) {
    static const auto store_latest_id_cql = format("INSERT INTO system.{} (shard, group_id, snapshot_id) VALUES (?, ?, ?)",
        db::system_keyspace::RAFT_GROUPS);
    co_await _qp.execute_internal(
        store_latest_id_cql,
        {int16_t(_shard), _group_id.id, snap.id.id},
        cql3::query_processor::cache_internal::yes).discard_result();

    if (preserve_log_entries >= snap.idx.value()) {
        co_return;
    }

    raft::index_t log_tail_idx(snap.idx.value() - preserve_log_entries);
    co_await truncate_prefix_log_impl(log_tail_idx);
    if (_commitlog) {
        co_await _commitlog->sync_all_segments();
    }
}

future<> raft_groups_storage::store_snapshot_descriptor(const raft::snapshot_descriptor& snap, size_t preserve_log_entries) {
    return execute_with_linearization_point([this, &snap, preserve_log_entries] () -> future<> {
        static const auto store_snp_cql = format("INSERT INTO system.{} (shard, group_id, snapshot_id, idx, term) VALUES (?, ?, ?, ?, ?)",
            db::system_keyspace::RAFT_GROUPS_SNAPSHOTS);
        co_await _qp.execute_internal(
            store_snp_cql,
            {int16_t(_shard), _group_id.id, snap.id.id, int64_t(snap.idx.value()), int64_t(snap.term.value())},
            cql3::query_processor::cache_internal::yes
        );
        static const auto delete_raft_cfg_cql = format("DELETE FROM system.{} WHERE shard = ? AND group_id = ?", db::system_keyspace::RAFT_GROUPS_SNAPSHOT_CONFIG);
        co_await _qp.execute_internal(delete_raft_cfg_cql, {int16_t(_shard), _group_id.id}, cql3::query_processor::cache_internal::yes);
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

        co_await update_snapshot_and_truncate_log_tail(snap, preserve_log_entries);
    });
}

future<> raft_groups_storage::replay_log_entries_from_file(const sstring& file) {
    co_await db::commitlog::read_log_file(file, COMMITLOG_FILENAME_PREFIX,
        [this] (db::commitlog::buffer_and_replay_position buf_rp) -> future<> {
            commitlog_entry_reader reader(buf_rp.buffer, buf_rp.segment_version);
            auto raft_entry = reader.get_raft_entry();
            if (!raft_entry || raft_entry->group_id() != _group_id.id) {
                co_return;
            }

            if (raft_entry->entry()) {
                apply_replayed_entry(*raft_entry->entry());
            } else if (raft_entry->truncate_idx()) {
                apply_replayed_truncate(*raft_entry->truncate_idx());
            } else if (raft_entry->truncate_prefix_idx()) {
                apply_replayed_truncate_prefix(*raft_entry->truncate_prefix_idx());
            }
            co_await coroutine::maybe_yield();
        },
        0,
        &_qp.db().extensions());
}

future<> raft_groups_storage::replay_log_entries(std::vector<sstring> files) {
    bool replay_had_errors = false;
    auto replay_state = co_await replay_log_entries_for_groups(_qp, std::move(files), &replay_had_errors);
    set_replay_had_errors(replay_had_errors);
    if (auto it = replay_state.find(_group_id); it != replay_state.end()) {
        set_replayed_log_state(std::move(it->second));
    } else {
        set_replayed_log_state({});
    }
}

future<std::unordered_map<raft::group_id, raft_groups_storage::group_replay_state>>
raft_groups_storage::replay_log_entries_for_groups(cql3::query_processor& qp, std::vector<sstring> files, bool* had_errors) {
    groups_replay_state_map replay_state;

    std::vector<std::pair<db::segment_id_type, sstring>> ordered_files;
    ordered_files.reserve(files.size());
    for (auto& file : files) {
        try {
            db::commitlog::descriptor d(file, COMMITLOG_FILENAME_PREFIX);
            ordered_files.emplace_back(d.id, std::move(file));
        } catch (const std::domain_error&) {
            continue;
        }
    }
    std::ranges::sort(ordered_files, std::less{}, &std::pair<db::segment_id_type, sstring>::first);

    for (const auto& [id, file] : ordered_files) {
        (void) id;
        try {
            co_await db::commitlog::read_log_file(file, COMMITLOG_FILENAME_PREFIX,
                [&replay_state] (db::commitlog::buffer_and_replay_position buf_rp) -> future<> {
                    commitlog_entry_reader reader(buf_rp.buffer, buf_rp.segment_version);
                    auto raft_entry = reader.get_raft_entry();
                    if (!raft_entry) {
                        co_return;
                    }

                    auto gid = raft::group_id{raft_entry->group_id()};
                    auto& group_state = replay_state[gid];
                    if (raft_entry->entry()) {
                        apply_group_replayed_entry(group_state, *raft_entry->entry());
                    } else if (raft_entry->truncate_idx()) {
                        apply_group_replayed_truncate(group_state, *raft_entry->truncate_idx());
                    } else if (raft_entry->truncate_prefix_idx()) {
                        auto idx = *raft_entry->truncate_prefix_idx();
                        group_state.replayed_has_truncate = true;
                        group_state.replayed_truncate_prefix_idx = idx;
                        group_state.replayed_truncate_idx.reset();
                        if (!group_state.replayed_seen_max_idx || group_state.replayed_seen_max_idx->value() < idx.value()) {
                            group_state.replayed_seen_max_idx = idx;
                        }
                    }
                    co_await coroutine::maybe_yield();
                },
                0,
                &qp.db().extensions());
        } catch (const db::commitlog::segment_truncation& e) {
            if (had_errors) {
                *had_errors = true;
            }
            rgslog.warn("Ignoring truncated raft commitlog segment {} at position {}", file, e.position());
        } catch (const db::commitlog::segment_data_corruption_error& e) {
            if (had_errors) {
                *had_errors = true;
            }
            rgslog.warn("Ignoring corrupted raft commitlog segment {} ({} bytes)", file, e.bytes());
        } catch (const db::commitlog::header_checksum_error&) {
            if (had_errors) {
                *had_errors = true;
            }
            rgslog.warn("Ignoring raft commitlog segment {} with broken header checksum", file);
        }
    }

    co_return replay_state;
}

void raft_groups_storage::set_replayed_log_state(group_replay_state state) {
    _log = std::move(state.log);
    _entry_rp_handles.clear();
    _truncate_tail_rp_handle.reset();
    _truncate_prefix_rp_handle.reset();
    _replayed_max_idx = state.replayed_max_idx;
    _replayed_seen_max_idx = state.replayed_seen_max_idx;
    _replayed_truncate_idx = state.replayed_truncate_idx;
    _replayed_truncate_prefix_idx = state.replayed_truncate_prefix_idx;
    _replayed_has_truncate = state.replayed_has_truncate;
}

void raft_groups_storage::set_replay_had_errors(bool had_errors) {
    _replay_had_errors = had_errors;
}

future<> raft_groups_storage::filter_replayed_log_with_snapshot() {
    if (!_replayed_truncate_prefix_idx) {
        co_return;
    }

    auto snap = co_await load_snapshot_descriptor();
    if (!snap.id || _replayed_truncate_prefix_idx->value() > snap.idx.value()) {
        _replayed_truncate_prefix_idx.reset();
        if (!_replayed_truncate_idx) {
            _replayed_has_truncate = false;
        }
        co_return;
    }

    if (_replayed_truncate_prefix_idx) {
        apply_replayed_truncate_prefix(*_replayed_truncate_prefix_idx);
    }
    co_return;
}

future<> raft_groups_storage::materialize_log_state_to_commitlog() {
    if (_entry_rp_handles.size() == _log.size()
            && _truncate_tail_rp_handle.has_value() == _replayed_truncate_idx.has_value()
            && _truncate_prefix_rp_handle.has_value() == _replayed_truncate_prefix_idx.has_value()) {
        co_return;
    }

    if (_log.empty() && !_replayed_has_truncate) {
        co_return;
    }

    std::vector<raft::log_entry_ptr> entries;
    entries.reserve(_log.size());
    for (const auto& [idx, entry] : _log) {
        (void) idx;
        entries.push_back(entry);
        co_await coroutine::maybe_yield();
    }

    _entry_rp_handles.clear();
    _truncate_tail_rp_handle.reset();
    _truncate_prefix_rp_handle.reset();
    _log.clear();
    _replayed_max_idx.reset();
    _replayed_seen_max_idx.reset();
    auto replayed_truncate_idx = _replayed_truncate_idx;
    auto replayed_truncate_prefix_idx = _replayed_truncate_prefix_idx;
    _replayed_truncate_idx.reset();
    _replayed_truncate_prefix_idx.reset();

    if (!entries.empty()) {
        co_await store_log_entries_internal(entries);
    }

    if (_replayed_has_truncate) {
        if (replayed_truncate_prefix_idx) {
            co_await truncate_prefix_log(*replayed_truncate_prefix_idx);
        } else if (replayed_truncate_idx) {
            co_await truncate_log(*replayed_truncate_idx);
        }
    }

    _replayed_has_truncate = false;
}

bool raft_groups_storage::has_commitlog_log_entries() const {
    return _replayed_max_idx.has_value() || _replayed_has_truncate;
}

bool raft_groups_storage::should_cleanup_legacy_log_entries() const {
    return _allow_legacy_cleanup;
}

future<bool> raft_groups_storage::import_legacy_log_entries() {
    _allow_legacy_cleanup = has_commitlog_log_entries();
    if (_replay_had_errors) {
        _allow_legacy_cleanup = false;
    }

    static const auto load_cql = format("SELECT term, \"index\", data FROM system.{} WHERE shard = ? AND group_id = ?", db::system_keyspace::RAFT_GROUPS);
    auto rs = co_await _qp.execute_internal(load_cql, {int16_t(_shard), _group_id.id}, cql3::query_processor::cache_internal::yes);

    std::vector<raft::log_entry_ptr> to_append;
    to_append.reserve(rs->size());
    const auto replayed_max = _replayed_max_idx ? _replayed_max_idx->value() : 0;
    for (const auto& row : *rs) {
        if (!row.has("data")) {
            continue;
        }
        raft::index_t idx(row.get_as<int64_t>("index"));
        if (idx.value() <= replayed_max) {
            continue;
        }
        if (_log.contains(idx)) {
            continue;
        }
        if (_replayed_truncate_idx && idx.value() >= _replayed_truncate_idx->value()) {
            continue;
        }
        if (_replayed_truncate_prefix_idx && idx.value() <= _replayed_truncate_prefix_idx->value()) {
            continue;
        }
        auto raw_data = row.get_view("data");
        auto in = ser::as_input_stream(raw_data);
        using data_variant_type = decltype(raft::log_entry::data);
        data_variant_type data = ser::deserialize(in, std::type_identity<data_variant_type>());
        auto term = raft::term_t(row.get_as<int64_t>("term"));
        to_append.push_back(make_lw_shared<const raft::log_entry>(raft::log_entry{.term = term, .idx = idx, .data = std::move(data)}));
    }

    std::ranges::sort(to_append, std::less{}, [] (const raft::log_entry_ptr& e) { return e->idx; });
    if (!to_append.empty()) {
        co_await store_log_entries_internal(to_append);
        if (_commitlog) {
            co_await _commitlog->sync_all_segments();
        }
        if (!_replay_had_errors) {
            _allow_legacy_cleanup = true;
        }
        co_return true;
    }

    co_return false;
}

future<> raft_groups_storage::cleanup_legacy_log_entries() {
    if (!_allow_legacy_cleanup) {
        co_return;
    }

    static const auto load_idx_cql = format("SELECT \"index\" FROM system.{} WHERE shard = ? AND group_id = ?", db::system_keyspace::RAFT_GROUPS);
    static const auto delete_idx_cql = format("DELETE FROM system.{} WHERE shard = ? AND group_id = ? AND \"index\" = ?", db::system_keyspace::RAFT_GROUPS);
    auto rs = co_await _qp.execute_internal(load_idx_cql, {int16_t(_shard), _group_id.id}, cql3::query_processor::cache_internal::yes);
    for (const auto& row : *rs) {
        if (!row.has("index")) {
            continue;
        }
        co_await _qp.execute_internal(delete_idx_cql, {int16_t(_shard), _group_id.id, row.get_as<int64_t>("index")}, cql3::query_processor::cache_internal::yes).discard_result();
    }
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

} // namespace service::strong_consistency
