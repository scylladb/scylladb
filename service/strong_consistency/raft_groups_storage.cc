/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "service/strong_consistency/raft_groups_storage.hh"

#include "cql3/untyped_result_set.hh"
#include "db/system_keyspace.hh"
#include "types/list.hh"
#include "types/tuple.hh"
#include "raft/raft.hh"
#include "replica/database.hh"
#include "utils/UUID.hh"
#include "utils/UUID_gen.hh"
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

bytes serialize_config(const raft::configuration& config) {
    bytes_ostream out;
    ser::serialize(out, config);
    return bytes(out.linearize());
}

raft::configuration deserialize_config(const bytes& blob) {
    auto in = ser::as_input_stream(blob);
    return ser::deserialize(in, std::type_identity<raft::configuration>());
}

// The truncation history is a frozen list of (segment_id, from, to) tuples, so
// it reads in cqlsh and carries no encoding of its own. Frozen because a release
// rewrites the whole list rather than editing it, and because a frozen
// collection is one cell — which is what a mutation can set directly.
// thread_local, not a plain static: data_type is a seastar::shared_ptr, whose
// reference count is a plain long. One instance shared by every shard would have
// them racing on that count, and would be destroyed at exit off-shard.
data_type truncations_type() {
    static thread_local const data_type t = list_type_impl::get_instance(
            tuple_type_impl::get_instance({long_type, long_type, long_type}), false);
    return t;
}

data_value serialize_truncations(const std::vector<truncation_record>& truncations) {
    const auto element = tuple_type_impl::get_instance({long_type, long_type, long_type});
    std::vector<data_value> values;
    values.reserve(truncations.size());
    for (const auto& t : truncations) {
        values.push_back(make_tuple_value(element, tuple_type_impl::native_type{
                data_value(int64_t(t.segment)),
                data_value(int64_t(t.from.value())),
                data_value(int64_t(t.to.value()))}));
    }
    return make_list_value(truncations_type(), std::move(values));
}

std::vector<truncation_record> deserialize_truncations(const managed_bytes_view& blob) {
    std::vector<truncation_record> ret;
    const auto value = value_cast<list_type_impl::native_type>(
            truncations_type()->deserialize(blob));
    ret.reserve(value.size());
    for (const auto& element : value) {
        const auto& fields = value_cast<tuple_type_impl::native_type>(element);
        ret.push_back(truncation_record{
                .segment = db::segment_id_type(value_cast<int64_t>(fields[0])),
                .from = raft::index_t(value_cast<int64_t>(fields[1])),
                .to = raft::index_t(value_cast<int64_t>(fields[2]))});
    }
    return ret;
}

} // namespace

raft_groups_storage::raft_groups_storage(cql3::query_processor& qp, replica::database& db, raft::group_id gid,
        raft::server_id server_id, shard_id shard, db::commitlog& commit_log, table_id target_table_id,
        replayed_data_per_group replayed_data)
    : _group_id(std::move(gid))
    , _server_id(std::move(server_id))
    , _qp(qp)
    , _db(db)
    , _raft_groups_table_id(db::system_keyspace::raft_groups()->id())
    , _raft_commitlog(_group_id, commit_log, target_table_id, _raft_groups_table_id, std::move(replayed_data))
    , _pending_op_fut(make_ready_future<>())
{
    rgslog.trace("Creating raft_groups_storage for group_id={}, server_id={}, shard={}", _group_id, _server_id, shard);
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
    // No IO: the commit index reaches the row through the record releases
    // below, and after a crash through the commit_idx the batch headers carry.
    _commit_index = idx;
    maybe_release();
    return make_ready_future<>();
}

future<raft::index_t> raft_groups_storage::load_commit_idx() {
    return load_commit_idx(_qp, _group_id, _shard);
}

future<raft::index_t> raft_groups_storage::load_commit_idx(cql3::query_processor& qp, raft::group_id gid, shard_id shard) {
    // The persisted snapshot index *is* the commit index: a record is released
    // only once every index it covers is committed, so what the row says is
    // committed by definition.
    static const auto load_cql = format("SELECT snapshot_idx FROM system.{} WHERE shard = ? AND group_id = ? LIMIT 1", db::system_keyspace::RAFT_GROUPS);
    ::shared_ptr<cql3::untyped_result_set> rs = co_await qp.execute_internal(load_cql, {int16_t(shard), gid.id}, cql3::query_processor::cache_internal::yes);
    if (rs->empty()) {
        co_return raft::index_t(0);
    }
    co_return raft::index_t(rs->one().get_or<int64_t>("snapshot_idx", raft::index_t{}.value()));
}

future<raft::log_entries> raft_groups_storage::load_log() {
    return make_ready_future<raft::log_entries>(_raft_commitlog.load_log());
}

future<raft::snapshot_descriptor> raft_groups_storage::load_snapshot_descriptor() {
    static const auto load_cql = format(
            "SELECT snapshot_id, snapshot_idx, snapshot_term, snapshot_config, truncations FROM system.{} "
            "WHERE shard = ? AND group_id = ? LIMIT 1",
            db::system_keyspace::RAFT_GROUPS);
    ::shared_ptr<cql3::untyped_result_set> rs = co_await _qp.execute_internal(load_cql,
            {int16_t(_shard), _group_id.id}, cql3::query_processor::cache_internal::yes);
    if (rs->empty() || !rs->one().has("snapshot_id")) {
        // No descriptor yet: this node has not hosted the group before, and
        // groups_manager will bootstrap() it.
        co_return raft::snapshot_descriptor();
    }
    const auto& row = rs->one();
    raft::snapshot_descriptor snap{
        .idx = raft::index_t(row.get_or<int64_t>("snapshot_idx", 0)),
        .term = raft::term_t(row.get_or<int64_t>("snapshot_term", 0)),
        .id = raft::snapshot_id(row.get_as<utils::UUID>("snapshot_id")),
    };
    if (row.has("snapshot_config")) {
        snap.config = deserialize_config(row.get_blob_unfragmented("snapshot_config"));
    }

    // Runs before the group starts, and runs twice — groups_manager reads the
    // descriptor to decide whether to bootstrap, raft::server reads it again as
    // it starts — so the seeding below has to be idempotent, which it is: every
    // assignment is a plain overwrite with what the row says. The second read is
    // what picks up a bootstrap written between the two.
    _snapshot_id = snap.id;
    _snapshot_config = snap.config;
    if (row.has("truncations")) {
        // The row already holds this, so the first release after a restart has
        // no reason to write it again.
        _persisted_truncations = deserialize_truncations(row.get_view("truncations"));
        _raft_commitlog.seed_truncations(_persisted_truncations);
    }
    _commit_index = std::max(_commit_index, snap.idx);
    _apply_index = std::max(_apply_index, snap.idx);
    rgslog.debug("loaded descriptor for group_id={}: idx={}, term={}, truncations={}",
            _group_id, snap.idx, snap.term, _raft_commitlog.truncations().size());
    co_return snap;
}

future<> raft_groups_storage::store_snapshot_descriptor(const raft::snapshot_descriptor& snap, size_t preserve_log_entries) {
    // Nothing to do: see the declaration.
    return make_ready_future<>();
}

future<> raft_groups_storage::store_log_entries(const std::vector<raft::log_entry_ptr>& entries) {
    return _raft_commitlog.store_log_entries(entries, _commit_index);
}

future<> raft_groups_storage::truncate_log(raft::index_t idx) {
    _raft_commitlog.truncate_log(idx);
    return make_ready_future<>();
}

future<> raft_groups_storage::abort() {
    // wait for pending write requests to complete.
    return std::move(_pending_op_fut);
}

db::rp_handle raft_groups_storage::pin_for_apply(raft::index_t idx) {
    return _raft_commitlog.pin_for_apply(idx);
}

void raft_groups_storage::note_applied(raft::index_t idx) {
    _apply_index = std::max(_apply_index, idx);
    maybe_release();
}

void raft_groups_storage::release_all() {
    _raft_commitlog.release_all();
}

void raft_groups_storage::mark_segment_closed(db::replay_position pos) {
    _raft_commitlog.mark_segment_closed(pos);
    maybe_release();
}

void raft_groups_storage::maybe_release() {
    // pop only after the write: a record whose descriptor write threw stays in
    // place to be retried rather than dropping its segment reference.
    while (auto* rec = _raft_commitlog.front_releasable(_commit_index, _apply_index)) {
        write_snapshot_descriptor(*rec);
        _raft_commitlog.pop_released();
    }
}

void raft_groups_storage::write_snapshot_descriptor(segment_record& rec) {
    // Truncation records are only needed while the segments they name are still
    // on disk; raft_commitlog knows which those are.
    _raft_commitlog.purge_stale_truncations();
    // A record carrying no configuration of its own re-persists the current
    // one: the descriptor is written whole, and a group's membership does not
    // become unknown just because the last segment held no configuration entry.
    if (auto conf = rec.last_conf()) {
        _snapshot_config = std::move(conf->second);
    }
    if (!_snapshot_id) {
        _snapshot_id = raft::snapshot_id(utils::make_random_uuid());
    }

    auto schema = db::system_keyspace::raft_groups();
    auto pk = partition_key::from_exploded(*schema, {
        short_type->decompose(int16_t(_shard)),
        timeuuid_type->decompose(_group_id.id),
    });
    mutation m(schema, std::move(pk));
    // Strictly increasing per group, so successive releases never tie (see
    // _last_row_timestamp). Across a restart the clock is all we have; a
    // backwards step there is the same exposure any write in the system has.
    _last_row_timestamp = std::max(api::new_timestamp(), _last_row_timestamp + 1);
    const auto ts = _last_row_timestamp;
    m.set_static_cell("snapshot_id", data_value(_snapshot_id.id), ts);
    m.set_static_cell("snapshot_idx", data_value(int64_t(rec.max.value())), ts);
    m.set_static_cell("snapshot_term", data_value(int64_t(rec.max_term().value())), ts);
    m.set_static_cell("snapshot_config", data_value(serialize_config(_snapshot_config)), ts);
    // Only when it actually moved — see _persisted_truncations.
    const auto& truncations = _raft_commitlog.truncations();
    const bool rewrite_truncations = truncations != _persisted_truncations;
    if (rewrite_truncations) {
        m.set_static_cell("truncations", serialize_truncations(truncations), ts);
    }

    auto& cf = _db.find_column_family(_raft_groups_table_id);
    try {
        // Synchronous and IO-free: the mutation goes straight into the
        // raft_groups memtable, carrying the segment's raft_groups reference, so
        // the flush that makes this descriptor durable is the same flush that
        // lets the segment go.
        cf.apply(m, std::move(rec.pin_rg));
    } catch (...) {
        // The reference was moved into a mutation that did not land. Take
        // another one so the segment is still held, and leave the record for
        // the next attempt.
        rec.pin_rg = rec.pin_table.clone(_raft_groups_table_id);
        throw;
    }
    // Only once the mutation has landed, so a failed release does not leave us
    // believing the row holds a history it never received.
    if (rewrite_truncations) {
        _persisted_truncations = truncations;
    }
    rgslog.debug("released record for group_id={}: segment={}, snapshot=({}, {}), truncations={}{}",
            _group_id, rec.segment(), rec.max, rec.max_term(), truncations.size(),
            rewrite_truncations ? "" : " (unchanged, not rewritten)");
}

future<raft_groups_storage::persisted_descriptor> raft_groups_storage::load_descriptor(
        cql3::query_processor& qp, raft::group_id gid, shard_id shard) {
    static const auto load_cql = format(
            "SELECT snapshot_idx, snapshot_term, snapshot_config, truncations FROM system.{} "
            "WHERE shard = ? AND group_id = ? LIMIT 1",
            db::system_keyspace::RAFT_GROUPS);
    auto rs = co_await qp.execute_internal(load_cql, {int16_t(shard), gid.id},
            cql3::query_processor::cache_internal::yes);
    persisted_descriptor ret;
    if (rs->empty()) {
        co_return ret;
    }
    const auto& row = rs->one();
    ret.exists = true;
    ret.idx = raft::index_t(row.get_or<int64_t>("snapshot_idx", 0));
    ret.term = raft::term_t(row.get_or<int64_t>("snapshot_term", 0));
    if (row.has("snapshot_config")) {
        ret.config = deserialize_config(row.get_blob_unfragmented("snapshot_config"));
    }
    if (row.has("truncations")) {
        ret.truncations = deserialize_truncations(row.get_view("truncations"));
    }
    co_return ret;
}

future<> raft_groups_storage::store_descriptor(cql3::query_processor& qp, raft::group_id gid, shard_id shard,
        raft::index_t idx, raft::term_t term, const raft::configuration& config,
        const std::vector<truncation_record>& truncations) {
    // Only advance, never regress: a prior run (or an earlier replay) may
    // already have persisted a value at or beyond this one. An equal index is a
    // no-op too — index and term are written atomically, so the same index
    // cannot legitimately arrive with a different term.
    //
    // Index 0 is the exception, and only when the row also holds 0: a group
    // whose recovered floor is 0 still has replay-computed truncations to
    // persist, and treating that as a no-op would drop them. Written this way
    // rather than as a blanket `idx != 0` carve-out so that the guarantee above
    // holds for every input, not just the ones replay happens to produce.
    const auto persisted = co_await load_descriptor(qp, gid, shard);
    if (persisted.exists
            && (persisted.idx > idx || (persisted.idx == idx && idx != raft::index_t{0}))) {
        co_return;
    }
    static const auto store_cql = format(
            "INSERT INTO system.{} (shard, group_id, snapshot_id, snapshot_idx, snapshot_term, snapshot_config, truncations) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            db::system_keyspace::RAFT_GROUPS);
    // A descriptor raft will accept has to have an id; replay is as good a place
    // to mint one as bootstrap, and only its being set is ever checked.
    co_await qp.execute_internal(store_cql,
            {int16_t(shard), gid.id, utils::make_random_uuid(), int64_t(idx.value()), int64_t(term.value()),
             data_value(serialize_config(config)), serialize_truncations(truncations)},
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
    // The one descriptor written by CQL: there is no record to release yet, and
    // no group running whose memtable mutation could carry it.
    const auto init_index = nontrivial_snapshot ? raft::index_t{1} : raft::index_t{0};
    _snapshot_id = raft::snapshot_id(utils::make_random_uuid());
    _snapshot_config = std::move(initial_configuation);
    _commit_index = std::max(_commit_index, init_index);
    _apply_index = std::max(_apply_index, init_index);
    static const auto store_cql = format(
            "INSERT INTO system.{} (shard, group_id, snapshot_id, snapshot_idx, snapshot_term, snapshot_config, truncations) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            db::system_keyspace::RAFT_GROUPS);
    co_await _qp.execute_internal(store_cql,
            {int16_t(_shard), _group_id.id, _snapshot_id.id, int64_t(init_index.value()), int64_t(0),
             data_value(serialize_config(_snapshot_config)),
             serialize_truncations({})},
            cql3::query_processor::cache_internal::yes);
}

} // namespace service::strong_consistency
