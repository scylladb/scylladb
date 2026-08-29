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

// Frozen list of (segment_id, from, to) tuples: a frozen collection is one
// cell, which is what a mutation can set directly. thread_local, not static:
// data_type is a seastar::shared_ptr with a non-atomic reference count, so
// shards must not share one instance.
// The element type of truncations_type(); make_tablet_raft_groups_schema() spells
// the same triple for the column, and a mismatch shows up on the first row read.
data_type truncation_tuple_type() {
    static thread_local const data_type type =
            tuple_type_impl::get_instance({long_type, long_type, long_type});
    return type;
}

data_type truncations_type() {
    static thread_local const data_type type =
            list_type_impl::get_instance(truncation_tuple_type(), false);
    return type;
}

data_value serialize_truncations(const std::vector<truncation_record>& truncations) {
    const auto element = truncation_tuple_type();
    std::vector<data_value> values;
    values.reserve(truncations.size());
    for (const auto& truncation : truncations) {
        values.push_back(make_tuple_value(element, tuple_type_impl::native_type{
                data_value(int64_t(truncation.segment)),
                data_value(int64_t(truncation.from.value())),
                data_value(int64_t(truncation.to.value()))}));
    }
    return make_list_value(truncations_type(), std::move(values));
}

std::vector<truncation_record> deserialize_truncations(const managed_bytes_view& blob) {
    std::vector<truncation_record> ret;
    const auto truncation_list = value_cast<list_type_impl::native_type>(
            truncations_type()->deserialize(blob));
    ret.reserve(truncation_list.size());
    for (const auto& element : truncation_list) {
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
    // No IO: the commit index reaches the row through the record releases, and
    // after a crash through the commit_idx in the batch headers.
    _commit_index = idx;
    maybe_release();
    return make_ready_future<>();
}

future<raft::index_t> raft_groups_storage::load_commit_idx() {
    return load_commit_idx(_qp, _group_id, _shard);
}

future<raft::index_t> raft_groups_storage::load_commit_idx(cql3::query_processor& qp, raft::group_id gid, shard_id shard) {
    co_return (co_await load_commit_idx_if_persisted(qp, gid, shard)).value_or(raft::index_t(0));
}

future<std::optional<raft::index_t>> raft_groups_storage::load_commit_idx_if_persisted(cql3::query_processor& qp,
        raft::group_id gid, shard_id shard) {
    // An empty result means the partition isn't there at all. A partition that exists but
    // has no index yet still comes back as one row, with the column unset, which is what
    // separates "nothing persisted" from "persisted, nothing committed yet".
    //
    // snapshot_idx is a safe commit index: a record is released only once every index it
    // covers is committed. It is usually behind what the group had committed when it
    // stopped.
    static const auto load_cql = format("SELECT snapshot_idx FROM system.{} WHERE shard = ? AND group_id = ? LIMIT 1", db::system_keyspace::RAFT_GROUPS);
    ::shared_ptr<cql3::untyped_result_set> rs = co_await qp.execute_internal(load_cql, {int16_t(shard), gid.id}, cql3::query_processor::cache_internal::yes);
    if (rs->empty()) {
        co_return std::nullopt;
    }
    co_return raft::index_t(rs->one().get_or<int64_t>("snapshot_idx", raft::index_t{}.value()));
}

future<> raft_groups_storage::erase_persisted_state(cql3::query_processor& qp, raft::group_id gid, shard_id shard) {
    // One row holds everything this shard persists about the group, so the erase is one
    // statement and cannot be left half done. Afterwards the group reads as one that was
    // never bootstrapped, which is what start_raft_group() needs in order to bootstrap it
    // afresh if this shard ever hosts it again.
    static const auto delete_group_cql = format("DELETE FROM system.{} WHERE shard = ? AND group_id = ?",
        db::system_keyspace::RAFT_GROUPS);
    co_await qp.execute_internal(delete_group_cql, {int16_t(shard), gid.id}, cql3::query_processor::cache_internal::yes);

    rgslog.info("erase_persisted_state: erased the persisted raft state of group {} on shard {}", gid, shard);
}

future<raft::log_entries> raft_groups_storage::load_log() {
    return make_ready_future<raft::log_entries>(_raft_commitlog.load_log());
}

future<raft::snapshot_descriptor> raft_groups_storage::load_snapshot_descriptor() {
    // The whole descriptor is in the group's own row: one read, and the index,
    // the term and the configuration are always the ones that were written
    // together.
    static const auto load_cql = format(
            "SELECT snapshot_id, snapshot_idx, snapshot_term, snapshot_config, truncations FROM system.{} "
            "WHERE shard = ? AND group_id = ? LIMIT 1",
            db::system_keyspace::RAFT_GROUPS);
    ::shared_ptr<cql3::untyped_result_set> result = co_await _qp.execute_internal(load_cql,
            {int16_t(_shard), _group_id.id}, cql3::query_processor::cache_internal::yes);
    if (result->empty() || !result->one().has("snapshot_id")) {
        // No descriptor yet; groups_manager will bootstrap() the group.
        co_return raft::snapshot_descriptor();
    }
    const auto& row = result->one();
    raft::snapshot_descriptor snap{
        .idx = raft::index_t(row.get_or<int64_t>("snapshot_idx", 0)),
        .term = raft::term_t(row.get_or<int64_t>("snapshot_term", 0)),
        .id = raft::snapshot_id(row.get_as<utils::UUID>("snapshot_id")),
    };
    if (row.has("snapshot_config")) {
        snap.config = deserialize_config(row.get_blob_unfragmented("snapshot_config"));
    }

    // Called twice before the group starts: groups_manager reads the descriptor
    // to decide whether to bootstrap, raft::server reads it again as it starts.
    // The seeding below must stay idempotent.
    _snapshot_config = snap.config;
    if (row.has("truncations")) {
        // The row already holds this, so the first release need not rewrite it.
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
    // An earlier release may have failed on memtable pressure, leaving a record at the
    // front that should be gone; the pin below aborts on the first index past it. Retry
    // here, where the retry is still cheap and the indexes are known.
    maybe_release();
    return _raft_commitlog.pin_for_apply(idx);
}

void raft_groups_storage::note_applied(raft::index_t idx) {
    _apply_index = std::max(_apply_index, idx);
    maybe_release();
}

void raft_groups_storage::mark_segment_closed(db::replay_position pos) {
    _raft_commitlog.mark_segment_closed(pos);
    maybe_release();
}

void raft_groups_storage::maybe_release() {
    while (auto* record = _raft_commitlog.front_releasable(_commit_index, _apply_index)) {
        try {
            write_descriptor_and_purge_stale_truncations(*record);
        } catch (...) {
            // The write logged it and left the record in place for a retry. Raft is
            // not told: the entries are already in the commitlog, so a descriptor
            // that did not land is not a failed log write, and raft treats a throw
            // on the persistence path as fatal. Returning rather than continuing,
            // because the front record has not moved.
            return;
        }
        _raft_commitlog.pop_released();
    }
}

void raft_groups_storage::write_descriptor_and_purge_stale_truncations(segment_record& record) {
    // Remove truncation records for segments that no longer exist, so replay does not
    // need them. Pruned here because the write below persists the truncation history.
    _raft_commitlog.purge_stale_truncations();
    // A record carrying no configuration of its own re-persists the current one:
    // the descriptor is written whole, and a group's membership does not become
    // unknown just because the last segment held no configuration entry.
    if (auto last_configuration = record.last_conf()) {
        _snapshot_config = std::move(last_configuration->second);
    }

    auto schema = db::system_keyspace::raft_groups();
    auto pk = partition_key::from_exploded(*schema, {
        short_type->decompose(int16_t(_shard)),
        timeuuid_type->decompose(_group_id.id),
    });
    mutation m(schema, std::move(pk));
    // No clustering key: the whole row is one clustered row at the empty key.
    const auto ckey = clustering_key::make_empty();
    // Strictly increasing per group; see _last_row_timestamp. This counter does not
    // see the one the CQL writers of this row use, so under a backwards clock step
    // the two can order wrongly against each other - including an erase that loses to
    // surviving descriptor cells. Closing that needs the tombstone to derive its
    // timestamp from the row itself, which is the fence design's rule 2.
    _last_row_timestamp = std::max(api::new_timestamp(), _last_row_timestamp + 1);
    const api::timestamp_type ts = _last_row_timestamp;
    m.set_clustered_cell(ckey, "snapshot_idx", data_value(int64_t(record.max_index.value())), ts);
    m.set_clustered_cell(ckey, "snapshot_term", data_value(int64_t(record.max_term().value())), ts);
    m.set_clustered_cell(ckey, "snapshot_config", data_value(serialize_config(_snapshot_config)), ts);
    // Only when it actually moved: see _persisted_truncations.
    const auto& truncations = _raft_commitlog.truncations();
    const bool rewrite_truncations = truncations != _persisted_truncations;
    if (rewrite_truncations) {
        m.set_clustered_cell(ckey, "truncations", serialize_truncations(truncations), ts);
    }

    m.partition().clustered_row(*schema, ckey).apply(row_marker(ts));

    auto& cf = _db.find_column_family(_raft_groups_table_id);
    // Cloned before the apply, not in the handler below: apply() fails under memory
    // pressure, and cloning there would allocate under the same pressure and lose the
    // reference for good. On the way out this spare is destroyed, undoing the clone.
    auto spare_pin = record.pin_user_table.clone(_raft_groups_table_id);
    try {
        // Synchronous and IO-free, straight into the raft_groups memtable.
        cf.apply(m, std::move(record.pin_raft_groups));
    } catch (...) {
        // The reference went into a mutation that did not land; put the spare back so
        // the segment stays held, and leave the record for a retry.
        record.pin_raft_groups = std::move(spare_pin);
        rgslog.warn("group_id={}: writing the descriptor for segment {} failed, the record "
                "stays for a retry: {}", _group_id, record.segment(), std::current_exception());
        throw;
    }
    // Only once the mutation has landed, so a failed release does not leave us
    // believing the row holds a history it never received.
    if (rewrite_truncations) {
        _persisted_truncations = truncations;
    }
    rgslog.debug("released record for group_id={}: segment={}, snapshot=({}, {}), truncations={}{}",
            _group_id, record.segment(), record.max_index, record.max_term(), truncations.size(),
            rewrite_truncations ? "" : " (unchanged, not rewritten)");
}

future<raft_groups_storage::persisted_descriptor> raft_groups_storage::load_descriptor(
        cql3::query_processor& qp, raft::group_id gid, shard_id shard) {
    static const auto load_cql = format(
            "SELECT snapshot_idx, snapshot_term, snapshot_config, truncations FROM system.{} "
            "WHERE shard = ? AND group_id = ? LIMIT 1",
            db::system_keyspace::RAFT_GROUPS);
    auto result = co_await qp.execute_internal(load_cql, {int16_t(shard), gid.id},
            cql3::query_processor::cache_internal::yes);
    persisted_descriptor ret;
    if (result->empty()) {
        co_return ret;
    }
    const auto& row = result->one();
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
    // The configuration goes with the index and term. Writing those two alone
    // would leave whatever configuration the previous run put in the row, which
    // is the mismatch keeping all three in one row exists to rule out.
    static const auto store_cql = format(
            "INSERT INTO system.{} (shard, group_id, snapshot_id, snapshot_idx, snapshot_term, snapshot_config) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            db::system_keyspace::RAFT_GROUPS);
    co_await qp.execute_internal(store_cql,
            {int16_t(shard), gid.id, snap.id.id, int64_t(snap.idx.value()), int64_t(snap.term.value()),
             data_value(serialize_config(snap.config))},
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
    // Written once. Raft only checks that an id is set, so no release rewrites it.
    const raft::snapshot_id snapshot_id(utils::make_random_uuid());
    _snapshot_config = std::move(initial_configuation);
    _commit_index = std::max(_commit_index, init_index);
    _apply_index = std::max(_apply_index, init_index);
    static const auto store_cql = format(
            "INSERT INTO system.{} (shard, group_id, snapshot_id, snapshot_idx, snapshot_term, snapshot_config, truncations) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            db::system_keyspace::RAFT_GROUPS);
    co_await _qp.execute_internal(store_cql,
            {int16_t(_shard), _group_id.id, snapshot_id.id, int64_t(init_index.value()), int64_t(0),
             data_value(serialize_config(_snapshot_config)),
             serialize_truncations({})},
            cql3::query_processor::cache_internal::yes);
}

} // namespace service::strong_consistency
