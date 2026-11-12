/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/abort_source.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/on_internal_error.hh>
#include "state_machine.hh"
#include "mutation/frozen_mutation.hh"
#include "schema/schema_registry.hh"
#include "serializer_impl.hh"
#include "idl/strong_consistency/state_machine.dist.hh"
#include "idl/strong_consistency/state_machine.dist.impl.hh"
#include "replica/database.hh"
#include "service/migration_manager.hh"
#include "service/raft/raft_group_registry.hh"
#include "db/system_keyspace.hh"
#include "utils/error_injection.hh"
#include "schema/schema_registry.hh"
#include "service/strong_consistency/raft_commitlog.hh"
#include "service/strong_consistency/raft_resize_tracker.hh"
#include "utils/latency.hh"

#include <algorithm>

using namespace std::chrono_literals;

namespace service::strong_consistency {

static logging::logger logger("sc_state_machine");

mutation make_resize_marker_mutation(raft::group_id gid, shard_id shard, const resize_marker& marker) {
    auto s = db::system_keyspace::raft_groups();
    auto pk = partition_key::from_exploded(*s, {
        short_type->decompose(int16_t(shard)),
        timeuuid_type->decompose(gid.id),
    });
    mutation m(s, std::move(pk));
    m.set_clustered_cell(clustering_key::make_empty(), to_bytes(fmt::to_string(marker.kind)), data_value(true),
            marker.timestamp);
    return m;
}

class state_machine : public raft_state_machine {
    locator::global_tablet_id _tablet;
    raft::group_id _group_id;
    replica::database& _db;
    service::migration_manager& _mm;
    db::system_keyspace& _sys_ks;
    raft_groups_storage& _persistence;
    raft_resize_tracker& _resize_tracker;
    lw_shared_ptr<state_machine_stats> _stats;

    abort_source _as;

    // The schema_changed the writes being applied were announced under: the last one
    // applied, or, before any is, the one the group's row persisted at the floor. Loaded
    // by the first write, since the state machine is created without yielding.
    std::optional<schema_changed> _announced;
    // Resolves the announced version once. Cleared on every announcement, so that the
    // schemas of superseded versions are not kept alive.
    schema_store _schemas;

    future<> trigger_group0_barrier() {
        ++_stats->schema_barrier_waits;
        if (utils::get_local_injector().enter("disable_raft_drop_append_entries_for_specified_group")) {
            utils::get_local_injector().disable("raft_drop_incoming_append_entries_for_specified_group");
        }
        logger.debug("apply(): waiting for the group0 barrier for tablet {}, group_id={}", _tablet, _group_id);
        while (true) {
            try {
                co_await _mm.get_group0_barrier().trigger(false, &_as);
                co_return;
            } catch (const raft_operation_timeout_error& e) {
                // The write cannot be applied without the schema, and an error out of
                // apply() ends the group (on_internal_error for SC), so wait again.
                logger.warn("apply(): the group0 barrier for tablet {}, group_id={} timed out, retrying: {}",
                    _tablet, _group_id, e.what());
            }
        }
    }

public:
    state_machine(locator::global_tablet_id tablet,
        raft::group_id gid,
        replica::database& db,
        service::migration_manager& mm,
        db::system_keyspace& sys_ks,
        raft_groups_storage& persistence,
        raft_resize_tracker& resize_tracker,
        lw_shared_ptr<state_machine_stats> stats)
        : _tablet(tablet)
        , _group_id(gid)
        , _db(db)
        , _mm(mm)
        , _sys_ks(sys_ks)
        , _persistence(persistence)
        , _resize_tracker(resize_tracker)
        , _stats(std::move(stats))
        , _schemas(db, [this] { return trigger_group0_barrier(); })
    {
    }

    // Report a batch as applied when it never will be, because its table or
    // keyspace is gone. A record is released only once its last command applies,
    // so without the report the record stays at the front of the queue for good.
    // Releases are front-first, so the whole queue freezes behind it and every segment
    // the group holds stays dirty. Safe: no memtable holds this batch's data.
    void note_batch_discarded(const raft::log_entry_ptr_list& command) {
        if (!command.empty()) {
            _persistence.note_applied(command.back()->idx);
        }
    }

    future<> apply(raft::log_entry_ptr_list command) override {
        static thread_local logging::logger::rate_limit rate_limit(std::chrono::seconds(10));
        try {
            co_await utils::get_local_injector().inject("strong_consistency_state_machine_wait_before_apply", utils::wait_for_message(20min));
            logger.trace("Tablet {}, raft group {}, applies {} entries, indexes {} to {}.",
                _tablet, _group_id, command.size(), command.front()->idx, command.back()->idx);
            // Apply the commands sequentially to preserve linearizability.
            // E.g., for writes A-B-C-D, a reader must observe
            // them in that order and never see A-C or A-D skipping intermediate values.
            for (size_t i = 0; i < command.size(); ++i) {
                utils::latency_counter lc;
                lc.start();
                auto cmd = detail::deserialize_raft_command(command[i]);
                if (const auto* marker = std::get_if<resize_marker>(&cmd.change)) {
                    logger.log(log_level::trace, rate_limit, "apply(): applying the {} marker of group {}",
                        marker->kind, _group_id);
                    auto m = make_resize_marker_mutation(_group_id, this_shard_id(), *marker);
                    // We pin the segment under system.raft_groups, which the marker's row goes to.
                    co_await _db.apply_in_memory(m, m.schema()->table(),
                            _persistence.pin_raft_groups_for_apply(command[i]->idx), db::no_timeout);
                    _resize_tracker.mark_resize_phase(_group_id, *marker);
                    _persistence.note_applied(command[i]->idx);
                    continue;
                }
                if (const auto* tc = std::get_if<truncate_command>(&cmd.change)) {
                    auto record = co_await raft_groups_storage::load_truncate_record(
                            _sys_ks.query_processor(), _group_id, this_shard_id());
                    // Nothing of this entry reaches a memtable, so no pin. The entry is
                    // reported applied only below, once the record is durable, so its
                    // segment outlives the record's sync and a crash replays the entry.
                    co_await apply_truncate_command(_db, _tablet, _group_id, command[i]->idx, *tc, record);
                    _persistence.note_applied(command[i]->idx);
                    continue;
                }
                if (auto* changed = std::get_if<schema_changed>(&cmd.change)) {
                    logger.debug("apply(): group {} switches to schema version {}", _group_id, changed->version);
                    if (!_announced) {
                        _announced = co_await _persistence.load_schema_changed();
                    }
                    if (_announced && _announced->version == changed->version) {
                        // A new term announces the version in effect again. The row keeps the
                        // earlier index: moving it would put the writes since then, still in
                        // a memtable, below it, and replay skips what is below as flushed.
                        _persistence.note_applied(command[i]->idx);
                        continue;
                    }
                    if (_announced) {
                        // The writes made under the previous version reach sstables before the
                        // row can name this one. A segment goes away once its writes are flushed,
                        // so the one holding the previous schema_changed may be gone while a later
                        // segment still holds writes announced under it, below the floor. Replay
                        // has only the row's announcement for those, and skips the writes below
                        // its index; this flush is what makes the skip safe. Before note_applied():
                        // the row is written by the release this entry gates.
                        logger.debug("apply(): group {} flushes tablet {} before switching to schema version {}",
                            _group_id, _tablet, changed->version);
                        co_await _db.find_column_family(_tablet.table).flush_tablet(_tablet.tablet);
                    }
                    // Before note_applied(): the release it may trigger persists the announcement.
                    _persistence.note_schema_changed(command[i]->idx, *changed);
                    _announced = std::move(*changed);
                    _schemas.clear();
                    _persistence.note_applied(command[i]->idx);
                    continue;
                }
                auto& mut = std::get<write_mutation>(cmd.change).mutation;
                if (!_announced) {
                    _announced = co_await _persistence.load_schema_changed();
                    if (!_announced) {
                        on_internal_error(logger, fmt::format("tablet {}, group {}: the write at index {} is not "
                            "preceded by a schema_changed, and the group's row holds none",
                            _tablet, _group_id, command[i]->idx));
                    }
                }
                auto schema = co_await _schemas.resolve_and_upgrade(_tablet.table, *_announced, mut);
                // Taken after the schema resolution, which may yield on the
                // group0 barrier.
                auto commitlog_pin = _persistence.pin_target_table_for_apply(command[i]->idx);
                if (utils::get_local_injector().enter("strong_consistency_state_machine_abort_before_apply_in_memory")) {
                    throw std::runtime_error("strong_consistency_state_machine_abort_before_apply_in_memory");
                }
                // Concurrent apply_in_memory() calls can complete out of order under memory pressure
                // (suspended at run_when_memory_available()), making mutations visible out of Raft log order.
                // Therefore we must await each apply_in_memory() sequentially to preserve Raft log order.
                // apply_in_memory() skips the write counters of database::apply(), so count here.
                try {
                    co_await _db.apply_in_memory(mut, std::move(schema), std::move(commitlog_pin), db::no_timeout, db::noop_large_data_guardrail::instance());
                } catch (...) {
                    ++_db.get_stats().total_writes_failed;
                    throw;
                }
                ++_db.get_stats().total_writes;
                _stats->apply_latency.add(lc.stop().latency());
                _persistence.note_applied(command[i]->idx);
            }
        } catch (replica::no_such_column_family&) {
            // If the table doesn't exist, it means it was already dropped.
            // This cannot happen if the table wasn't created yet on the node
            // because the state machine is created only after the table is created
            // (see `schema_applier::commit_on_shard()` and `storage_service::commit_token_metadata_change()`).
            // In this case, we should just ignore mutations without throwing an error.
            logger.log(log_level::warn, rate_limit, "apply(): table {} was already dropped, ignoring mutations", _tablet.table);
            note_batch_discarded(command);
        } catch (replica::no_such_keyspace&) {
            // Thrown when DROP KEYSPACE races with the raft applier fiber.
            // The path: schema_store::resolve_and_upgrade() calls
            // local_schema_registry().get_or_null(schema_version), which may
            // trigger lazy unfreezing of a cached frozen_schema. Unfreezing
            // re-parses column types via cql_type_parser::parse(), which
            // consults db_user_types_storage::get(ks_name), which calls
            // database::find_keyspace() — and that throws no_such_keyspace
            // if the keyspace was concurrently dropped.
            // Safe to ignore: the table's raft group is about to be destroyed
            // by schedule_raft_group_deletion() anyway.
            logger.log(log_level::warn, rate_limit, "apply(): keyspace for table {} was already dropped, ignoring mutations", _tablet.table);
            note_batch_discarded(command);
        } catch (const abort_requested_exception& ex) {
            // The exception can be thrown by schema_store::resolve_and_upgrade.
            // It means that the Raft group is being removed.
            //
            // No note_batch_discarded() here, unlike the two arms above. abort() is
            // called only while the group is torn down, and the teardown gives up the
            // whole queue (raft_commitlog::release_all(), or the destructor), so no
            // record is left waiting for the batch.
            //
            // Technically, throwing an exception from a state machine
            // may result in killing the corresponding Raft instance:
            // cf. the description of raft::state_machine:
            //
            //  "Any of the functions may return an error, but it will kill the
            //   raft instance that uses it. Depending on what state the failure
            //   leaves the state is the raft instance will either have to be recreated
            //   with the same state machine and rejoined the cluster with the same server_id
            //   or it new raft instance will have to be created with empty state machine and
            //   it will have to rejoin to the cluster with different server_id through
            //   configuration change."
            //
            // Fortunately, in strong consistency, we use the default Raft server
            // implementation, which handles abort_requested_exception thrown by
            // raft::state_machine::apply -- it will simply end the applier fiber.
            logger.debug("apply(): execution for tablet {}, group_id={} aborted due to: {}",
                _tablet, _group_id, ex);
            throw;
        }
         catch (...) {
            // Nested, so that the Raft server can still tell what the error is.
            std::throw_with_nested(std::runtime_error(::format(
                "tablet {}, group id {}: error while applying mutations of entries {} to {}",
                _tablet, _group_id, command.front()->idx, command.back()->idx)));
        }
    }

    future<raft::snapshot_id> take_snapshot() override {
        // Until snapshot transfer is fully implemented, return a fake ID
        // and don't actually do anything. As long as we don't do snapshot
        // transfers (attempting to do that throws an exception), we should
        // be safe.
        return make_ready_future<raft::snapshot_id>(raft::snapshot_id(utils::make_random_uuid()));
    }

    void drop_snapshot(raft::snapshot_id id) override {
        // Taking a snapshot is a no-op, so dropping a snapshot is also a no-op.
        (void) id;
    }

    future<> load_snapshot(raft::snapshot_id id) override {
        return make_ready_future<>();
    }

    future<> abort() override {
        logger.debug("abort(): Aborting state machine for group {}", _group_id);
        _as.request_abort();
        return make_ready_future<>();
    }

    // FIXME: once implemented, the transfer has to carry the group's metadata stored in
    // system.raft_groups: the resize markers, and the schema_changed in effect at the
    // snapshot's index - that one and not a newer one, since the entries above the index
    // are applied under it.
    future<> transfer_snapshot(raft::server_id from_id, raft::snapshot_descriptor snp) override {
        throw std::runtime_error("transfer_snapshot() not implemented");
    }
};

// True if every column `mapping` names is, or was, a column of `s`: the mapping is then
// of an older version of `s` and converts to it without losing a column `s` never had.
static bool column_mapping_fits(const column_mapping& mapping, const schema& s) {
    return std::ranges::all_of(mapping.columns(), [&s] (const column_mapping_entry& entry) {
        return s.get_column_definition(entry.name())
                || s.dropped_columns().contains(sstring(to_string_view(entry.name())));
    });
}

future<schema_store::schema_entry> schema_store::get_schema(table_id table, const schema_changed& announced) {
    if (utils::get_local_injector().enter("sc_state_machine_return_empty_schema")) {
        co_return schema_entry{};
    }
    co_return schema_entry{.schema = local_schema_registry().get_or_null(announced.version)};
}

schema_store::schema_store(replica::database& db, std::function<future<>()> barrier_trigger)
    : _db(db)
    , _barrier_trigger(std::move(barrier_trigger))
{
}

future<schema_ptr> schema_store::resolve_and_upgrade(table_id table, const schema_changed& announced, frozen_mutation& m) {
    if (m.schema_version() != announced.version) {
        on_internal_error(logger, fmt::format("a write of table {} has schema version {}, but the announced one is {}",
                table, m.schema_version(), announced.version));
    }
    auto it = _schema_mappings.find(announced.version);
    if (it == _schema_mappings.end()) {
        auto entry = co_await get_schema(table, announced);
        if (!entry.schema && _barrier_trigger) {
            logger.debug("Table {} has no local schema version {}, running a group0 read barrier.",
                table, announced.version);
            co_await _barrier_trigger();
            entry = co_await get_schema(table, announced);
            if (entry.schema) {
                logger.debug("Table {} has schema version {} after the group0 read barrier.", table, announced.version);
            }
        }
        if (!entry.schema) {
            // The version is older than the table's: it is not the table's, and the barrier
            // above brought the table at least to what the leader announced from. Replay
            // runs no barrier, so there the mapping itself has to tell.
            // `db.find_schema()` may throw `replica::no_such_column_family` if the table was already dropped.
            entry.schema = _db.find_schema(table);
            if (!column_mapping_fits(announced.mapping, *entry.schema)) {
                on_internal_error(logger, fmt::format("schema version {} of table {} is unknown, and its column "
                        "mapping names columns the table's schema {} never had",
                        announced.version, table, entry.schema->version()));
            }
            logger.trace("Upgrading writes of table {} from schema version {} to {}.",
                table, announced.version, entry.schema->version());
            entry.mapping = announced.mapping;
        }
        it = _schema_mappings.insert({announced.version, std::move(entry)}).first;
    }

    const auto& [schema, mapping] = it->second;
    if (mapping) {
        m = freeze(m.unfreeze_upgrading(schema, *mapping));
    }
    co_return schema;
}

std::unique_ptr<raft_state_machine> make_state_machine(locator::global_tablet_id tablet,
    raft::group_id gid,
    replica::database& db,
    service::migration_manager& mm,
    db::system_keyspace& sys_ks,
    raft_groups_storage& persistence,
    raft_resize_tracker& resize_tracker,
    lw_shared_ptr<state_machine_stats> stats)
{
    return std::make_unique<state_machine>(tablet, gid, db, mm, sys_ks, persistence, resize_tracker, std::move(stats));
}

future<> apply_truncate_command(replica::database& db, locator::global_tablet_id tablet, raft::group_id gid,
        raft::index_t idx, const truncate_command& tc, truncate_record& record) {
    if (record.last_truncate_request_id == tc.request_id) {
        logger.debug("group {}: truncate of tablet {} by request {} was already applied, skipping",
                gid, tablet, tc.request_id);
        co_return;
    }
    logger.info("group {}: truncating tablet {} at {} by request {}",
            gid, tablet, tc.truncated_at, tc.request_id);
    co_await db.find_column_family(tablet.table).truncate_tablet_locally(db, tablet.tablet);
    record = truncate_record{.truncated_at = tc.truncated_at, .last_truncate_request_id = tc.request_id, .truncate_index = idx,
            .written_at = record.written_at};
    // Durable before returning, so no entry after this one is applied until the record is.
    // Otherwise a crash could leave an sstable of post-truncate writes without the record,
    // and the replayed entry would delete it.
    co_await utils::get_local_injector().inject("truncate_apply_before_record", utils::wait_for_message(5min));
    co_await raft_groups_storage::store_truncate_record(db, gid, this_shard_id(), record);
}

namespace detail {

raft_command deserialize_raft_command(const raft::log_entry_ptr& entry) {
    const auto& cmd = std::get<raft::command>(entry->data);
    auto is = ser::as_input_stream(cmd);
    return ser::deserialize(is, std::type_identity<raft_command>());
}

} // namespace detail
}; // namespace service::strong_consistency

auto fmt::formatter<service::strong_consistency::resize_marker_kind>::format(service::strong_consistency::resize_marker_kind kind,
        fmt::format_context& ctx) const -> decltype(ctx.out()) {
    using enum service::strong_consistency::resize_marker_kind;
    std::string_view name;
    switch (kind) {
    case start_resize: name = "start_resize"; break;
    case end_resize: name = "end_resize"; break;
    }
    return fmt::formatter<string_view>::format(name, ctx);
}
