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
#include "db/schema_tables.hh"
#include "mutation/frozen_mutation.hh"
#include "schema/schema_registry.hh"
#include "serializer_impl.hh"
#include "idl/strong_consistency/state_machine.dist.hh"
#include "idl/strong_consistency/state_machine.dist.impl.hh"
#include "replica/database.hh"
#include "service/migration_manager.hh"
#include "db/system_keyspace.hh"
#include "utils/loading_cache.hh"
#include "utils/error_injection.hh"
#include "schema/schema_registry.hh"
#include "service/strong_consistency/raft_commitlog.hh"

using namespace std::chrono_literals;

namespace service::strong_consistency {

static logging::logger logger("sc_state_machine");

class state_machine : public raft_state_machine {
    locator::global_tablet_id _tablet;
    raft::group_id _group_id;
    replica::database& _db;
    service::migration_manager& _mm;
    db::system_keyspace& _sys_ks;
    raft_groups_storage& _persistence;

    abort_source _as;

public:
    state_machine(locator::global_tablet_id tablet,
        raft::group_id gid,
        replica::database& db,
        service::migration_manager& mm,
        db::system_keyspace& sys_ks,
        raft_groups_storage& persistence)
        : _tablet(tablet)
        , _group_id(gid)
        , _db(db)
        , _mm(mm)
        , _sys_ks(sys_ks)
        , _persistence(persistence)
    {
    }

    future<> apply(raft::log_entry_ptr_list command) override {
        static thread_local logging::logger::rate_limit rate_limit(std::chrono::seconds(10));
        try {
            co_await utils::get_local_injector().inject("strong_consistency_state_machine_wait_before_apply", utils::wait_for_message(20min));
            // Collect mutations from the command list.
            utils::chunked_vector<frozen_mutation> muts;
            muts.reserve(command.size());
            for (auto& log_entry : command) {
                auto mutation = detail::deserialize_to_frozen_mutation(log_entry);
                muts.emplace_back(std::move(mutation));
            }
            // Get replay positions for the commands.
            auto replay_positions = _persistence.acquire_replay_position_handles_for(command);
            // Make sure we got replay positions for all commands.
            throwing_assert(replay_positions.size() == command.size());
            // Get schemas for all mutations (also upgrades mutations to current schema if needed).
            auto barrier = [this]() -> future<> {
                if (utils::get_local_injector().enter("disable_raft_drop_append_entries_for_specified_group")) {
                    utils::get_local_injector().disable("raft_drop_incoming_append_entries_for_specified_group");
                }
                co_await _mm.get_group0_barrier().trigger(false, &_as);
            };
            auto schemas = co_await resolve_and_upgrade_mutations(muts, _tablet.table, _db, _sys_ks, barrier);
            // Apply mutations in order. Writes are started sequentially to preserve
            // strong consistency ordering, then we wait for all to complete.
            // E.g., for writes A-B-C-D, a reader must observe
            // them in that order and never see A-C or A-D skipping intermediate values.
            std::vector<future<>> apply_futures;
            apply_futures.reserve(muts.size());
            for (size_t i = 0; i < command.size(); ++i) {
                throwing_assert(replay_positions[i].index == command[i]->idx);
                apply_futures.emplace_back(_db.apply_in_memory(muts[i], schemas[i], std::move(replay_positions[i].replay_position_handle), db::no_timeout, db::noop_large_data_guardrail::instance()));
            }
            co_await when_all_succeed(apply_futures.begin(), apply_futures.end());
        } catch (replica::no_such_column_family&) {
            // If the table doesn't exist, it means it was already dropped.
            // This cannot happen if the table wasn't created yet on the node
            // because the state machine is created only after the table is created
            // (see `schema_applier::commit_on_shard()` and `storage_service::commit_token_metadata_change()`).
            // In this case, we should just ignore mutations without throwing an error.
            logger.log(log_level::warn, rate_limit, "apply(): table {} was already dropped, ignoring mutations", _tablet.table);
        } catch (replica::no_such_keyspace&) {
            // Thrown when DROP KEYSPACE races with the raft applier fiber.
            // The path: resolve_and_upgrade_mutations() calls
            // local_schema_registry().get_or_null(schema_version), which may
            // trigger lazy unfreezing of a cached frozen_schema. Unfreezing
            // re-parses column types via cql_type_parser::parse(), which
            // consults db_user_types_storage::get(ks_name), which calls
            // database::find_keyspace() — and that throws no_such_keyspace
            // if the keyspace was concurrently dropped.
            // Safe to ignore: the table's raft group is about to be destroyed
            // by schedule_raft_group_deletion() anyway.
            logger.log(log_level::warn, rate_limit, "apply(): keyspace for table {} was already dropped, ignoring mutations", _tablet.table);
        } catch (const abort_requested_exception& ex) {
            // The exception can be thrown by get_schema_and_upgrade_mutations.
            // It means that the Raft group is being removed.
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
            throw std::runtime_error(::format(
                "tablet {}, group id {}: error while applying mutations {}",
                _tablet, _group_id, std::current_exception()));
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

    future<> transfer_snapshot(raft::server_id from_id, raft::snapshot_descriptor snp) override {
        throw std::runtime_error("transfer_snapshot() not implemented");
    }
};

using column_mappings_cache = utils::loading_cache<table_schema_version, column_mapping>;
using schema_entry = std::pair<schema_ptr, column_mappings_cache::value_ptr>;

future<std::vector<schema_ptr>> resolve_and_upgrade_mutations(utils::chunked_vector<frozen_mutation>& muts, table_id table, replica::database& db,
        db::system_keyspace& sys_ks, std::function<future<>()> barrier_trigger) {
    // Cache column mappings to avoid querying `system.scylla_table_schema_history` multiple times.
    static thread_local column_mappings_cache column_mapping_cache(std::numeric_limits<size_t>::max(), 1h, logger);
    using schema_store = std::unordered_map<table_schema_version, schema_entry>;
    // Stores schema pointer and optional column mapping for each schema version present in the mutations
    schema_store schema_mappings;
    bool barrier_executed = false;

    auto get_schema = [&] (table_schema_version schema_version) -> future<schema_entry> {
        if (utils::get_local_injector().enter("sc_state_machine_return_empty_schema")) {
            co_return schema_entry{nullptr, nullptr};
        }

        auto schema = local_schema_registry().get_or_null(schema_version);
        if (schema) {
            co_return schema_entry{std::move(schema), nullptr};
        }

        // `db.find_schema()` may throw `replica::no_such_column_family` if the table was already dropped.
        schema = db.find_schema(table);
        // The column mapping may be already present in the cache from another call
        auto cm_ptr = column_mapping_cache.find(schema_version);
        if (cm_ptr) {
            co_return schema_entry{std::move(schema), std::move(cm_ptr)};
        }

        // We may not find the column mapping if the mutation schema is newer than the present schema.
        // In this case, we should trigger the barrier to wait for the schema to be updated and then try again.
        auto cm_opt = co_await db::schema_tables::get_column_mapping_if_exists(sys_ks, table, schema_version);
        if (!cm_opt) {
            co_return schema_entry{nullptr, nullptr};
        }

        cm_ptr = co_await column_mapping_cache.get_ptr(schema_version, [cm = std::move(*cm_opt)](auto schema_version) -> future<column_mapping> {
            co_return std::move(cm);
        });
        co_return schema_entry{std::move(schema), std::move(cm_ptr)};
    };

    auto resolve_schema = [&] (const frozen_mutation& mut) -> future<const schema_store::mapped_type*> {
        auto schema_version = mut.schema_version();
        auto it = schema_mappings.find(schema_version);
        if (it != schema_mappings.end()) {
            co_return &it->second;
        }

        auto schema_cm = co_await get_schema(schema_version);
        if (!schema_cm.first && barrier_trigger && !barrier_executed) {
            co_await barrier_trigger();
            barrier_executed = true;
            schema_cm = co_await get_schema(schema_version);
        }

        if (schema_cm.first) {
            const auto [it, _] = schema_mappings.insert({schema_version, std::move(schema_cm)});
            co_return &it->second;
        }
        co_return nullptr;
    };

    // Build parallel vector of schema pointers (one per mutation).
    // We can't return the map keyed by schema_version because upgrade
    // changes a mutation's schema_version to the current one.
    std::vector<schema_ptr> result;
    result.reserve(muts.size());
    for (auto& m : muts) {
        auto entry = co_await resolve_schema(m);
        if (!entry) {
            // Old schema are TTLed after 10 days (see comment in `schema_applier::finalize_tables_and_views()`),
            // so this error theoretically may be triggered if a node is stuck longer than this.
            // But in practice we should do a snapshot much earlier, that's why `on_internal_error()` here.
            // And if the table or keyspace was already dropped, `no_such_column_family`
            // or `no_such_keyspace` will be thrown earlier.
            on_internal_error(logger, fmt::format("couldn't find schema for table {} and mutation schema version {}", table, m.schema_version()));
        }
        if (entry->second) {
            m = freeze(m.unfreeze_upgrading(entry->first, *entry->second));
        }
        result.push_back(entry->first);
    }
    co_return std::move(result);
}

std::unique_ptr<raft_state_machine> make_state_machine(locator::global_tablet_id tablet,
    raft::group_id gid,
    replica::database& db,
    service::migration_manager& mm,
    db::system_keyspace& sys_ks,
    raft_groups_storage& persistence)
{
    return std::make_unique<state_machine>(tablet, gid, db, mm, sys_ks, persistence);
}

namespace detail {

frozen_mutation deserialize_to_frozen_mutation(const raft::log_entry_ptr& entry) {
    const auto& cmd = std::get<raft::command>(entry->data);
    auto is = ser::as_input_stream(cmd);
    auto command = ser::deserialize(is, std::type_identity<raft_command>());
    return std::move(command.mutation);
}

} // namespace detail
}; // namespace service::strong_consistency
