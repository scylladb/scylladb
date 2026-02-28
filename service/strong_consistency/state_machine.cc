/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

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

using namespace std::chrono_literals;

namespace service::strong_consistency {

static logging::logger logger("sc_state_machine");

class state_machine : public raft_state_machine {
    locator::global_tablet_id _tablet;
    raft::group_id _group_id;
    replica::database& _db;
    service::migration_manager& _mm;
    db::system_keyspace& _sys_ks;

public:
    state_machine(locator::global_tablet_id tablet,
        raft::group_id gid,
        replica::database& db,
        service::migration_manager& mm,
        db::system_keyspace& sys_ks)
        : _tablet(tablet)
        , _group_id(gid)
        , _db(db)
        , _mm(mm)
        , _sys_ks(sys_ks)
    {
    }

    future<> apply(std::vector<raft::command_cref> command) override {
        try {
            co_await utils::get_local_injector().inject("strong_consistency_state_machine_wait_before_apply", utils::wait_for_message(20min));
            utils::chunked_vector<frozen_mutation> muts;
            muts.reserve(command.size());
            for (const auto& c: command) {
                auto is = ser::as_input_stream(c);
                auto cmd = ser::deserialize(is, std::type_identity<raft_command>{});
                muts.push_back(std::move(cmd.mutation));
            }
            // Hold pointers to schemas until `_db.apply()` is finished
            auto schemas = co_await get_schema_and_upgrade_mutations(muts);
            co_await _db.apply(std::move(muts), db::no_timeout);
        } catch (replica::no_such_column_family&) {
            // This exception means that the table was already dropped
            logger.warn("apply(): got replica::no_such_column_family exception, ignoring mutations");
        } catch (...) {
            throw std::runtime_error(::format(
                "tablet {}, group id {}: error while applying mutations {}",
                _tablet, _group_id, std::current_exception()));
        }
    }

    future<raft::snapshot_id> take_snapshot() override {
        throw std::runtime_error("take_snapshot() not implemented");
    }

    void drop_snapshot(raft::snapshot_id id) override {
        throw std::runtime_error("drop_snapshot() not implemented");
    }

    future<> load_snapshot(raft::snapshot_id id) override {
        return make_ready_future<>();
    }

    future<> abort() override {
        return make_ready_future<>();
    }

    future<> transfer_snapshot(raft::server_id from_id, raft::snapshot_descriptor snp) override {
        throw std::runtime_error("transfer_snapshot() not implemented");
    }

private:
    using schema_store = std::unordered_map<table_schema_version, std::pair<schema_ptr, std::optional<column_mapping>>>;
    future<schema_store> get_schema_and_upgrade_mutations(utils::chunked_vector<frozen_mutation>& muts) {
        // Stores schema pointer and optional column mapping for each schema version present in the mutations
        schema_store schema_mappings;
        bool barrier_executed = false;

        auto get_schema = [&] (table_schema_version schema_version) -> future<std::pair<schema_ptr, std::optional<column_mapping>>> {
            auto schema = local_schema_registry().get_or_null(schema_version);
            if (schema) {
                co_return std::pair{std::move(schema), std::nullopt};
            }

            auto cm_opt = co_await db::schema_tables::get_column_mapping_if_exists(_sys_ks, _tablet.table, schema_version);
            if (cm_opt) {
                // If the table is already dropped, this will throw `no_such_column_family`
                schema = _db.find_schema(_tablet.table);
                co_return std::pair{std::move(schema), std::move(cm_opt)};
            }

            co_return std::pair{nullptr, std::nullopt};
        };

        auto resolve_schema = [&] (const frozen_mutation& mut) -> future<const schema_store::mapped_type*> {
            auto schema_version = mut.schema_version();
            auto it = schema_mappings.find(schema_version);
            if (it != schema_mappings.end()) {
                co_return &it->second;
            }

            auto schema_cm = co_await get_schema(schema_version);
            if (!schema_cm.first && !barrier_executed) {
                // TODO: pass valid abort source
                co_await _mm.get_group0_barrier().trigger();
                barrier_executed = true;

                schema_cm = co_await get_schema(schema_version);
            }

            if (schema_cm.first) {
                const auto [it, _] = schema_mappings.insert({schema_version, std::move(schema_cm)});
                co_return &it->second;
            }
            co_return nullptr;
        };

        for (auto& m: muts) {
            auto schema_entry = co_await resolve_schema(m);
            if (!schema_entry) {
                // Old schema are TTLed after 10 days (see comment in `schema_applier::finalize_tables_and_views()`),
                // so this error theoretically  may be triggered if a node is stuck longer than this.
                // But in practise we should do a snapshot much earlier, that's why `on_internal_error()` here.
                //
                // And if the table was already dropped, `no_such_column_family` will be dropped earlier.
                on_internal_error(logger, fmt::format("couldn't find schema for table {} and mutation schema version {}", _tablet.table, m.schema_version()));
            }
            if (schema_entry->second) {
                m = freeze(m.unfreeze_upgrading(schema_entry->first, *schema_entry->second));
            }   
        }

        // We only need vector of schema pointers but we're returning the whole map
        // to avoid another allocation
        co_return std::move(schema_mappings);
    }
};

std::unique_ptr<raft_state_machine> make_state_machine(locator::global_tablet_id tablet,
    raft::group_id gid,
    replica::database& db,
    service::migration_manager& mm,
    db::system_keyspace& sys_ks)
{
    return std::make_unique<state_machine>(tablet, gid, db, mm, sys_ks);
}

};