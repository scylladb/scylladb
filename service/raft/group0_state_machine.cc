/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include "service/raft/group0_state_machine.hh"
#include "db/schema_tables.hh"
#include "mutation/atomic_cell.hh"
#include "cql3/selection/selection.hh"
#include "dht/i_partitioner.hh"
#include "dht/token.hh"
#include "message/messaging_service.hh"
#include "mutation/canonical_mutation.hh"
#include "mutation/async_utils.hh"
#include <seastar/core/abort_source.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "service/broadcast_tables/experimental/query_result.hh"
#include "schema_mutations.hh"
#include "frozen_schema.hh"
#include "serialization_visitors.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "idl/uuid.dist.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/frozen_schema.dist.hh"
#include "idl/frozen_schema.dist.impl.hh"
#include "idl/experimental/broadcast_tables_lang.dist.hh"
#include "idl/experimental/broadcast_tables_lang.dist.impl.hh"
#include "service/storage_service.hh"
#include "idl/storage_service.dist.hh"
#include "idl/group0_state_machine.dist.hh"
#include "idl/group0_state_machine.dist.impl.hh"
#include "service/migration_manager.hh"
#include "db/system_keyspace.hh"
#include "service/storage_proxy.hh"
#include "service/raft/raft_group0_client.hh"
#include "partition_slice_builder.hh"
#include "timestamp.hh"
#include "utils/overloaded_functor.hh"
#include "utils/to_string.hh"
#include <optional>
#include "db/config.hh"
#include "replica/database.hh"
#include "replica/tablets.hh"
#include "service/raft/group0_state_machine_merger.hh"
#include "gms/feature_service.hh"
#include "idl/migration_manager.dist.hh"
#include "build_mode.hh"

namespace service {

static logging::logger slogger("group0_raft_sm");

group0_state_machine::group0_state_machine(raft_group0_client& client, migration_manager& mm, storage_proxy& sp, storage_service& ss,
        group0_server_accessor server_accessor, gms::gossiper& gossiper, gms::feature_service& feat,
        bool topology_change_enabled)
    : _client(client), _mm(mm), _sp(sp), _ss(ss)
    , _gate("group0_state_machine")
    , _topology_change_enabled(topology_change_enabled)
    , _state_id_handler(sp.local_db(), gossiper, server_accessor), _feature_service(feat)
    , _topology_on_raft_support_listener(feat.supports_consistent_topology_changes.when_enabled([this] () noexcept {
        // Using features to decide whether to start fetching topology snapshots
        // or not is technically not correct because we also use features to guard
        // whether upgrade can be started, and upgrade starts by writing
        // to the system.topology table (namely, to the `upgrade_state` column).
        // If some node at that point didn't mark the feature as enabled
        // locally, there is a risk that it might try to pull a snapshot
        // and will decide not to use `raft_pull_snapshot` verb.
        //
        // The above issue is mitigated by requiring administrators to
        // wait until the SUPPORTS_CONSISTENT_TOPOLOGY_CHANGES feature
        // is enabled on all nodes.
        //
        // The biggest value of using a cluster feature here is so that
        // the node won't try to fetch a topology snapshot if the other
        // node doesn't support it yet.
        _topology_change_enabled = true;
    })) {
    _state_id_handler.run();
}

static mutation extract_history_mutation(std::vector<canonical_mutation>& muts, const data_dictionary::database db) {
    auto s = db.find_schema(db::system_keyspace::NAME, db::system_keyspace::GROUP0_HISTORY);
    auto it = std::find_if(muts.begin(), muts.end(), [history_table_id = s->id()]
            (canonical_mutation& m) { return m.column_family_id() == history_table_id; });
    if (it == muts.end()) {
        on_internal_error(slogger, "group0 history table mutation not found");
    }
    auto res = it->to_mutation(s);
    muts.erase(it);
    return res;
}

static future<> mutate_locally(utils::chunked_vector<canonical_mutation> muts, storage_proxy& sp) {
    auto db = sp.data_dictionary();
    co_await max_concurrent_for_each(muts, 128, [&sp, &db] (const canonical_mutation& cmut) -> future<> {
        auto schema = db.find_schema(cmut.column_family_id());
        return sp.mutate_locally(cmut.to_mutation(schema), nullptr, db::commitlog::force_sync::yes);
    });
}

bool should_flush_system_topology_after_applying(const mutation& mut, const data_dictionary::database db) {
    if (!db.has_schema(db::system_keyspace::NAME, db::system_keyspace::TOPOLOGY)) {
        return false;
    }

    auto s_topology = db.find_schema(db::system_keyspace::NAME, db::system_keyspace::TOPOLOGY);
    if (mut.column_family_id() == s_topology->id()) {
        auto enabled_features_id = s_topology->columns_by_name().at("enabled_features")->id;
        if (mut.partition().static_row().find_cell(enabled_features_id)) {
            return true;
        }
        auto supported_features_id = s_topology->columns_by_name().at("supported_features")->id;
        for (const auto& r : mut.partition().clustered_rows()) {
            if (r.row().cells().find_cell(supported_features_id) != nullptr) {
                return true;
            }
        }
    }
    return false;
}

future<> write_mutations_to_database(storage_proxy& proxy, gms::inet_address from, std::vector<canonical_mutation> cms) {
    std::vector<frozen_mutation_and_schema> mutations;
    mutations.reserve(cms.size());
    bool need_system_topology_flush = false;
    try {
        for (auto& cm : cms) {
            auto& tbl = proxy.local_db().find_column_family(cm.column_family_id());
            auto& s = tbl.schema();
            auto mut = co_await to_mutation_gently(cm, s);
            need_system_topology_flush = need_system_topology_flush || should_flush_system_topology_after_applying(mut, proxy.data_dictionary());
            mutations.emplace_back(co_await freeze_gently(mut), s);
        }
    } catch (replica::no_such_column_family& e) {
        slogger.error("Error while applying mutations from {}: {}", from, e);
        throw std::runtime_error(::format("Error while applying mutations: {}", e));
    }

    co_await proxy.mutate_locally(std::move(mutations), tracing::trace_state_ptr(), db::commitlog::force_sync::no);

    if (need_system_topology_flush) {
        slogger.trace("write_mutations_to_database: flushing {}.{}", db::system_keyspace::NAME, db::system_keyspace::TOPOLOGY);
        co_await proxy.get_db().local().flush(db::system_keyspace::NAME, db::system_keyspace::TOPOLOGY);
    }
}

group0_state_machine::modules_to_reload group0_state_machine::get_modules_to_reload(const std::vector<canonical_mutation>& mutations) {
    modules_to_reload modules;

    for (auto& mut: mutations) {
        modules.entries.push_back({.pk = mut.key(), .table = mut.column_family_id()});
    }

    return modules;
}

// Defines set of table_ids, which should reload view building state if any of the table is changed.
static const std::unordered_set<table_id>& get_view_building_state_tables() {
    static const std::unordered_set<table_id> ids {
        db::system_keyspace::view_building_tasks()->id(),
        db::schema_tables::v3::views()->id(),
        db::system_keyspace::view_build_status_v2()->id(),
    };
    return ids;
}

future<> group0_state_machine::reload_modules(modules_to_reload modules) {
    bool update_service_levels_cache = false;
    bool update_service_levels_effective_cache = false;
    bool make_view_building_state_transition = false;

    for (const auto& m : modules.entries) {
        if (m.table == db::system_keyspace::service_levels_v2()->id()) {
            update_service_levels_cache = true;
        } else if (m.table == db::system_keyspace::role_members()->id() || m.table == db::system_keyspace::role_attributes()->id()) {
            update_service_levels_effective_cache = true;
        } else if (m.table == db::system_keyspace::dicts()->id()) {
            auto pk_type = db::system_keyspace::dicts()->partition_key_type();
            auto name_value = pk_type->deserialize_value(m.pk.representation());
            auto name_string = value_cast<sstring>(pk_type->types().front()->deserialize(name_value.front()));
            co_await _ss.compression_dictionary_updated_callback(name_string);
        } else if (get_view_building_state_tables().contains(m.table)) {
            make_view_building_state_transition = true;
        } else if (m.table == db::system_keyspace::v3::scylla_local()->id()) {
            make_view_building_state_transition = true;
        }
    }
    
    if (update_service_levels_cache || update_service_levels_effective_cache) { // this also updates SL effective cache
        co_await _ss.update_service_levels_cache(qos::update_both_cache_levels(update_service_levels_cache), qos::query_context::group0);
    }
    if (make_view_building_state_transition) {
        co_await _ss.view_building_transition();
    }
}

future<> group0_state_machine::merge_and_apply(group0_state_machine_merger& merger) {
    auto [_cmd, history] = merger.merge();
    auto cmd = std::move(_cmd);

    // We assume that `cmd.change` was constructed using group0 state which was observed *after* `cmd.prev_state_id` was obtained.
    // It is now important that we apply the change *before* we append the group0 state ID to the history table.
    //
    // If we crash before appending the state ID, when we reapply the command after restart, the change will be applied because
    // the state ID was not yet appended so the above check will pass.

    // TODO: reapplication of a command after a crash may require contacting a quorum (we need to learn that the command
    // is committed from a leader). But we may want to ensure that group 0 state is consistent after restart even without
    // access to quorum, which means we cannot allow partially applied commands. We need to ensure that either the entire
    // change is applied and the state ID is updated or none of this happens.
    // E.g. use a write-ahead-entry which contains all this information and make sure it's replayed during restarts.

    co_await std::visit(make_visitor(
    [&] (schema_change& chng) -> future<> {
        auto modules_to_reload = get_modules_to_reload(chng.mutations);
        co_await _mm.merge_schema_from(locator::host_id{cmd.creator_id.uuid()}, std::move(chng.mutations));
        co_await reload_modules(std::move(modules_to_reload));
    },
    [&] (broadcast_table_query& query) -> future<> {
        auto result = co_await service::broadcast_tables::execute_broadcast_table_query(_sp, query.query, cmd.new_state_id);
        _client.set_query_result(cmd.new_state_id, std::move(result));
    },
    [&] (topology_change& chng) -> future<> {
        auto modules_to_reload = get_modules_to_reload(chng.mutations);
        auto tablet_keys = replica::get_tablet_metadata_change_hint(chng.mutations);
        co_await write_mutations_to_database(_sp, cmd.creator_addr, std::move(chng.mutations));
        co_await _ss.topology_transition({.tablets_hint = std::move(tablet_keys)});
        co_await reload_modules(std::move(modules_to_reload));
    },
    [&] (mixed_change& chng) -> future<> {
        auto modules_to_reload = get_modules_to_reload(chng.mutations);
        co_await _mm.merge_schema_from(locator::host_id{cmd.creator_id.uuid()}, std::move(chng.mutations));
        co_await _ss.topology_transition();
        co_await reload_modules(std::move(modules_to_reload));
    },
    [&] (write_mutations& muts) -> future<> {
        auto modules_to_reload = get_modules_to_reload(muts.mutations);
        co_await write_mutations_to_database(_sp, cmd.creator_addr, std::move(muts.mutations));
        co_await reload_modules(std::move(modules_to_reload));
    }
    ), cmd.change);

    co_await _sp.mutate_locally({std::move(history)}, nullptr);
}

#ifndef SCYLLA_BUILD_MODE_RELEASE
static void ensure_group0_schema(const group0_command& cmd, const replica::database& db) {
    auto validate_schema = [&db](const std::vector<canonical_mutation>& mutations) {
        for (const auto& mut : mutations) {
            // Get the schema for the column family
            auto schema = db.find_schema(mut.column_family_id());
            if (!schema) {
                on_internal_error(slogger, "ensure_group0_schema: schema not found");
            }

            if (!schema->static_props().is_group0_table) {
                on_internal_error(slogger, fmt::format("ensure_group0_schema: schema is not group0: {}", schema->cf_name()));
            }
        }
    };

    std::visit(overloaded_functor{
            [validate_schema](const schema_change& change) {
                validate_schema(change.mutations);
            },
            [](const broadcast_table_query&) {
                // no mutations to validate
            },
            [validate_schema](const topology_change& change) {
                validate_schema(change.mutations);
            },
            [validate_schema](const write_mutations& change) {
                validate_schema(change.mutations);
            },
            [validate_schema](const mixed_change& change) {
                validate_schema(change.mutations);
            },
        }, cmd.change);
}
#endif

future<> group0_state_machine::apply(std::vector<raft::command_cref> command) {
    slogger.trace("apply() is called with {} commands", command.size());

    co_await utils::get_local_injector().inject("group0_state_machine::delay_apply", 1s);

    auto read_apply_mutex_holder = co_await _client.hold_read_apply_mutex(_abort_source);

    // max_mutation_size = 1/2 of commitlog segment size, thus max_command_size is set 1/3 of commitlog segment size to leave space for metadata.
    size_t max_command_size = _sp.data_dictionary().get_config().commitlog_segment_size_in_mb() * 1024 * 1024 / 3;
    group0_state_machine_merger m(co_await _client.sys_ks().get_last_group0_state_id(), std::move(read_apply_mutex_holder),
                                  max_command_size, _sp.data_dictionary());

    for (auto&& c : command) {
        auto is = ser::as_input_stream(c);
        auto cmd = ser::deserialize(is, std::type_identity<group0_command>{});

#ifndef SCYLLA_BUILD_MODE_RELEASE
        // Ensure that the schema of the mutations is a group0 schema.
        // This validation is supposed to be only performed in tests, so it is skipped in the release mode.
        ensure_group0_schema(cmd, _client.sys_ks().local_db());
#endif

        slogger.trace("cmd: prev_state_id: {}, new_state_id: {}, creator_addr: {}, creator_id: {}",
                cmd.prev_state_id, cmd.new_state_id, cmd.creator_addr, cmd.creator_id);
        slogger.trace("cmd.history_append: {}", cmd.history_append);

        if (cmd.prev_state_id) {
            if (*cmd.prev_state_id != m.last_id()) {
                // This command used obsolete state. Make it a no-op.
                // BTW. on restart, all commands after last snapshot descriptor become no-ops even when they originally weren't no-ops.
                // This is because we don't restart from snapshot descriptor, but using current state of the tables so the last state ID
                // is the one given by the last command.
                // Similar thing may happen when we pull group0 state in transfer_snapshot - we pull the latest state of remote tables,
                // not state at the snapshot descriptor.
                slogger.trace("cmd.prev_state_id ({}) different than last group 0 state ID in history table ({})",
                        cmd.prev_state_id, m.last_id());
                continue;
            }
        } else {
            slogger.trace("unconditional modification, cmd.new_state_id: {}", cmd.new_state_id);
        }

        auto size = m.cmd_size(cmd);
        if (!m.can_merge(cmd, size)) {
            co_await merge_and_apply(m);
        }

        m.add(std::move(cmd), size);
    }

    if (!m.empty()) {
        // apply remainder
        co_await merge_and_apply(m);
    }

    co_await _state_id_handler.advertise_state_id(m.last_id());
}

future<raft::snapshot_id> group0_state_machine::take_snapshot() {
    return make_ready_future<raft::snapshot_id>(raft::snapshot_id::create_random_id());
}

void group0_state_machine::drop_snapshot(raft::snapshot_id id) {
    (void) id;
}

future<> group0_state_machine::load_snapshot(raft::snapshot_id id) {
    // topology_state_load applies persisted state machine state into
    // memory and thus needs to be protected with apply mutex
    auto read_apply_mutex_holder = co_await _client.hold_read_apply_mutex(_abort_source);
    co_await _ss.topology_state_load();
    co_await _ss.view_building_state_load();
    if (_feature_service.compression_dicts) {
        co_await _ss.compression_dictionary_updated_callback_all();
    }
    _ss._topology_state_machine.event.broadcast();
    _ss._view_building_state_machine.event.broadcast();
}

future<> group0_state_machine::transfer_snapshot(raft::server_id from_id, raft::snapshot_descriptor snp) {
  try {
    // Note that this may bring newer state than the group0 state machine raft's
    // log, so some raft entries may be double applied, but since the state
    // machine is idempotent it is not a problem.
    locator::host_id hid{from_id.uuid()};

    auto holder = _gate.hold();

    slogger.trace("transfer snapshot from {} index {} snp id {}", hid, snp.idx, snp.id);
    auto& as = _abort_source;

    // (Ab)use MIGRATION_REQUEST to also transfer group0 history table mutation besides schema tables mutations.
    auto [_, cm] = co_await ser::migration_manager_rpc_verbs::send_migration_request(&_mm._messaging, hid, as, netw::schema_pull_options { .group0_snapshot_transfer = true });
    if (!cm) {
        // If we're running this code then remote supports Raft group 0, so it should also support canonical mutations
        // (which were introduced a long time ago).
        on_internal_error(slogger, "Expected MIGRATION_REQUEST to return canonical mutations");
    }

    std::optional<service::raft_snapshot> topology_snp;
    std::optional<service::raft_snapshot> raft_snp;

    if (_topology_change_enabled) {
        auto auth_tables = db::system_keyspace::auth_tables();
        std::vector<table_id> tables;
        tables.reserve(3);
        tables.push_back(db::system_keyspace::topology()->id());
        tables.push_back(db::system_keyspace::topology_requests()->id());
        tables.push_back(db::system_keyspace::cdc_generations_v3()->id());

        topology_snp = co_await ser::storage_service_rpc_verbs::send_raft_pull_snapshot(
            &_mm._messaging, hid, as, from_id, service::raft_snapshot_pull_params{std::move(tables)});

        tables = std::vector<table_id>();
        tables.reserve(auth_tables.size() + 1);

        for (const auto& schema : auth_tables) {
            tables.push_back(schema->id());
        }
        tables.push_back(db::system_keyspace::service_levels_v2()->id());

        raft_snp = co_await ser::storage_service_rpc_verbs::send_raft_pull_snapshot(
            &_mm._messaging, hid, as, from_id, service::raft_snapshot_pull_params{std::move(tables)});
    }

    auto history_mut = extract_history_mutation(*cm, _sp.data_dictionary());

    // TODO ensure atomicity of snapshot application in presence of crashes (see TODO in `apply`)

    auto read_apply_mutex_holder = co_await _client.hold_read_apply_mutex(as);

    co_await _mm.merge_schema_from(hid, std::move(*cm));

    if (topology_snp && !topology_snp->mutations.empty()) {
        co_await _ss.merge_topology_snapshot(std::move(*topology_snp));
        // Flush so that current supported and enabled features are readable before commitlog replay
        co_await _sp.get_db().local().flush(db::system_keyspace::NAME, db::system_keyspace::TOPOLOGY);
    }

    if (raft_snp) {
        co_await mutate_locally(std::move(raft_snp->mutations), _sp);
    }

    co_await _sp.mutate_locally({std::move(history_mut)}, nullptr);
  } catch (const abort_requested_exception&) {
    throw raft::request_aborted(fmt::format(
        "Abort requested while transferring snapshot from ID: {}, snapshot descriptor id: {}, snapshot index: {}", from_id, snp.id, snp.idx));
  }
}

future<> group0_state_machine::abort() {
    _abort_source.request_abort();
    return _gate.close();
}

} // end of namespace service
