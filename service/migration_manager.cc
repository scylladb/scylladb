/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "auth/resource.hh"
#include "schema/schema_registry.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "service/raft/group0_state_machine.hh"

#include "service/migration_listener.hh"
#include "message/messaging_service.hh"
#include "gms/feature_service.hh"
#include "utils/runtime.hh"
#include "gms/gossiper.hh"
#include "view_info.hh"
#include "schema/schema_builder.hh"
#include "replica/database.hh"
#include "replica/tablets.hh"
#include "db/schema_tables.hh"
#include "types/user.hh"
#include "db/system_keyspace.hh"
#include "cql3/functions/user_aggregate.hh"
#include "cql3/functions/user_function.hh"
#include "cql3/functions/function_name.hh"

namespace service {

static logging::logger mlogger("migration_manager");

using namespace std::chrono_literals;

const std::chrono::milliseconds migration_manager::migration_delay = 60000ms;
static future<schema_ptr> get_schema_definition(table_schema_version v, netw::messaging_service::msg_addr dst, netw::messaging_service& ms, service::storage_proxy& sp);

migration_manager::migration_manager(migration_notifier& notifier, gms::feature_service& feat, netw::messaging_service& ms,
            service::storage_proxy& storage_proxy, gms::gossiper& gossiper, service::raft_group0_client& group0_client, sharded<db::system_keyspace>& sysks) :
          _notifier(notifier)
        , _group0_barrier(this_shard_id() == 0 ?
            std::function<future<>()>([this] () -> future<> {
                // This will run raft barrier and will sync schema with the leader
                (void)co_await start_group0_operation();
            }) :
            std::function<future<>()>([this] () -> future<> {
                co_await container().invoke_on(0, [] (migration_manager& mm) -> future<> {
                    // batch group0 raft barriers
                    co_await mm._group0_barrier.trigger();
                });
            })
        )
        , _feat(feat), _messaging(ms), _storage_proxy(storage_proxy), _gossiper(gossiper), _group0_client(group0_client)
        , _sys_ks(sysks)
        , _schema_push([this] { return passive_announce(); })
        , _concurrent_ddl_retries{10}
{
    init_messaging_service();
}

future<> migration_manager::stop() {
    if (!_as.abort_requested()) {
        co_await drain();
    }
    try {
        co_await _schema_push.join();
    } catch (...) {
        mlogger.error("schema_push failed: {}", std::current_exception());
    }
}

future<> migration_manager::drain()
{
    mlogger.info("stopping migration service");
    _as.request_abort();

    co_await uninit_messaging_service();
    try {
        co_await coroutine::parallel_for_each(_schema_pulls, [] (auto&& e) {
            return e.second.join();
        });
    } catch (...) {
        mlogger.error("schema_pull failed: {}", std::current_exception());
    }
    co_await _group0_barrier.join();
    co_await _background_tasks.close();
}

void migration_manager::init_messaging_service()
{
    auto update_schema = [this] {
        //FIXME: future discarded.
        (void)with_gate(_background_tasks, [this] {
            mlogger.debug("features changed, recalculating schema version");
            return db::schema_tables::recalculate_schema_version(_sys_ks, _storage_proxy.container(), _feat);
        });
    };

    if (this_shard_id() == 0) {
        _feature_listeners.push_back(_feat.view_virtual_columns.when_enabled(update_schema));
        _feature_listeners.push_back(_feat.digest_insensitive_to_expiry.when_enabled(update_schema));
        _feature_listeners.push_back(_feat.cdc.when_enabled(update_schema));
        _feature_listeners.push_back(_feat.per_table_partitioners.when_enabled(update_schema));

        if (!_feat.table_digest_insensitive_to_expiry) {
            _feature_listeners.push_back(_feat.table_digest_insensitive_to_expiry.when_enabled([this] {
                (void) with_gate(_background_tasks, [this] {
                    return reload_schema();
                });
            }));
        }
    }

    _messaging.register_definitions_update([this] (const rpc::client_info& cinfo, std::vector<frozen_mutation> fm, rpc::optional<std::vector<canonical_mutation>> cm) {
        auto src = netw::messaging_service::get_source(cinfo);
        auto f = make_ready_future<>();
        if (cm) {
            f = do_with(std::move(*cm), [this, src] (const std::vector<canonical_mutation>& mutations) {
                return merge_schema_in_background(src, mutations);
            });
        } else {
            f = do_with(std::move(fm), [this, src] (const std::vector<frozen_mutation>& mutations) {
                return merge_schema_in_background(src, mutations);
            });
        }
        // Start a new fiber.
        (void)f.then_wrapped([src] (auto&& f) {
            if (f.failed()) {
                mlogger.error("Failed to update definitions from {}: {}", src, f.get_exception());
            } else {
                mlogger.debug("Applied definitions update from {}.", src);
            }
        });
        return netw::messaging_service::no_wait();
    });
    _messaging.register_migration_request(std::bind_front(
            [] (migration_manager& self, const rpc::client_info& cinfo, rpc::optional<netw::schema_pull_options> options)
                -> future<rpc::tuple<std::vector<frozen_mutation>, std::vector<canonical_mutation>>> {
        const auto cm_retval_supported = options && options->remote_supports_canonical_mutation_retval;

        auto features = self._feat.cluster_schema_features();
        auto& proxy = self._storage_proxy.container();
        auto& db = proxy.local().get_db();
        auto cm = co_await db::schema_tables::convert_schema_to_mutations(proxy, features);
        if (options->group0_snapshot_transfer) {
            // if `group0_snapshot_transfer` is `true`, the sender must also understand canonical mutations
            // (`group0_snapshot_transfer` was added more recently).
            if (!cm_retval_supported) {
                on_internal_error(mlogger,
                    "migration request handler: group0 snapshot transfer requested, but canonical mutations not supported");
            }
            cm.emplace_back(co_await db::system_keyspace::get_group0_history(db));
            if (proxy.local().local_db().get_config().check_experimental(db::experimental_features_t::feature::TABLETS)) {
                for (auto&& m: co_await replica::read_tablet_mutations(db)) {
                    cm.emplace_back(std::move(m));
                }
            }
        }
        if (cm_retval_supported) {
            co_return rpc::tuple(std::vector<frozen_mutation>{}, std::move(cm));
        }
        auto fm = boost::copy_range<std::vector<frozen_mutation>>(cm | boost::adaptors::transformed([&db = db.local()] (const canonical_mutation& cm) {
            return cm.to_mutation(db.find_column_family(cm.column_family_id()).schema());
        }));
        co_return rpc::tuple(std::move(fm), std::move(cm));
    }, std::ref(*this)));
    _messaging.register_schema_check([this] {
        return make_ready_future<table_schema_version>(_storage_proxy.get_db().local().get_version());
    });
    _messaging.register_get_schema_version([this] (unsigned shard, table_schema_version v) {
        // FIXME: should this get an smp_service_group? Probably one separate from reads and writes.
        return container().invoke_on(shard, [v] (auto&& sp) {
            mlogger.debug("Schema version request for {}", v);
            return local_schema_registry().get_frozen(v);
        });
    });
}

future<> migration_manager::uninit_messaging_service()
{
    return when_all_succeed(
        _messaging.unregister_migration_request(),
        _messaging.unregister_definitions_update(),
        _messaging.unregister_schema_check(),
        _messaging.unregister_get_schema_version()
    ).discard_result();
}

void migration_notifier::register_listener(migration_listener* listener)
{
    _listeners.add(listener);
}

future<> migration_notifier::unregister_listener(migration_listener* listener)
{
    return _listeners.remove(listener);
}

void migration_manager::schedule_schema_pull(const gms::inet_address& endpoint, const gms::endpoint_state& state)
{
    if (!_enable_schema_pulls) {
        mlogger.debug("Not pulling schema because schema pulls were disabled due to Raft.");
        return;
    }

    const auto* value = state.get_application_state_ptr(gms::application_state::SCHEMA);

    if (endpoint != utils::fb_utilities::get_broadcast_address() && value) {
        // FIXME: discarded future
        (void)maybe_schedule_schema_pull(table_schema_version(utils::UUID{value->value()}), endpoint).handle_exception([endpoint] (auto ep) {
            mlogger.warn("Fail to pull schema from {}: {}", endpoint, ep);
        });
    }
}

bool migration_manager::have_schema_agreement() {
    if (_gossiper.num_endpoints() == 1) {
        // Us.
        return true;
    }
    auto our_version = _storage_proxy.get_db().local().get_version();
    bool match = false;
    _gossiper.for_each_endpoint_state_until([&] (const gms::inet_address& endpoint, const gms::endpoint_state& eps) {
        if (endpoint == utils::fb_utilities::get_broadcast_address() || !_gossiper.is_alive(endpoint)) {
            return stop_iteration::no;
        }
        mlogger.debug("Checking schema state for {}.", endpoint);
        auto schema = eps.get_application_state_ptr(gms::application_state::SCHEMA);
        if (!schema) {
            mlogger.debug("Schema state not yet available for {}.", endpoint);
            match = false;
            return stop_iteration::yes;
        }
        auto remote_version = table_schema_version(utils::UUID{schema->value()});
        if (our_version != remote_version) {
            mlogger.debug("Schema mismatch for {} ({} != {}).", endpoint, our_version, remote_version);
            match = false;
            return stop_iteration::yes;
        } else {
            match = true;
        }
        return stop_iteration::no;
    });
    return match;
}

future<> migration_manager::wait_for_schema_agreement(const replica::database& db, db::timeout_clock::time_point deadline, seastar::abort_source* as) {
    while (db.get_version() == replica::database::empty_version || !have_schema_agreement()) {
        if (as) {
            as->check();
        }
        if (db::timeout_clock::now() > deadline) {
            throw std::runtime_error("Unable to reach schema agreement");
        }
        co_await (as ? sleep_abortable(std::chrono::milliseconds(500), *as) : sleep(std::chrono::milliseconds(500)));
    }
}

/**
 * If versions differ this node sends request with local migration list to the endpoint
 * and expecting to receive a list of migrations to apply locally.
 */
future<> migration_manager::maybe_schedule_schema_pull(const table_schema_version& their_version, const gms::inet_address& endpoint)
{
    auto& proxy = _storage_proxy;
    auto& db = proxy.get_db().local();

    if (db.get_version() == their_version || !should_pull_schema_from(endpoint)) {
        mlogger.debug("Not pulling schema because versions match or shouldPullSchemaFrom returned false");
        return make_ready_future<>();
    }

    if (db.get_version() == replica::database::empty_version || runtime::get_uptime() < migration_delay) {
        // If we think we may be bootstrapping or have recently started, submit MigrationTask immediately
        mlogger.debug("Submitting migration task for {}", endpoint);
        return submit_migration_task(endpoint);
    }

    return with_gate(_background_tasks, [this, &db, endpoint] {
        // Include a delay to make sure we have a chance to apply any changes being
        // pushed out simultaneously. See CASSANDRA-5025
        return sleep_abortable(migration_delay, _as).then([this, &db, endpoint] {
            // grab the latest version of the schema since it may have changed again since the initial scheduling
            auto ep_state = _gossiper.get_endpoint_state_ptr(endpoint);
            if (!ep_state) {
                mlogger.debug("epState vanished for {}, not submitting migration task", endpoint);
                return make_ready_future<>();
            }
            const auto* value = ep_state->get_application_state_ptr(gms::application_state::SCHEMA);
            if (!value) {
                mlogger.debug("application_state::SCHEMA does not exist for {}, not submitting migration task", endpoint);
                return make_ready_future<>();
            }
            auto current_version = table_schema_version(utils::UUID{value->value()});
            if (db.get_version() == current_version) {
                mlogger.debug("not submitting migration task for {} because our versions match", endpoint);
                return make_ready_future<>();
            }
            mlogger.debug("submitting migration task for {}", endpoint);
            return submit_migration_task(endpoint);
        });
    }).finally([me = shared_from_this()] {});
}

future<> migration_manager::disable_schema_pulls() {
    return container().invoke_on_all([] (migration_manager& mm) {
        mm._enable_schema_pulls = false;
    });
}

future<> migration_manager::submit_migration_task(const gms::inet_address& endpoint, bool can_ignore_down_node)
{
    if (!_gossiper.is_alive(endpoint)) {
        auto msg = format("Can't send migration request: node {} is down.", endpoint);
        mlogger.warn("{}", msg);
        return can_ignore_down_node ? make_ready_future<>() : make_exception_future<>(std::runtime_error(msg));
    }
    netw::messaging_service::msg_addr id{endpoint, 0};
    return merge_schema_from(id).handle_exception([](std::exception_ptr e) {
        try {
            std::rethrow_exception(e);
        } catch (const exceptions::configuration_exception& e) {
            mlogger.error("Configuration exception merging remote schema: {}", e.what());
            return make_exception_future<>(e);
        }
    });
}

future<> migration_manager::do_merge_schema_from(netw::messaging_service::msg_addr id)
{
    mlogger.info("Pulling schema from {}", id);
    auto frozen_and_canonical_mutations = co_await _messaging.send_migration_request(id, netw::schema_pull_options{});
    auto&& [mutations, canonical_mutations] = frozen_and_canonical_mutations;
    if (canonical_mutations) {
        co_await merge_schema_from(id, *canonical_mutations);
    } else {
        co_await merge_schema_from(id, mutations);
    }
    mlogger.info("Schema merge with {} completed", id);
}

future<> migration_manager::merge_schema_from(netw::messaging_service::msg_addr id)
{
    if (_as.abort_requested()) {
        return make_exception_future<>(abort_requested_exception());
    }

    mlogger.info("Requesting schema pull from {}", id);
    // FIXME: Drop entries for removed nodes (or earlier).
    auto res = _schema_pulls.try_emplace(id, [id, this] {
        return do_merge_schema_from(id);
    });
    return res.first->second.trigger();
}

future<> migration_manager::merge_schema_from(netw::messaging_service::msg_addr src, const std::vector<canonical_mutation>& canonical_mutations) {
    canonical_mutation_merge_count++;
    mlogger.debug("Applying schema mutations from {}", src);
    auto& proxy = _storage_proxy;
    const auto& db = proxy.get_db().local();

    if (_as.abort_requested()) {
        return make_exception_future<>(abort_requested_exception());
    }

    std::vector<mutation> mutations;
    mutations.reserve(canonical_mutations.size());
    try {
        for (const auto& cm : canonical_mutations) {
            auto& tbl = db.find_column_family(cm.column_family_id());
            mutations.emplace_back(cm.to_mutation(
                    tbl.schema()));
        }
    } catch (replica::no_such_column_family& e) {
        mlogger.error("Error while applying schema mutations from {}: {}", src, e);
        return make_exception_future<>(std::make_exception_ptr<std::runtime_error>(
                    std::runtime_error(fmt::format("Error while applying schema mutations: {}", e))));
    }
    return db::schema_tables::merge_schema(_sys_ks, proxy.container(), _feat, std::move(mutations));
}

future<> migration_manager::reload_schema() {
    mlogger.info("Reloading schema");
    std::vector<mutation> mutations;
    return db::schema_tables::merge_schema(_sys_ks, _storage_proxy.container(), _feat, std::move(mutations), true);
}

future<> migration_manager::merge_schema_from(netw::messaging_service::msg_addr src, const std::vector<frozen_mutation>& mutations)
{
    if (_as.abort_requested()) {
        return make_exception_future<>(abort_requested_exception());
    }

    mlogger.debug("Applying schema mutations from {}", src);
    return map_reduce(mutations, [this, src](const frozen_mutation& fm) {
        // schema table's schema is not syncable so just use get_schema_definition()
        return get_schema_definition(fm.schema_version(), src, _messaging, _storage_proxy).then([&fm](schema_ptr s) {
            s->registry_entry()->mark_synced();
            return fm.unfreeze(std::move(s));
        });
    }, std::vector<mutation>(), [](std::vector<mutation>&& all, mutation&& m) {
        all.emplace_back(std::move(m));
        return std::move(all);
    }).then([this](std::vector<mutation> schema) {
        return db::schema_tables::merge_schema(_sys_ks, _storage_proxy.container(), _feat, std::move(schema));
    });
}

bool migration_manager::has_compatible_schema_tables_version(const gms::inet_address& endpoint) {
    auto* version = _gossiper.get_application_state_ptr(endpoint, gms::application_state::SCHEMA_TABLES_VERSION);
    return version && version->value() == db::schema_tables::version;
}

bool migration_manager::should_pull_schema_from(const gms::inet_address& endpoint) {
    return has_compatible_schema_tables_version(endpoint)
            && !_gossiper.is_gossip_only_member(endpoint);
}

future<> migration_notifier::on_schema_change(std::function<void(migration_listener*)> notify, std::function<std::string(std::exception_ptr)> describe_error) {
    return seastar::async([this, notify = std::move(notify), describe_error = std::move(describe_error)] {
        std::exception_ptr ex;
        _listeners.thread_for_each([&] (migration_listener* listener) {
            try {
                notify(listener);
            } catch (...) {
                ex = std::current_exception();
                mlogger.error("{}", describe_error(ex));
            }
        });
        if (ex) {
            std::rethrow_exception(std::move(ex));
        }
    });
}

future<> migration_notifier::create_keyspace(lw_shared_ptr<keyspace_metadata> ksm) {
    const auto& name = ksm->name();
    co_await on_schema_change([&] (migration_listener* listener) {
        listener->on_create_keyspace(name);
    }, [&] (std::exception_ptr ex) {
        return fmt::format("Create keyspace notification failed {}: {}", name, ex);
    });
}

future<> migration_notifier::create_column_family(schema_ptr cfm) {
    const auto& ks_name = cfm->ks_name();
    const auto& cf_name = cfm->cf_name();
    co_await on_schema_change([&] (migration_listener* listener) {
        listener->on_create_column_family(ks_name, cf_name);
    }, [&] (std::exception_ptr ex) {
        return fmt::format("Create column family notification failed {}.{}: {}", ks_name, cf_name, ex);
    });
}

future<> migration_notifier::create_user_type(user_type type) {
    const auto& ks_name = type->_keyspace;
    const auto& type_name = type->get_name_as_string();
    co_await on_schema_change([&] (migration_listener* listener) {
        listener->on_create_user_type(ks_name, type_name);
    }, [&] (std::exception_ptr ex) {
        return fmt::format("Create user type notification failed {}.{}: {}", ks_name, type_name, ex);
    });
}

future<> migration_notifier::create_view(view_ptr view) {
    const auto& ks_name = view->ks_name();
    const auto& view_name = view->cf_name();
    co_await on_schema_change([&] (migration_listener* listener) {
        listener->on_create_view(ks_name, view_name);
    }, [&] (std::exception_ptr ex) {
        return fmt::format("Create view notification failed {}.{}: {}", ks_name, view_name, ex);
    });
}

#if 0
public void notifyCreateFunction(UDFunction udf)
{
    for (IMigrationListener listener : listeners)
        listener.onCreateFunction(udf.name().keyspace, udf.name().name);
}

public void notifyCreateAggregate(UDAggregate udf)
{
    for (IMigrationListener listener : listeners)
        listener.onCreateAggregate(udf.name().keyspace, udf.name().name);
}
#endif

future<> migration_notifier::update_keyspace(lw_shared_ptr<keyspace_metadata> ksm) {
    const auto& name = ksm->name();
    co_await on_schema_change([&] (migration_listener* listener) {
        listener->on_update_keyspace(name);
    }, [&] (std::exception_ptr ex) {
        return fmt::format("Update keyspace notification failed {}: {}", name, ex);
    });
}

future<> migration_notifier::update_column_family(schema_ptr cfm, bool columns_changed) {
    const auto& ks_name = cfm->ks_name();
    const auto& cf_name = cfm->cf_name();
    co_await on_schema_change([&] (migration_listener* listener) {
        listener->on_update_column_family(ks_name, cf_name, columns_changed);
    }, [&] (std::exception_ptr ex) {
        return fmt::format("Update column family notification failed {}.{}: {}", ks_name, cf_name, ex);
    });
}

future<> migration_notifier::update_user_type(user_type type) {
    const auto& ks_name = type->_keyspace;
    const auto& type_name = type->get_name_as_string();
    co_await on_schema_change([&] (migration_listener* listener) {
        listener->on_update_user_type(ks_name, type_name);
    }, [&] (std::exception_ptr ex) {
        return fmt::format("Update user type notification failed {}.{}: {}", ks_name, type_name, ex);
    });
}

future<> migration_notifier::update_view(view_ptr view, bool columns_changed) {
    const auto& ks_name = view->ks_name();
    const auto& view_name = view->cf_name();
    co_await on_schema_change([&] (migration_listener* listener) {
        listener->on_update_view(ks_name, view_name, columns_changed);
    }, [&] (std::exception_ptr ex) {
        return fmt::format("Update view notification failed {}.{}: {}", ks_name, view_name, ex);
    });
}

future<> migration_notifier::update_tablet_metadata() {
    return seastar::async([this] {
        _listeners.thread_for_each([] (migration_listener* listener) {
            listener->on_update_tablet_metadata();
        });
    });
}

#if 0
public void notifyUpdateFunction(UDFunction udf)
{
    for (IMigrationListener listener : listeners)
        listener.onUpdateFunction(udf.name().keyspace, udf.name().name);
}

public void notifyUpdateAggregate(UDAggregate udf)
{
    for (IMigrationListener listener : listeners)
        listener.onUpdateAggregate(udf.name().keyspace, udf.name().name);
}
#endif

future<> migration_notifier::drop_keyspace(sstring ks_name) {
    co_await on_schema_change([&] (migration_listener* listener) {
        listener->on_drop_keyspace(ks_name);
    }, [&] (std::exception_ptr ex) {
        return fmt::format("Drop keyspace notification failed {}: {}", ks_name, ex);
    });
}

future<> migration_notifier::drop_column_family(schema_ptr cfm) {
    const auto& ks_name = cfm->ks_name();
    const auto& cf_name = cfm->cf_name();
    co_await on_schema_change([&] (migration_listener* listener) {
        listener->on_drop_column_family(ks_name, cf_name);
    }, [&] (std::exception_ptr ex) {
        return fmt::format("Drop column family notification failed {}.{}: {}", ks_name, cf_name, ex);
    });
}

future<> migration_notifier::drop_user_type(user_type type) {
    const auto& ks_name = type->_keyspace;
    const auto& type_name = type->get_name_as_string();
    co_await on_schema_change([&] (migration_listener* listener) {
        listener->on_drop_user_type(ks_name, type_name);
    }, [&] (std::exception_ptr ex) {
        return fmt::format("Drop user type notification failed {}.{}: {}", ks_name, type_name, ex);
    });
}

future<> migration_notifier::drop_view(view_ptr view) {
    const auto& ks_name = view->ks_name();
    const auto& view_name = view->cf_name();
    co_await on_schema_change([&] (migration_listener* listener) {
        listener->on_drop_view(ks_name, view_name);
    }, [&] (std::exception_ptr ex) {
        return fmt::format("Drop view notification failed {}.{}: {}", ks_name, view_name, ex);
    });
}

future<> migration_notifier::drop_function(const db::functions::function_name& fun_name, const std::vector<data_type>& arg_types) {
    auto&& ks_name = fun_name.keyspace;
    auto&& sig = auth::encode_signature(fun_name.name, arg_types);
    co_await on_schema_change([&] (migration_listener* listener) {
        listener->on_drop_function(ks_name, sig);
    }, [&] (std::exception_ptr ex) {
        return fmt::format("Drop function notification failed {}.{}: {}", ks_name, sig, ex);
    });
}

future<> migration_notifier::drop_aggregate(const db::functions::function_name& fun_name, const std::vector<data_type>& arg_types) {
    auto&& ks_name = fun_name.keyspace;
    auto&& sig = auth::encode_signature(fun_name.name, arg_types);
    co_await on_schema_change([&] (migration_listener* listener) {
        listener->on_drop_aggregate(ks_name, sig);
    }, [&] (std::exception_ptr ex) {
        return fmt::format("Drop aggregate notification failed {}.{}: {}", ks_name, sig, ex);
    });
}

void migration_notifier::before_create_column_family(const schema& schema,
        std::vector<mutation>& mutations, api::timestamp_type timestamp) {
    _listeners.thread_for_each([&mutations, &schema, timestamp] (migration_listener* listener) {
        // allow exceptions. so a listener can effectively kill a create-table
        listener->on_before_create_column_family(schema, mutations, timestamp);
    });
}

void migration_notifier::before_update_column_family(const schema& new_schema,
        const schema& old_schema, std::vector<mutation>& mutations, api::timestamp_type ts) {
    _listeners.thread_for_each([&mutations, &new_schema, &old_schema, ts] (migration_listener* listener) {
        // allow exceptions. so a listener can effectively kill an update-column
        listener->on_before_update_column_family(new_schema, old_schema, mutations, ts);
    });
}

void migration_notifier::before_drop_column_family(const schema& schema,
        std::vector<mutation>& mutations, api::timestamp_type ts) {
    _listeners.thread_for_each([&mutations, &schema, ts] (migration_listener* listener) {
        // allow exceptions. so a listener can effectively kill a drop-column
        listener->on_before_drop_column_family(schema, mutations, ts);
    });
}

void migration_notifier::before_drop_keyspace(const sstring& keyspace_name,
        std::vector<mutation>& mutations, api::timestamp_type ts) {
    _listeners.thread_for_each([&mutations, &keyspace_name, ts] (migration_listener* listener) {
        listener->on_before_drop_keyspace(keyspace_name, mutations, ts);
    });
}

#if 0
public void notifyDropFunction(UDFunction udf)
{
    for (IMigrationListener listener : listeners)
        listener.onDropFunction(udf.name().keyspace, udf.name().name);
}

public void notifyDropAggregate(UDAggregate udf)
{
    for (IMigrationListener listener : listeners)
        listener.onDropAggregate(udf.name().keyspace, udf.name().name);
}
#endif

std::vector<mutation> prepare_keyspace_update_announcement(replica::database& db, lw_shared_ptr<keyspace_metadata> ksm, api::timestamp_type ts) {
    db.validate_keyspace_update(*ksm);
    mlogger.info("Update Keyspace: {}", ksm);
    return db::schema_tables::make_create_keyspace_mutations(db.features().cluster_schema_features(), ksm, ts);
}

std::vector<mutation> prepare_new_keyspace_announcement(replica::database& db, lw_shared_ptr<keyspace_metadata> ksm, api::timestamp_type timestamp) {
    db.validate_new_keyspace(*ksm);
    mlogger.info("Create new Keyspace: {}", ksm);
    return db::schema_tables::make_create_keyspace_mutations(db.features().cluster_schema_features(), ksm, timestamp);
}

static future<std::vector<mutation>> include_keyspace(
        storage_proxy& sp, const keyspace_metadata& keyspace, std::vector<mutation> mutations) {
    // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
    mutation m = co_await db::schema_tables::read_keyspace_mutation(sp.container(), keyspace.name());
    mutations.push_back(std::move(m));
    co_return std::move(mutations);
}

future<std::vector<mutation>> prepare_new_column_family_announcement(storage_proxy& sp, schema_ptr cfm, api::timestamp_type timestamp) {
#if 0
    cfm.validate();
#endif
    try {
        auto& db = sp.get_db().local();
        auto&& keyspace = db.find_keyspace(cfm->ks_name());
        if (db.has_schema(cfm->ks_name(), cfm->cf_name())) {
            throw exceptions::already_exists_exception(cfm->ks_name(), cfm->cf_name());
        }
        if (db.column_family_exists(cfm->id())) {
            throw exceptions::invalid_request_exception(format("Table with ID {} already exists: {}", cfm->id(), db.find_schema(cfm->id())));
        }

        mlogger.info("Create new ColumnFamily: {}", cfm);

        auto ksm = keyspace.metadata();
        return seastar::async([&db, cfm, timestamp, ksm] {
            auto mutations = db::schema_tables::make_create_table_mutations(cfm, timestamp);
            db.get_notifier().before_create_column_family(*cfm, mutations, timestamp);
            return mutations;
        }).then([&sp, ksm](std::vector<mutation> mutations) {
            return include_keyspace(sp, *ksm, std::move(mutations));
        });
    } catch (const replica::no_such_keyspace& e) {
        throw exceptions::configuration_exception(format("Cannot add table '{}' to non existing keyspace '{}'.", cfm->cf_name(), cfm->ks_name()));
    }
}

future<std::vector<mutation>> prepare_column_family_update_announcement(storage_proxy& sp,
        schema_ptr cfm, bool from_thrift, std::vector<view_ptr> view_updates, api::timestamp_type ts) {
    warn(unimplemented::cause::VALIDATION);
#if 0
    cfm.validate();
#endif
    try {
        auto& db = sp.local_db();
        auto&& old_schema = db.find_column_family(cfm->ks_name(), cfm->cf_name()).schema(); // FIXME: Should we lookup by id?
#if 0
        oldCfm.validateCompatility(cfm);
#endif
        mlogger.info("Update table '{}.{}' From {} To {}", cfm->ks_name(), cfm->cf_name(), *old_schema, *cfm);
        auto&& keyspace = db.find_keyspace(cfm->ks_name()).metadata();

        auto mutations = db::schema_tables::make_update_table_mutations(db, keyspace, old_schema, cfm, ts, from_thrift);
        for (auto&& view : view_updates) {
            auto& old_view = keyspace->cf_meta_data().at(view->cf_name());
            mlogger.info("Update view '{}.{}' From {} To {}", view->ks_name(), view->cf_name(), *old_view, *view);
            auto view_mutations = db::schema_tables::make_update_view_mutations(keyspace, view_ptr(old_view), std::move(view), ts, false);
            std::move(view_mutations.begin(), view_mutations.end(), std::back_inserter(mutations));
            co_await coroutine::maybe_yield();
        }
        co_await seastar::async([&] {
            db.get_notifier().before_update_column_family(*cfm, *old_schema, mutations, ts);
        });
        co_return co_await include_keyspace(sp, *keyspace, std::move(mutations));
    } catch (const replica::no_such_column_family& e) {
        auto&& ex = std::make_exception_ptr(exceptions::configuration_exception(format("Cannot update non existing table '{}' in keyspace '{}'.",
                                                         cfm->cf_name(), cfm->ks_name())));
        co_return coroutine::exception(std::move(ex));
    }
}

static future<std::vector<mutation>> do_prepare_new_type_announcement(storage_proxy& sp, user_type new_type, api::timestamp_type ts) {
    auto& db = sp.local_db();
    auto&& keyspace = db.find_keyspace(new_type->_keyspace);
    auto mutations = db::schema_tables::make_create_type_mutations(keyspace.metadata(), new_type, ts);
    return include_keyspace(sp, *keyspace.metadata(), std::move(mutations));
}

future<std::vector<mutation>> prepare_new_type_announcement(storage_proxy& sp, user_type new_type, api::timestamp_type ts) {
    mlogger.info("Prepare Create new User Type: {}", new_type->get_name_as_string());
    return do_prepare_new_type_announcement(sp, std::move(new_type), ts);
}

future<std::vector<mutation>> prepare_update_type_announcement(storage_proxy& sp, user_type updated_type, api::timestamp_type ts) {
    mlogger.info("Prepare Update User Type: {}", updated_type->get_name_as_string());
    return do_prepare_new_type_announcement(sp, updated_type, ts);
}

future<std::vector<mutation>> prepare_new_function_announcement(storage_proxy& sp, shared_ptr<cql3::functions::user_function> func, api::timestamp_type ts) {
    auto& db = sp.local_db();
    auto&& keyspace = db.find_keyspace(func->name().keyspace);
    auto mutations = db::schema_tables::make_create_function_mutations(func, ts);
    return include_keyspace(sp, *keyspace.metadata(), std::move(mutations));
}

future<std::vector<mutation>> prepare_function_drop_announcement(storage_proxy& sp, shared_ptr<cql3::functions::user_function> func, api::timestamp_type ts) {
    auto& db = sp.local_db();
    auto&& keyspace = db.find_keyspace(func->name().keyspace);
    auto mutations = db::schema_tables::make_drop_function_mutations(func, ts);
    return include_keyspace(sp, *keyspace.metadata(), std::move(mutations));
}

future<std::vector<mutation>> prepare_new_aggregate_announcement(storage_proxy& sp, shared_ptr<cql3::functions::user_aggregate> aggregate, api::timestamp_type ts) {
    auto& db = sp.local_db();
    auto&& keyspace = db.find_keyspace(aggregate->name().keyspace);
    auto mutations = db::schema_tables::make_create_aggregate_mutations(db.features().cluster_schema_features(), aggregate, ts);
    return include_keyspace(sp, *keyspace.metadata(), std::move(mutations));
}

future<std::vector<mutation>> prepare_aggregate_drop_announcement(storage_proxy& sp, shared_ptr<cql3::functions::user_aggregate> aggregate, api::timestamp_type ts) {
    auto& db = sp.local_db();
    auto&& keyspace = db.find_keyspace(aggregate->name().keyspace);
    auto mutations = db::schema_tables::make_drop_aggregate_mutations(db.features().cluster_schema_features(), aggregate, ts);
    return include_keyspace(sp, *keyspace.metadata(), std::move(mutations));
}

future<std::vector<mutation>> prepare_keyspace_drop_announcement(replica::database& db, const sstring& ks_name, api::timestamp_type ts) {
    if (!db.has_keyspace(ks_name)) {
        throw exceptions::configuration_exception(format("Cannot drop non existing keyspace '{}'.", ks_name));
    }
    auto& keyspace = db.find_keyspace(ks_name);
    mlogger.info("Drop Keyspace '{}'", ks_name);
    return seastar::async([&db, &keyspace, ts, ks_name] {
        auto mutations = db::schema_tables::make_drop_keyspace_mutations(db.features().cluster_schema_features(), keyspace.metadata(), ts);
        db.get_notifier().before_drop_keyspace(ks_name, mutations, ts);
        return mutations;
    });
}

future<std::vector<mutation>> prepare_column_family_drop_announcement(storage_proxy& sp,
        const sstring& ks_name, const sstring& cf_name, api::timestamp_type ts, drop_views drop_views) {
    try {
        auto& db = sp.local_db();
        auto& old_cfm = db.find_column_family(ks_name, cf_name);
        auto& schema = old_cfm.schema();
        if (schema->is_view()) {
            co_await coroutine::return_exception(exceptions::invalid_request_exception("Cannot use DROP TABLE on Materialized View"));
        }
        auto keyspace = db.find_keyspace(ks_name).metadata();

        // If drop_views is false (the default), we don't allow to delete a
        // table which has views which aren't part of an index. If drop_views
        // is true, we delete those views as well.
        auto&& views = old_cfm.views();
        if (!drop_views && views.size() > schema->all_indices().size()) {
            auto explicit_view_names = views
                                    | boost::adaptors::filtered([&old_cfm](const view_ptr& v) { return !old_cfm.get_index_manager().is_index(v); })
                                    | boost::adaptors::transformed([](const view_ptr& v) { return v->cf_name(); });
            co_await coroutine::return_exception(exceptions::invalid_request_exception(format("Cannot drop table when materialized views still depend on it ({}.{{{}}})",
                        schema->ks_name(), fmt::join(explicit_view_names, ", "))));
        }
        mlogger.info("Drop table '{}.{}'", schema->ks_name(), schema->cf_name());

        std::vector<mutation> drop_si_mutations;
        if (!schema->all_indices().empty()) {
            auto builder = schema_builder(schema).without_indexes();
            drop_si_mutations = db::schema_tables::make_update_table_mutations(db, keyspace, schema, builder.build(), ts, false);
        }
        auto mutations = db::schema_tables::make_drop_table_mutations(keyspace, schema, ts);
        mutations.insert(mutations.end(), std::make_move_iterator(drop_si_mutations.begin()), std::make_move_iterator(drop_si_mutations.end()));
        for (auto& v : views) {
            if (!old_cfm.get_index_manager().is_index(v)) {
                mlogger.info("Drop view '{}.{}' of table '{}'", v->ks_name(), v->cf_name(), schema->cf_name());
                auto m = db::schema_tables::make_drop_view_mutations(keyspace, v, ts);
                mutations.insert(mutations.end(), std::make_move_iterator(m.begin()), std::make_move_iterator(m.end()));
            }
        }

        // notifiers must run in seastar thread
        co_await seastar::async([&] {
            db.get_notifier().before_drop_column_family(*schema, mutations, ts);
        });
        co_return co_await include_keyspace(sp, *keyspace, std::move(mutations));
    } catch (const replica::no_such_column_family& e) {
        auto&& ex = std::make_exception_ptr(exceptions::configuration_exception(format("Cannot drop non existing table '{}' in keyspace '{}'.", cf_name, ks_name)));
        co_return coroutine::exception(std::move(ex));
    }
}

future<std::vector<mutation>> prepare_type_drop_announcement(storage_proxy& sp, user_type dropped_type, api::timestamp_type ts) {
    auto& db = sp.local_db();
    auto&& keyspace = db.find_keyspace(dropped_type->_keyspace);
    mlogger.info("Drop User Type: {}", dropped_type->get_name_as_string());
    auto mutations =
            db::schema_tables::make_drop_type_mutations(keyspace.metadata(), dropped_type, ts);
    return include_keyspace(sp, *keyspace.metadata(), std::move(mutations));
}

future<std::vector<mutation>> prepare_new_view_announcement(storage_proxy& sp, view_ptr view, api::timestamp_type ts) {
#if 0
    view.metadata.validate();
#endif
    auto& db = sp.local_db();
    try {
        auto&& keyspace = db.find_keyspace(view->ks_name()).metadata();
        if (keyspace->cf_meta_data().contains(view->cf_name())) {
            throw exceptions::already_exists_exception(view->ks_name(), view->cf_name());
        }
        mlogger.info("Create new view: {}", view);
        auto mutations = db::schema_tables::make_create_view_mutations(keyspace, std::move(view), ts);
        co_return co_await include_keyspace(sp, *keyspace, std::move(mutations));
    } catch (const replica::no_such_keyspace& e) {
        auto&& ex = std::make_exception_ptr(exceptions::configuration_exception(format("Cannot add view '{}' to non existing keyspace '{}'.", view->cf_name(), view->ks_name())));
        co_return coroutine::exception(std::move(ex));
    }
}

future<std::vector<mutation>> prepare_view_update_announcement(storage_proxy& sp, view_ptr view, api::timestamp_type ts) {
#if 0
    view.metadata.validate();
#endif
    auto db = sp.data_dictionary();
    try {
        auto&& keyspace = db.find_keyspace(view->ks_name()).metadata();
        auto& old_view = keyspace->cf_meta_data().at(view->cf_name());
        if (!old_view->is_view()) {
            co_await coroutine::return_exception(exceptions::invalid_request_exception("Cannot use ALTER MATERIALIZED VIEW on Table"));
        }
#if 0
        oldCfm.validateCompatility(cfm);
#endif
        mlogger.info("Update view '{}.{}' From {} To {}", view->ks_name(), view->cf_name(), *old_view, *view);
        auto mutations = db::schema_tables::make_update_view_mutations(keyspace, view_ptr(old_view), std::move(view), ts, true);
        co_return co_await include_keyspace(sp, *keyspace, std::move(mutations));
    } catch (const std::out_of_range& e) {
        auto&& ex = std::make_exception_ptr(exceptions::configuration_exception(format("Cannot update non existing materialized view '{}' in keyspace '{}'.",
                                                         view->cf_name(), view->ks_name())));
        co_return coroutine::exception(std::move(ex));
    }
}

future<std::vector<mutation>> prepare_view_drop_announcement(storage_proxy& sp, const sstring& ks_name, const sstring& cf_name, api::timestamp_type ts) {
    auto& db = sp.local_db();
    try {
        auto& view = db.find_column_family(ks_name, cf_name).schema();
        if (!view->is_view()) {
            throw exceptions::invalid_request_exception("Cannot use DROP MATERIALIZED VIEW on Table");
        }
        if (db.find_column_family(view->view_info()->base_id()).get_index_manager().is_index(view_ptr(view))) {
            throw exceptions::invalid_request_exception("Cannot use DROP MATERIALIZED VIEW on Index");
        }
        auto keyspace = db.find_keyspace(ks_name).metadata();
        mlogger.info("Drop view '{}.{}'", view->ks_name(), view->cf_name());
        auto mutations = db::schema_tables::make_drop_view_mutations(keyspace, view_ptr(std::move(view)), ts);
        return include_keyspace(sp, *keyspace, std::move(mutations));
    } catch (const replica::no_such_column_family& e) {
        throw exceptions::configuration_exception(format("Cannot drop non existing materialized view '{}' in keyspace '{}'.",
                                                         cf_name, ks_name));
    }
}

future<> migration_manager::push_schema_mutation(const gms::inet_address& endpoint, const std::vector<mutation>& schema)
{
    netw::messaging_service::msg_addr id{endpoint, 0};
    auto schema_features = _feat.cluster_schema_features();
    auto adjusted_schema = db::schema_tables::adjust_schema_for_schema_features(schema, schema_features);
    auto fm = std::vector<frozen_mutation>(adjusted_schema.begin(), adjusted_schema.end());
    auto cm = std::vector<canonical_mutation>(adjusted_schema.begin(), adjusted_schema.end());
    return _messaging.send_definitions_update(id, std::move(fm), std::move(cm));
}

future<> migration_manager::announce_with_raft(std::vector<mutation> schema, group0_guard guard, std::string_view description) {
    assert(this_shard_id() == 0);
    auto schema_features = _feat.cluster_schema_features();
    auto adjusted_schema = db::schema_tables::adjust_schema_for_schema_features(schema, schema_features);

    auto group0_cmd = _group0_client.prepare_command(
        schema_change{
            .mutations{adjusted_schema.begin(), adjusted_schema.end()},
        },
        guard, std::move(description));

    co_return co_await _group0_client.add_entry(std::move(group0_cmd), std::move(guard), &_as);
}

future<> migration_manager::announce_without_raft(std::vector<mutation> schema, group0_guard guard) {
    auto f = db::schema_tables::merge_schema(_sys_ks, _storage_proxy.container(), _feat, schema);

    try {
        using namespace std::placeholders;
        auto all_live = _gossiper.get_live_members();
        auto live_members = all_live | boost::adaptors::filtered([this] (const gms::inet_address& endpoint) {
            // only push schema to nodes with known and equal versions
            return endpoint != utils::fb_utilities::get_broadcast_address() &&
                _messaging.knows_version(endpoint) &&
                _messaging.get_raw_version(endpoint) == netw::messaging_service::current_version;
        });
        co_await coroutine::parallel_for_each(live_members.begin(), live_members.end(),
            std::bind(std::mem_fn(&migration_manager::push_schema_mutation), this, std::placeholders::_1, schema));
    } catch (...) {
        mlogger.error("failed to announce migration to all nodes: {}", std::current_exception());
    }

    co_return co_await std::move(f);
}

// Returns a future on the local application of the schema
future<> migration_manager::announce(std::vector<mutation> schema, group0_guard guard, std::string_view description) {
    if (guard.with_raft()) {
        return announce_with_raft(std::move(schema), std::move(guard), std::move(description));
    } else {
        return announce_without_raft(std::move(schema), std::move(guard));
    }
}

future<group0_guard> migration_manager::start_group0_operation() {
    assert(this_shard_id() == 0);
    return _group0_client.start_operation(&_as);
}

/**
 * Announce my version passively over gossip.
 * Used to notify nodes as they arrive in the cluster.
 *
 * @param version The schema version to announce
 */
void migration_manager::passive_announce(table_schema_version version) {
    _schema_version_to_publish = version;
    (void)_schema_push.trigger().handle_exception([version = std::move(version)] (std::exception_ptr ex) {
        mlogger.warn("Passive announcing of version {} failed: {}. Ignored.", version, ex);
    });
}

future<> migration_manager::passive_announce() {
    assert(this_shard_id() == 0);
    mlogger.debug("Gossiping my schema version {}", _schema_version_to_publish);
    return _gossiper.add_local_application_state(gms::application_state::SCHEMA, gms::versioned_value::schema(_schema_version_to_publish));
}

#if 0
/**
 * Clear all locally stored schema information and reset schema to initial state.
 * Called by user (via JMX) who wants to get rid of schema disagreement.
 *
 * @throws IOException if schema tables truncation fails
 */
public static void resetLocalSchema() throws IOException
{
    mlogger.info("Starting local schema reset...");

    mlogger.debug("Truncating schema tables...");

    LegacySchemaTables.truncateSchemaTables();

    mlogger.debug("Clearing local schema keyspace definitions...");

    Schema.instance.clear();

    Set<InetAddress> liveEndpoints = Gossiper.instance.getLiveMembers();
    liveEndpoints.remove(FBUtilities.getBroadcastAddress());

    // force migration if there are nodes around
    for (InetAddress node : liveEndpoints)
    {
        if (shouldPullSchemaFrom(node))
        {
            mlogger.debug("Requesting schema from {}", node);
            FBUtilities.waitOnFuture(submitMigrationTask(node));
            break;
        }
    }

    mlogger.info("Local schema reset is complete.");
}

public static class MigrationsSerializer implements IVersionedSerializer<Collection<Mutation>>
{
    public static MigrationsSerializer instance = new MigrationsSerializer();

    public void serialize(Collection<Mutation> schema, DataOutputPlus out, int version) throws IOException
    {
        out.writeInt(schema.size());
        for (Mutation mutation : schema)
            Mutation.serializer.serialize(mutation, out, version);
    }

    public Collection<Mutation> deserialize(DataInput in, int version) throws IOException
    {
        int count = in.readInt();
        Collection<Mutation> schema = new ArrayList<>(count);

        for (int i = 0; i < count; i++)
            schema.add(Mutation.serializer.deserialize(in, version));

        return schema;
    }

    public long serializedSize(Collection<Mutation> schema, int version)
    {
        int size = TypeSizes.NATIVE.sizeof(schema.size());
        for (Mutation mutation : schema)
            size += Mutation.serializer.serializedSize(mutation, version);
        return size;
    }
}
#endif


// Ensure that given schema version 's' was synced with on current node. See schema::is_synced().
//
// The endpoint is the node from which 's' originated.
//
future<> migration_manager::maybe_sync(const schema_ptr& s, netw::messaging_service::msg_addr endpoint) {
    if (s->is_synced()) {
        return make_ready_future<>();
    }

    return s->registry_entry()->maybe_sync([this, s, endpoint] {
        // Serialize schema sync by always doing it on shard 0.
        if (this_shard_id() == 0) {
            mlogger.debug("Syncing schema of {}.{} (v={}) with {}", s->ks_name(), s->cf_name(), s->version(), endpoint);
            return merge_schema_from(endpoint);
        } else {
            return container().invoke_on(0, [gs = global_schema_ptr(s), endpoint] (migration_manager& local_mm) {
                schema_ptr s = gs.get();
                mlogger.debug("Syncing schema of {}.{} (v={}) with {}", s->ks_name(), s->cf_name(), s->version(), endpoint);
                return local_mm.merge_schema_from(endpoint);
            });
        }
    });
}

// Returns schema of given version, either from cache or from remote node identified by 'from'.
// Doesn't affect current node's schema in any way.
static future<schema_ptr> get_schema_definition(table_schema_version v, netw::messaging_service::msg_addr dst, netw::messaging_service& ms, service::storage_proxy& storage_proxy) {
    return local_schema_registry().get_or_load(v, [&ms, &storage_proxy, dst] (table_schema_version v) {
        mlogger.debug("Requesting schema {} from {}", v, dst);
        return ms.send_get_schema_version(dst, v).then([&storage_proxy] (frozen_schema s) {
            auto& proxy = storage_proxy.container();
            // Since the latest schema version is always present in the schema registry
            // we only happen to query already outdated schema version, which is
            // referenced by the incoming request.
            // That means the column mapping for the schema should always be inserted
            // with TTL (refresh TTL in case column mapping already existed prior to that).
            auto us = s.unfreeze(db::schema_ctxt(proxy));
            // if this is a view - sanity check that its schema doesn't need fixing.
            if (us->is_view()) {
                auto& db = proxy.local().local_db();
                schema_ptr base_schema = db.find_schema(us->view_info()->base_id());
                db::schema_tables::check_no_legacy_secondary_index_mv_schema(db, view_ptr(us), base_schema);
            }
            return db::schema_tables::store_column_mapping(proxy, us, true).then([us] {
                return frozen_schema{us};
            });
        });
    }).then([&storage_proxy] (schema_ptr s) {
        // If this is a view so this schema also needs a reference to the base
        // table.
        if (s->is_view()) {
            if (!s->view_info()->base_info()) {
                auto& db = storage_proxy.local_db();
                // This line might throw a no_such_column_family
                // It should be fine since if we tried to register a view for which
                // we don't know the base table, our registry is broken.
                schema_ptr base_schema = db.find_schema(s->view_info()->base_id());
                s->view_info()->set_base_info(s->view_info()->make_base_dependent_view_info(*base_schema));
            }
        }
        return s;
    });
}

future<schema_ptr> migration_manager::get_schema_for_read(table_schema_version v, netw::messaging_service::msg_addr dst, netw::messaging_service& ms, abort_source* as) {
    return get_schema_for_write(v, dst, ms, as);
}

future<schema_ptr> migration_manager::get_schema_for_write(table_schema_version v, netw::messaging_service::msg_addr dst, netw::messaging_service& ms, abort_source* as) {
    if (_as.abort_requested()) {
        co_return coroutine::exception(std::make_exception_ptr(abort_requested_exception()));
    }

    auto s = local_schema_registry().get_or_null(v);

    if (s && s->is_synced()) {
        co_return s;
    }

    // `_enable_schema_pulls` may change concurrently with this function (but only from `true` to `false`).
    bool use_raft = !_enable_schema_pulls;

    if (use_raft) {
        // Schema is synchronized through Raft, so perform a group 0 read barrier.
        // Batch the barriers so we don't invoke them redundantly.
        mlogger.trace("Performing raft read barrier because schema is not synced, version: {}", v);
        co_await (as ? _group0_barrier.trigger(*as) : _group0_barrier.trigger());
    }

    s = co_await get_schema_definition(v, dst, ms, _storage_proxy);

    if (use_raft) {
        // If Raft is used the schema is synced already (through barrier above), mark it as such.
        mlogger.trace("Mark schema {} as synced", v);
        co_await s->registry_entry()->maybe_sync([] { return make_ready_future<>(); });
    } else {
        co_await maybe_sync(s, dst);
    }

    co_return s;
}

future<> migration_manager::sync_schema(const replica::database& db, const std::vector<gms::inet_address>& nodes) {
    using schema_and_hosts = std::unordered_map<table_schema_version, std::vector<gms::inet_address>>;
    schema_and_hosts schema_map;
    co_await coroutine::parallel_for_each(nodes, [this, &schema_map, &db] (const gms::inet_address& node) -> future<> {
        const auto& my_version = db.get_version();
        auto remote_version = co_await _messaging.send_schema_check(netw::msg_addr(node));
        if (my_version != remote_version) {
            schema_map[remote_version].emplace_back(node);
        }
    });
    co_await coroutine::parallel_for_each(schema_map, [this] (auto& x) -> future<> {
        auto& [schema, hosts] = x;
        const auto& src = hosts.front();
        mlogger.debug("Pulling schema {} from {}", schema, src);
        bool can_ignore_down_node = false;
        return submit_migration_task(src, can_ignore_down_node);
    });
}

future<column_mapping> get_column_mapping(db::system_keyspace& sys_ks, table_id table_id, table_schema_version v) {
    schema_ptr s = local_schema_registry().get_or_null(v);
    if (s) {
        return make_ready_future<column_mapping>(s->get_column_mapping());
    }
    return db::schema_tables::get_column_mapping(sys_ks, table_id, v);
}

future<> migration_manager::on_join(gms::inet_address endpoint, gms::endpoint_state_ptr ep_state, gms::permit_id) {
    schedule_schema_pull(endpoint, *ep_state);
    return make_ready_future();
}

future<> migration_manager::on_change(gms::inet_address endpoint, gms::application_state state, const gms::versioned_value& value, gms::permit_id) {
    if (state == gms::application_state::SCHEMA) {
        auto ep_state = _gossiper.get_endpoint_state_ptr(endpoint);
        if (!ep_state || _gossiper.is_dead_state(*ep_state)) {
            mlogger.debug("Ignoring state change for dead or unknown endpoint: {}", endpoint);
            return make_ready_future();
        }
        if (_storage_proxy.get_token_metadata_ptr()->is_normal_token_owner(endpoint)) {
            schedule_schema_pull(endpoint, *ep_state);
        }
    }
    return make_ready_future();
}

future<> migration_manager::on_alive(gms::inet_address endpoint, gms::endpoint_state_ptr state, gms::permit_id) {
    schedule_schema_pull(endpoint, *state);
    return make_ready_future();
}

void migration_manager::set_concurrent_ddl_retries(size_t n) {
    _concurrent_ddl_retries = n;
}

}
