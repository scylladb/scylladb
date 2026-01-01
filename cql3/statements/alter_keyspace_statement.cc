/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include <fmt/format.h>
#include <seastar/core/coroutine.hh>
#include <seastar/core/on_internal_error.hh>
#include <stdexcept>
#include <vector>
#include "alter_keyspace_statement.hh"
#include "cql3/statements/property_definitions.hh"
#include "locator/tablets.hh"
#include "locator/abstract_replication_strategy.hh"
#include "mutation/canonical_mutation.hh"
#include "prepared_statement.hh"
#include <seastar/coroutine/exception.hh>
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "service/topology_mutation.hh"
#include "db/system_keyspace.hh"
#include "data_dictionary/data_dictionary.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/ks_prop_defs.hh"
#include "create_keyspace_statement.hh"
#include "gms/feature_service.hh"
#include "replica/database.hh"
#include "db/config.hh"

using namespace std::string_literals;

static logging::logger mylogger("alter_keyspace");

bool is_system_keyspace(std::string_view keyspace);

cql3::statements::alter_keyspace_statement::alter_keyspace_statement(sstring name, ::shared_ptr<ks_prop_defs> attrs)
    : _name(name)
    , _attrs(std::move(attrs))
{}

const sstring& cql3::statements::alter_keyspace_statement::keyspace() const {
    return _name;
}

future<> cql3::statements::alter_keyspace_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return state.has_keyspace_access(_name, auth::permission::ALTER);
}

static unsigned get_abs_rf_diff(const locator::replication_strategy_config_option& curr_rf, const locator::replication_strategy_config_option& new_rf) {
    return std::abs(ssize_t(locator::get_replication_factor(curr_rf)) - ssize_t(locator::get_replication_factor(new_rf)));
}

void cql3::statements::alter_keyspace_statement::validate(query_processor& qp, const service::client_state& state) const {
        auto tmp = _name;
        std::transform(tmp.begin(), tmp.end(), tmp.begin(), ::tolower);
        if (is_system_keyspace(tmp)) {
            throw exceptions::invalid_request_exception("Cannot alter system keyspace");
        }

        _attrs->validate();

        if (!bool(_attrs->get_replication_strategy_class()) && !_attrs->get_replication_options().empty()) {
            throw exceptions::configuration_exception("Missing replication strategy class");
        }
        try {
            auto ks = qp.db().find_keyspace(_name);
            data_dictionary::storage_options current_options = ks.metadata()->get_storage_options();
            data_dictionary::storage_options new_options = _attrs->get_storage_options();
            if (!qp.proxy().features().keyspace_storage_options && !new_options.is_local_type()) {
                throw exceptions::invalid_request_exception("Keyspace storage options not supported in the cluster");
            }
            if (!current_options.can_update_to(new_options)) {
                throw exceptions::invalid_request_exception(seastar::format("Cannot alter storage options: {} to {} is not supported",
                        current_options.type_string(), new_options.type_string()));
            }

            auto new_ks = _attrs->as_ks_metadata_update(ks.metadata(), *qp.proxy().get_token_metadata_ptr(), qp.proxy().features(), qp.db().get_config());

            auto tmptr = qp.proxy().get_token_metadata_ptr();
            const auto& topo = tmptr->get_topology();

            if (ks.get_replication_strategy().uses_tablets()) {
                auto& current_rf_per_dc = ks.metadata()->strategy_options();
                auto new_rf_per_dc = _attrs->get_replication_options();
                new_rf_per_dc.erase(ks_prop_defs::REPLICATION_STRATEGY_CLASS_KEY);
                unsigned total_abs_rfs_diff = 0;
                for (const auto& [new_dc, new_rf] : new_rf_per_dc) {
                    auto old_rf = locator::replication_strategy_config_option(sstring("0"));
                    if (auto new_dc_in_current_mapping = current_rf_per_dc.find(new_dc);
                             new_dc_in_current_mapping != current_rf_per_dc.end()) {
                        old_rf = new_dc_in_current_mapping->second;
                    } else if (!topo.get_datacenters().contains(new_dc)) {
                        // This means that the DC listed in ALTER doesn't exist. This error will be reported later,
                        // during validation in abstract_replication_strategy::validate_replication_strategy.
                        // We can't report this error now, because it'd change the order of errors reported:
                        // first we need to report non-existing DCs, then if RFs aren't changed by too much.
                        continue;
                    }
                    if (total_abs_rfs_diff += get_abs_rf_diff(old_rf, new_rf); total_abs_rfs_diff >= 2) {
                        throw exceptions::invalid_request_exception("Only one DC's RF can be changed at a time and not by more than 1");
                    }
                }
            }

            locator::replication_strategy_params params(new_ks->strategy_options(), new_ks->initial_tablets(), new_ks->consistency_option());
            auto new_rs = locator::abstract_replication_strategy::create_replication_strategy(new_ks->strategy_name(), params, topo);
            if (new_rs->is_per_table() != ks.get_replication_strategy().is_per_table()) {
                throw exceptions::invalid_request_exception(format("Cannot alter replication strategy vnode/tablets flavor"));
            }
            if (new_ks->consistency_option() && new_ks->consistency_option() != ks.metadata()->consistency_option()) {
                throw exceptions::invalid_request_exception(format("Cannot alter consistency option"));
            }
        } catch (const std::runtime_error& e) {
            throw exceptions::invalid_request_exception(e.what());
        }
#if 0
        // The strategy is validated through KSMetaData.validate() in announceKeyspaceUpdate below.
        // However, for backward compatibility with thrift, this doesn't validate unexpected options yet,
        // so doing proper validation here.
        AbstractReplicationStrategy.validateReplicationStrategy(name,
                                                                AbstractReplicationStrategy.getClass(attrs.getReplicationStrategyClass()),
                                                                StorageService.instance.getTokenMetadata(),
                                                                DatabaseDescriptor.getEndpointSnitch(),
                                                                attrs.getReplicationOptions());
#endif
}

bool cql3::statements::alter_keyspace_statement::changes_tablets(query_processor& qp) const {
    auto ks = qp.db().find_keyspace(_name);
    return ks.get_replication_strategy().uses_tablets() && !_attrs->get_replication_options().empty();
}

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, cql3::cql_warnings_vec>>
cql3::statements::alter_keyspace_statement::prepare_schema_mutations(query_processor& qp, service::query_state& state, const query_options& options, service::group0_batch& mc) const {
    using namespace cql_transport;
    bool unknown_keyspace = false;
    try {
        event::schema_change::target_type target_type = event::schema_change::target_type::KEYSPACE;
        auto ks = qp.db().find_keyspace(_name);
        auto ks_md = ks.metadata();
        const auto tmptr = qp.proxy().get_token_metadata_ptr();
        const auto& topo = tmptr->get_topology();
        const auto& feat = qp.proxy().features();
        auto ks_md_update = _attrs->as_ks_metadata_update(ks_md, *tmptr, feat, qp.db().get_config());
        utils::chunked_vector<mutation> muts;
        std::vector<sstring> warnings;

        auto ts = mc.write_timestamp();
        auto global_request_id = mc.new_group0_state_id();

        // we only want to run the tablets path if there are actually any tablets changes, not only schema changes
        // TODO: the current `if (changes_tablets(qp))` is insufficient: someone may set the same RFs as before,
        //       and we'll unnecessarily trigger the processing path for ALTER tablets KS,
        //       when in reality nothing or only schema is being changed
        if (changes_tablets(qp)) {
            if (!qp.proxy().features().topology_global_request_queue && !qp.topology_global_queue_empty()) {
                co_await coroutine::return_exception(
                    exceptions::invalid_request_exception("Another global topology request is ongoing, please retry."));
            }
            if (qp.proxy().features().rack_list_rf && co_await qp.ongoing_rf_change(mc.guard(),_name)) {
                co_await coroutine::return_exception(
                        exceptions::invalid_request_exception(format("Another RF change for this keyspace {} ongoing, please retry.", _name)));
            }
            qp.db().real_database().validate_keyspace_update(*ks_md_update);

            service::topology_mutation_builder builder(ts);
            service::topology_request_tracking_mutation_builder rtbuilder{global_request_id, qp.proxy().features().topology_requests_type_column};
            rtbuilder.set("done", false)
                     .set("start_time", db_clock::now());
            if (!qp.proxy().features().topology_global_request_queue) {
                builder.set_global_topology_request(service::global_topology_request::keyspace_rf_change);
                builder.set_global_topology_request_id(global_request_id);
                builder.set_new_keyspace_rf_change_data(_name, _attrs->flattened());
            } else {
                builder.queue_global_topology_request_id(global_request_id);
                rtbuilder.set("request_type", service::global_topology_request::keyspace_rf_change)
                         .set_new_keyspace_rf_change_data(_name, _attrs->flattened());

            };
            service::topology_change change{{builder.build()}};

            auto topo_schema = qp.db().find_schema(db::system_keyspace::NAME, db::system_keyspace::TOPOLOGY);
            std::ranges::transform(change.mutations, std::back_inserter(muts), [topo_schema] (const canonical_mutation& cm) {
                return cm.to_mutation(topo_schema);
            });

            service::topology_change req_change{{rtbuilder.build()}};

            auto topo_req_schema = qp.db().find_schema(db::system_keyspace::NAME, db::system_keyspace::TOPOLOGY_REQUESTS);
            std::ranges::transform(req_change.mutations, std::back_inserter(muts), [topo_req_schema] (const canonical_mutation& cm) {
                return cm.to_mutation(topo_req_schema);
            });
        } else {
            auto schema_mutations = service::prepare_keyspace_update_announcement(qp.db().real_database(), ks_md_update, ts);
            muts.insert(muts.begin(), schema_mutations.begin(), schema_mutations.end());
        }

        auto rs = locator::abstract_replication_strategy::create_replication_strategy(
                ks_md_update->strategy_name(),
                locator::replication_strategy_params(ks_md_update->strategy_options(), ks_md_update->initial_tablets(), ks_md_update->consistency_option()),
                topo);

        // If `rf_rack_valid_keyspaces` is enabled, it's forbidden to perform a schema change that
        // would lead to an RF-rack-valid keyspace. Verify that this change does not.
        // For more context, see: scylladb/scylladb#23071.
        try {
            // There are two things to note here:
            // 1. We hold a group0_guard, so it's correct to check this here.
            //    The topology or schema cannot change while we're performing this query.
            // 2. The replication strategy we use here does NOT represent the actual state
            //    we will arrive at after applying the schema change. For instance, if the user
            //    did not specify the RF for some of the DCs, it's equal to 0 in the replication
            //    strategy we pass to this function, while in reality that means that the RF
            //    will NOT change. That is not a problem:
            //    - RF=0 is valid for all DCs, so it won't trigger an exception on its own,
            //    - the keyspace must've been RF-rack-valid before this change. We check that
            //      condition for all keyspaces at startup.
            //    The second hyphen is not really true because currently topological changes can
            //    disturb it (see scylladb/scylladb#23345), but we ignore that.
            locator::assert_rf_rack_valid_keyspace(_name, tmptr, *rs);
        } catch (const std::exception& e) {
            if (qp.db().get_config().rf_rack_valid_keyspaces()) {
                // There's no guarantee what the type of the exception will be, so we need to
                // wrap it manually here in a type that can be passed to the user.
                throw exceptions::invalid_request_exception(e.what());
            } else {
                // Even when the configuration option `rf_rack_valid_keyspaces` is set to false,
                // we'd like to inform the user that the keyspace they're altering will not
                // satisfy the restriction after the change--but just as a warning.
                // For more context, see issue: scylladb/scylladb#23330.
                warnings.push_back(seastar::format(
                    "Keyspace '{}' is not RF-rack-valid: the replication factor doesn't match "
                    "the rack count in at least one datacenter. A rack failure may reduce availability. "
                    "For more context, see: "
                    "https://docs.scylladb.com/manual/stable/reference/glossary.html#term-RF-rack-valid-keyspace.",
                    _name));
            }
        }

        auto ret = ::make_shared<event::schema_change>(
                event::schema_change::change_type::UPDATED,
                target_type,
                keyspace());
        mc.add_mutations(std::move(muts), "CQL alter keyspace");
        co_return std::make_tuple(std::move(ret), warnings);
    } catch (data_dictionary::no_such_keyspace& e) {
        unknown_keyspace = true;
    }
    if (unknown_keyspace) {
        co_await coroutine::return_exception(
                exceptions::invalid_request_exception("Unknown keyspace " + _name));
    }
    std::unreachable();
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::alter_keyspace_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(audit_info(), make_shared<alter_keyspace_statement>(*this));
}


future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::alter_keyspace_statement::execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const {
    std::vector<sstring> warnings = check_against_restricted_replication_strategies(qp, keyspace(), *_attrs, qp.get_cql_stats());
    return schema_altering_statement::execute(qp, state, options, std::move(guard)).then([warnings = std::move(warnings)] (::shared_ptr<messages::result_message> msg) {
        for (const auto& warning : warnings) {
            msg->add_warning(warning);
            mylogger.warn("{}", warning);
        }
        return msg;
    });
}
