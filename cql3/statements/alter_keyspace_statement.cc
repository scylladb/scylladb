/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <boost/range/algorithm.hpp>
#include <fmt/format.h>
#include <seastar/core/coroutine.hh>
#include <seastar/core/on_internal_error.hh>
#include <stdexcept>
#include "alter_keyspace_statement.hh"
#include "prepared_statement.hh"
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

// We want to ensure that each DC's RF is going to change by at most 1
static bool validate_rf_difference(const std::string& curr_rf, const std::string& new_rf) {
    try {
        return std::abs(std::stoi(curr_rf) - std::stoi(new_rf)) <= 1;
    } catch (std::invalid_argument const& ex) {
        on_internal_error(mylogger, format("validate_rf_difference expects integer arguments, "
                                 "but got curr_rf:{} and new_rf:{}", curr_rf, new_rf));
    } catch (std::out_of_range const& ex) {
        on_internal_error(mylogger, format("validate_rf_difference expects integer arguments to fit into `int` type, "
                                 "but got curr_rf:{} and new_rf:{}", curr_rf, new_rf));
    }
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
                throw exceptions::invalid_request_exception(format("Cannot alter storage options: {} to {} is not supported",
                        current_options.type_string(), new_options.type_string()));
            }

            auto new_ks = _attrs->as_ks_metadata_update(ks.metadata(), *qp.proxy().get_token_metadata_ptr(), qp.proxy().features());

            if (ks.get_replication_strategy().uses_tablets()) {
                const std::map<sstring, sstring>& current_rf_per_dc = ks.metadata()->strategy_options();
                auto new_rf_per_dc = _attrs->get_replication_options();
                new_rf_per_dc.erase(ks_prop_defs::REPLICATION_STRATEGY_CLASS_KEY);
                for (const auto& [new_dc, new_rf] : new_rf_per_dc) {
                    sstring old_rf = "0";
                    if (auto new_dc_in_current_mapping = current_rf_per_dc.find(new_dc);
                             new_dc_in_current_mapping != current_rf_per_dc.end()) {
                        old_rf = new_dc_in_current_mapping->second;
                    } else if (!qp.proxy().get_token_metadata_ptr()->get_topology().get_datacenters().contains(new_dc)) {
                        // This means that the DC listed in ALTER doesn't exist. This error will be reported later,
                        // during validation in abstract_replication_strategy::validate_replication_strategy.
                        // We can't report this error now, because it'd change the order of errors reported:
                        // first we need to report non-existing DCs, then if RFs aren't changed by too much.
                        continue;
                    }
                    if (!validate_rf_difference(old_rf, new_rf)) {
                        throw exceptions::invalid_request_exception("Cannot modify replication factor of any DC by more than 1 at a time.");
                    }
                }
            }

            locator::replication_strategy_params params(new_ks->strategy_options(), new_ks->initial_tablets());
            auto new_rs = locator::abstract_replication_strategy::create_replication_strategy(new_ks->strategy_name(), params);
            if (new_rs->is_per_table() != ks.get_replication_strategy().is_per_table()) {
                throw exceptions::invalid_request_exception(format("Cannot alter replication strategy vnode/tablets flavor"));
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
    try {
        event::schema_change::target_type target_type = event::schema_change::target_type::KEYSPACE;
        auto ks = qp.db().find_keyspace(_name);
        auto ks_md = ks.metadata();
        const auto& tm = *qp.proxy().get_token_metadata_ptr();
        const auto& feat = qp.proxy().features();
        auto ks_md_update = _attrs->as_ks_metadata_update(ks_md, tm, feat);
        std::vector<mutation> muts;
        std::vector<sstring> warnings;
        auto ks_options = _attrs->get_all_options_flattened(feat);
        auto ts = mc.write_timestamp();
        auto global_request_id = mc.new_group0_state_id();

        // we only want to run the tablets path if there are actually any tablets changes, not only schema changes
        if (changes_tablets(qp)) {
            if (!qp.topology_global_queue_empty()) {
                return make_exception_future<std::tuple<::shared_ptr<::cql_transport::event::schema_change>, cql3::cql_warnings_vec>>(
                        exceptions::invalid_request_exception("Another global topology request is ongoing, please retry."));
            }
            if (_attrs->get_replication_options().contains(ks_prop_defs::REPLICATION_FACTOR_KEY)) {
                return make_exception_future<std::tuple<::shared_ptr<::cql_transport::event::schema_change>, cql3::cql_warnings_vec>>(
                       exceptions::invalid_request_exception("'replication_factor' tag is not allowed when executing ALTER KEYSPACE with tablets, please list the DCs explicitly"));
            }
            qp.db().real_database().validate_keyspace_update(*ks_md_update);

            service::topology_mutation_builder builder(ts);
            builder.set_global_topology_request(service::global_topology_request::keyspace_rf_change);
            builder.set_global_topology_request_id(global_request_id);
            builder.set_new_keyspace_rf_change_data(_name, ks_options);
            service::topology_change change{{builder.build()}};

            auto topo_schema = qp.db().find_schema(db::system_keyspace::NAME, db::system_keyspace::TOPOLOGY);
            boost::transform(change.mutations, std::back_inserter(muts), [topo_schema] (const canonical_mutation& cm) {
                return cm.to_mutation(topo_schema);
            });

            service::topology_request_tracking_mutation_builder rtbuilder{global_request_id};
            rtbuilder.set("done", false)
                     .set("start_time", db_clock::now());
            service::topology_change req_change{{rtbuilder.build()}};

            auto topo_req_schema = qp.db().find_schema(db::system_keyspace::NAME, db::system_keyspace::TOPOLOGY_REQUESTS);
            boost::transform(req_change.mutations, std::back_inserter(muts), [topo_req_schema] (const canonical_mutation& cm) {
                return cm.to_mutation(topo_req_schema);
            });
        } else {
            auto schema_mutations = service::prepare_keyspace_update_announcement(qp.db().real_database(), ks_md_update, ts);
            muts.insert(muts.begin(), schema_mutations.begin(), schema_mutations.end());
        }

        auto ret = ::make_shared<event::schema_change>(
                event::schema_change::change_type::UPDATED,
                target_type,
                keyspace());
        mc.add_mutations(std::move(muts), "CQL alter keyspace");
        return make_ready_future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, cql3::cql_warnings_vec>>(std::make_tuple(std::move(ret), warnings));
    } catch (data_dictionary::no_such_keyspace& e) {
        return make_exception_future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, cql3::cql_warnings_vec>>(exceptions::invalid_request_exception("Unknown keyspace " + _name));
    }
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::alter_keyspace_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<alter_keyspace_statement>(*this));
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
