/*
 * Copyright 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/core/coroutine.hh>
#include "cql3/statements/create_keyspace_statement.hh"
#include "cql3/statements/ks_prop_defs.hh"
#include "prepared_statement.hh"
#include "data_dictionary/data_dictionary.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "mutation/mutation.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "cql3/query_processor.hh"
#include "db/config.hh"
#include "gms/feature_service.hh"

#include <boost/regex.hpp>
#include <stdexcept>

bool is_system_keyspace(std::string_view keyspace);

namespace cql3 {

namespace statements {

static logging::logger mylogger("create_keyspace");

create_keyspace_statement::create_keyspace_statement(const sstring& name, shared_ptr<ks_prop_defs> attrs, bool if_not_exists)
    : _name{name}
    , _attrs{attrs}
    , _if_not_exists{if_not_exists}
{
}

const sstring& create_keyspace_statement::keyspace() const
{
    return _name;
}

future<> create_keyspace_statement::check_access(query_processor& qp, const service::client_state& state) const
{
    return state.has_all_keyspaces_access(auth::permission::CREATE);
}

void create_keyspace_statement::validate(query_processor& qp, const service::client_state& state) const
{
    std::string name;
    name.resize(_name.length());
    std::transform(_name.begin(), _name.end(), name.begin(), ::tolower);
    if (is_system_keyspace(name)) {
        throw exceptions::invalid_request_exception("system keyspace is not user-modifiable");
    }
    // keyspace name
    boost::regex name_regex("\\w+");
    if (!boost::regex_match(name, name_regex)) {
        throw exceptions::invalid_request_exception(format("\"{}\" is not a valid keyspace name", _name.c_str()));
    }
    if (name.length() > schema::NAME_LENGTH) {
        throw exceptions::invalid_request_exception(format("Keyspace names shouldn't be more than {:d} characters long (got \"{}\")", schema::NAME_LENGTH, _name.c_str()));
    }

    _attrs->validate();

    if (!bool(_attrs->get_replication_strategy_class())) {
        throw exceptions::configuration_exception("Missing mandatory replication strategy class");
    }
    try {
        _attrs->get_storage_options();
    } catch (const std::runtime_error& e) {
        throw exceptions::invalid_request_exception(e.what());
    }
#if 0
    // The strategy is validated through KSMetaData.validate() in announceNewKeyspace below.
    // However, for backward compatibility with thrift, this doesn't validate unexpected options yet,
    // so doing proper validation here.
    AbstractReplicationStrategy.validateReplicationStrategy(name,
                                                            AbstractReplicationStrategy.getClass(attrs.getReplicationStrategyClass()),
                                                            StorageService.instance.getTokenMetadata(),
                                                            DatabaseDescriptor.getEndpointSnitch(),
                                                            attrs.getReplicationOptions());
#endif
}

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>> create_keyspace_statement::prepare_schema_mutations(query_processor& qp, const query_options&, api::timestamp_type ts) const {
    using namespace cql_transport;
    const auto& tm = *qp.proxy().get_token_metadata_ptr();
    const auto& feat = qp.proxy().features();
    std::vector<mutation> m;
    std::vector<sstring> warnings;

    try {
        auto ksm = _attrs->as_ks_metadata(_name, tm, feat);
        m = service::prepare_new_keyspace_announcement(qp.db().real_database(), ksm, ts);
        // If the new keyspace uses tablets, as long as there are features
        // which aren't supported by tablets we want to warn the user that
        // they will not be usable on the new keyspace - and suggest how a
        // keyspace can be created without tablets (see rationale in #16807).
        // Once all feature will become supported with tablets, we should
        // remove this check.
        auto rs = locator::abstract_replication_strategy::create_replication_strategy(
            ksm->strategy_name(),
            locator::replication_strategy_params(ksm->strategy_options(), ksm->initial_tablets()));
        if (rs->uses_tablets()) {
            warnings.push_back(
                "Tables in this keyspace will be replicated using Tablets "
                "and will not support CDC, LWT and counters features. "
                "To use CDC, LWT or counters, drop this keyspace and re-create it "
                "without tablets by adding AND TABLETS = {'enabled': false} "
                "to the CREATE KEYSPACE statement.");
        }
    } catch (const exceptions::already_exists_exception& e) {
        if (!_if_not_exists) {
          co_return coroutine::exception(std::current_exception());
        }
    }

    // If an IF NOT EXISTS clause was used and resource was already created
    // we shouldn't emit created event. However it interacts badly with
    // concurrent clients creating resources. The client seeing no create event
    // assumes resource already previously existed and proceeds with its logic
    // which may depend on that resource. But it may send requests to nodes which
    // are not yet aware of new schema or client's metadata may be outdated.
    // To force synchronization always emit the event (see
    // github.com/scylladb/scylladb/issues/16909).
    co_return std::make_tuple(created_event(), std::move(m), std::move(warnings));
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::create_keyspace_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<create_keyspace_statement>(*this));
}

future<> cql3::statements::create_keyspace_statement::grant_permissions_to_creator(const service::client_state& cs, service::group0_batch& mc) const {
    auto resource = auth::make_data_resource(keyspace());
    try {
        co_await auth::grant_applicable_permissions(
                *cs.get_auth_service(),
                *cs.user(),
                resource,
                mc);
    } catch (const auth::unsupported_authorization_operation&) {
        // Nothing.
    }
}

// Check for replication strategy choices which are restricted by the
// configuration. This check can throw a configuration_exception immediately
// if the strategy is forbidden by the configuration, or return a warning
// string if the restriction was set to warning level.
// This function is only supposed to check for replication strategies
// restricted by the configuration. Checks for other types of strategy
// errors (such as unknown replication strategy name or unknown options
// to a known replication strategy) are done elsewhere.
std::vector<sstring> check_against_restricted_replication_strategies(
    query_processor& qp,
    const sstring& keyspace,
    const ks_prop_defs& attrs,
    cql_stats& stats)
{
    if (!attrs.get_replication_strategy_class()) {
        return {};
    }

    std::vector<sstring> warnings;
    locator::replication_strategy_config_options opts;
    locator::replication_strategy_params params(opts, std::nullopt);
    auto replication_strategy = locator::abstract_replication_strategy::create_replication_strategy(
            locator::abstract_replication_strategy::to_qualified_class_name(
                    *attrs.get_replication_strategy_class()), params)->get_type();
    auto rs_warn_list = qp.db().get_config().replication_strategy_warn_list();
    auto rs_fail_list = qp.db().get_config().replication_strategy_fail_list();

    if (replication_strategy == locator::replication_strategy_type::simple) {
        if (auto simple_strategy_restriction = qp.db().get_config().restrict_replication_simplestrategy();
                simple_strategy_restriction == db::tri_mode_restriction_t::mode::TRUE) {
            rs_fail_list.emplace_back(locator::replication_strategy_type::simple);
        } else if (simple_strategy_restriction == db::tri_mode_restriction_t::mode::WARN) {
            rs_warn_list.emplace_back(locator::replication_strategy_type::simple);
        } else if (auto &topology = qp.proxy().get_token_metadata_ptr()->get_topology();
                topology.get_datacenter_endpoints().size() > 1) {
            // Scylla was configured to allow SimpleStrategy, but let's warn
            // if it's used on a cluster which *already* has multiple DCs:
            warnings.emplace_back("Using SimpleStrategy in a multi-datacenter environment is not recommended.");
        }
    }

    if (auto present_on_fail_list = std::find(rs_fail_list.begin(), rs_fail_list.end(), replication_strategy); present_on_fail_list != rs_fail_list.end()) {
        ++stats.replication_strategy_fail_list_violations;
        throw exceptions::configuration_exception(format(
                "{} replication class is not recommended, and forbidden by the current configuration, "
                "but was used for keyspace {}. You may override this restriction by modifying "
                "replication_strategy_fail_list configuration option to not list {}.",
                *attrs.get_replication_strategy_class(), keyspace, *attrs.get_replication_strategy_class()));
    }
    if (auto present_on_warn_list = std::find(rs_warn_list.begin(), rs_warn_list.end(), replication_strategy); present_on_warn_list != rs_warn_list.end()) {
        ++stats.replication_strategy_warn_list_violations;
        warnings.push_back(format("{} replication class is not recommended, but was used for keyspace {}. "
                           "You may suppress this warning by delisting {} from replication_strategy_warn_list configuration option, "
                           "or make it into an error by listing this replication strategy on replication_strategy_fail_list.",
                           *attrs.get_replication_strategy_class(), keyspace, *attrs.get_replication_strategy_class()));
    }

    // The {minimum,maximum}_replication_factor_{warn,fail}_threshold configuration option can be used to forbid
    // a smaller/greater replication factor. We assume that all numeric replication
    // options except for initial_tablets are replication factors - this is true for both
    // SimpleStrategy and NetworkTopologyStrategy
    // A zero replication factor is not forbidden - it is the traditional
    // way to avoid replication on some DC.
    // We ignore errors (non-number, negative number, etc.) here,
    // these are checked and reported elsewhere.
    for (auto opt : attrs.get_replication_options()) {
        if (opt.first == sstring("initial_tablets")) {
            continue;
        }
        try {
            auto rf = std::stol(opt.second);
            if (rf > 0) {
                if (auto min_fail = qp.proxy().data_dictionary().get_config().minimum_replication_factor_fail_threshold();
                    min_fail >= 0 && rf < min_fail) {
                    ++stats.minimum_replication_factor_fail_violations;
                    throw exceptions::configuration_exception(format(
                            "Replication Factor {}={} is forbidden by the current "
                            "configuration setting of minimum_replication_factor_fail_threshold={}. Please "
                            "increase replication factor, or lower minimum_replication_factor_fail_threshold "
                            "set in the configuration.", opt.first, rf,
                            qp.proxy().data_dictionary().get_config().minimum_replication_factor_fail_threshold()));
                }
                else if (auto max_fail = qp.proxy().data_dictionary().get_config().maximum_replication_factor_fail_threshold();
                         max_fail >= 0 && rf > max_fail) {
                    ++stats.maximum_replication_factor_fail_violations;
                    throw exceptions::configuration_exception(format(
                            "Replication Factor {}={} is forbidden by the current "
                            "configuration setting of maximum_replication_factor_fail_threshold={}. Please "
                            "decrease replication factor, or increase maximum_replication_factor_fail_threshold "
                            "set in the configuration.", opt.first, rf,
                            qp.proxy().data_dictionary().get_config().maximum_replication_factor_fail_threshold()));
                }
                else if (auto min_warn = qp.proxy().data_dictionary().get_config().minimum_replication_factor_warn_threshold();
                         min_warn >= 0 && rf < min_warn)
                {
                    ++stats.minimum_replication_factor_warn_violations;
                    warnings.push_back(format("Using Replication Factor {}={} lower than the "
                                              "minimum_replication_factor_warn_threshold={} is not recommended.", opt.first, rf,
                                              qp.proxy().data_dictionary().get_config().minimum_replication_factor_warn_threshold()));
                }
                else if (auto max_warn = qp.proxy().data_dictionary().get_config().maximum_replication_factor_warn_threshold();
                        max_warn >= 0 && rf > max_warn)
                {
                    ++stats.maximum_replication_factor_warn_violations;
                    warnings.push_back(format("Using Replication Factor {}={} greater than the "
                                              "maximum_replication_factor_warn_threshold={} is not recommended.", opt.first, rf,
                                              qp.proxy().data_dictionary().get_config().maximum_replication_factor_warn_threshold()));
                }
            }
        } catch (std::invalid_argument&) {
        } catch (std::out_of_range& ) {
        }
    }
    return warnings;
}

future<::shared_ptr<messages::result_message>>
create_keyspace_statement::execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const {
    std::vector<sstring> warnings = check_against_restricted_replication_strategies(qp, keyspace(), *_attrs, qp.get_cql_stats());
        return schema_altering_statement::execute(qp, state, options, std::move(guard)).then([warnings = std::move(warnings)] (::shared_ptr<messages::result_message> msg) {
        for (const auto& warning : warnings) {
            msg->add_warning(warning);
            mylogger.warn("{}", warning);
        }
        return msg;
    });
}

lw_shared_ptr<data_dictionary::keyspace_metadata> create_keyspace_statement::get_keyspace_metadata(const locator::token_metadata& tm, const gms::feature_service& feat) {
    _attrs->validate();
    return _attrs->as_ks_metadata(_name, tm, feat);
}

::shared_ptr<schema_altering_statement::event_t> create_keyspace_statement::created_event() const {
    return make_shared<event_t>(
            event_t::change_type::CREATED,
            event_t::target_type::KEYSPACE,
            keyspace());
}

}

}
