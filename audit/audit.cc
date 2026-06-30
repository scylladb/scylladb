/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/future-util.hh>
#include "audit/audit.hh"
#include "audit/audit_rule.hh"
#include "audit/preprocessed_audit_rules.hh"
#include "utils/rjson.hh"
#include "db/config.hh"
#include "cql3/cql_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/batch_statement.hh"
#include "cql3/statements/modification_statement.hh"
#include "storage_helper.hh"
#include "audit_cf_storage_helper.hh"
#include "audit_syslog_storage_helper.hh"
#include "audit_composite_storage_helper.hh"
#include "audit.hh"
#include "../db/config.hh"
#include "service/migration_listener.hh"
#include "service/migration_manager.hh"

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/algorithm/string/classification.hpp>


namespace audit {

logging::logger logger("audit");

class audit_schema_listener : public service::migration_listener::empty_listener {
    preprocessed_audit_rules& _rules;

public:
    audit_schema_listener(preprocessed_audit_rules& rules)
        : _rules(rules) {}

    void on_create_column_family(const sstring& ks_name, const sstring& cf_name) override {
        logger.debug("Audit: table {}.{} created, adding to known tables", ks_name, cf_name);
        _rules.add_known_table(ks_name, cf_name);
    }

    void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) override {
        logger.debug("Audit: table {}.{} dropped, removing from known tables", ks_name, cf_name);
        _rules.remove_known_table(ks_name, cf_name);
    }

    void on_create_view(const sstring& ks_name, const sstring& view_name) override {
        logger.debug("Audit: table {}.{} created, adding to known tables", ks_name, view_name);
        _rules.add_known_table(ks_name, view_name);
    }

    void on_drop_view(const sstring& ks_name, const sstring& view_name) override {
        logger.debug("Audit: table {}.{} dropped, removing from known tables", ks_name, view_name);
        _rules.remove_known_table(ks_name, view_name);
    }
};

static audit_sink_set parse_audit_sinks(const sstring& data) {
    audit_sink_set result;
    if (!data.empty()) {
        std::vector<sstring> audit_modes;
        boost::split(audit_modes, data, boost::is_any_of(","));
        if (audit_modes.empty()) {
            return {};
        }
        for (sstring& audit_mode : audit_modes) {
            boost::trim(audit_mode);
            if (audit_mode == "none") {
                return {};
            }
            if (audit_mode == "table") {
                result.set(audit_sink::table);
            } else if (audit_mode == "syslog") {
                result.set(audit_sink::syslog);
            } else {
                throw audit_exception(fmt::format("Bad configuration: invalid 'audit': {}", audit_mode));
            }
        }
    }
    return result;
}

static void warn_on_sink_mismatch(const std::vector<audit_rule>& rules, audit_sink_set enabled_sinks) {
    for (size_t i = 0; i < rules.size(); ++i) {
        for (const auto& sink_name : rules[i].sinks) {
            bool supported = (sink_name == "table" && enabled_sinks.contains(audit_sink::table))
                          || (sink_name == "syslog" && enabled_sinks.contains(audit_sink::syslog));
            if (!supported) {
                logger.error("Audit rule {} references sink '{}' but the global 'audit' config does not enable it. "
                             "Events matching this rule will not be written to '{}'.",
                             i, sink_name, sink_name);
            }
        }
    }
}

static std::unique_ptr<storage_helper> create_storage_helper(audit_sink_set audit_sinks, cql3::query_processor& qp, service::migration_manager& mm) {
    SCYLLA_ASSERT(audit_sinks);

    std::vector<std::unique_ptr<storage_helper>> helpers;
    if (audit_sinks.contains(audit_sink::table)) {
        helpers.emplace_back(std::make_unique<audit_cf_storage_helper>(qp, mm));
    }
    if (audit_sinks.contains(audit_sink::syslog)) {
        helpers.emplace_back(std::make_unique<audit_syslog_storage_helper>(qp, mm));
    }

    SCYLLA_ASSERT(!helpers.empty());
    if (helpers.size() == 1) {
        return std::move(helpers.front());
    }
    return std::make_unique<audit_composite_storage_helper>(std::move(helpers));
}

sstring audit_info::category_string() const {
    return category_to_string(_category);
}

static category_set parse_audit_categories(const sstring& data) {
    category_set result;
    if (!data.empty()) {
        std::vector<sstring> tokens;
        boost::split(tokens, data, boost::is_any_of(","));
        for (sstring& category : tokens) {
            boost::trim(category);
            if (category == "QUERY") {
                result.set(statement_category::QUERY);
            } else if (category == "DML") {
                result.set(statement_category::DML);
            } else if (category == "DDL") {
                result.set(statement_category::DDL);
            } else if (category == "DCL") {
                result.set(statement_category::DCL);
            } else if (category == "AUTH") {
                result.set(statement_category::AUTH);
            } else if (category == "ADMIN") {
                result.set(statement_category::ADMIN);
            } else {
                throw audit_exception(fmt::format("Bad configuration: invalid 'audit_categories': {}", data));
            }
        }
    }
    return result;
}

static audit::audited_tables_t parse_audit_tables(const sstring& data) {
    audit::audited_tables_t result;
    if (!data.empty()) {
        std::vector<sstring> tokens;
        boost::split(tokens, data, boost::is_any_of(","));
        for (sstring& token : tokens) {
            std::vector<sstring> parts;
            boost::split(parts, token, boost::is_any_of("."));
            if (parts.size() != 2) {
                throw audit_exception(fmt::format("Bad configuration: invalid 'audit_tables': {}", data));
            }
            boost::trim(parts[0]);
            boost::trim(parts[1]);
            // The real keyspace name of an Alternator table T is
            // "alternator_T". The audit_tables config flag uses the format
            // "alternator.T" to refer to such tables, so we expand it here
            // to the real keyspace name.
            if (parts[0] == "alternator") {
                parts[0] = "alternator_" + parts[1];
            }
            result[parts[0]].insert(std::move(parts[1]));
        }
    }
    return result;
}

static audit::audited_keyspaces_t parse_audit_keyspaces(const sstring& data) {
    audit::audited_keyspaces_t result;
    if (!data.empty()) {
        std::vector<sstring> tokens;
        boost::split(tokens, data, boost::is_any_of(","));
        for (sstring& token : tokens) {
            boost::trim(token);
            result.insert(std::move(token));
        }
    }
    return result;
}

audit::audit(locator::shared_token_metadata& token_metadata,
             cql3::query_processor& qp,
             service::migration_manager& mm,
             audit_sink_set audit_sinks,
             audited_keyspaces_t&& audited_keyspaces,
             audited_tables_t&& audited_tables,
             category_set&& audited_categories,
             std::vector<audit_rule>&& audit_rules,
             const db::config& cfg)
    : _token_metadata(token_metadata)
    , _audited_keyspaces(std::move(audited_keyspaces))
    , _audited_tables(std::move(audited_tables))
    , _audited_categories(std::move(audited_categories))
    , _audit_sinks(audit_sinks)
    , _preprocessed_rules(std::move(audit_rules))
    , _pending_writes("audit::pending_writes")
    , _schema_listener(std::make_unique<audit_schema_listener>(_preprocessed_rules))
    , _migration_notifier(mm.get_notifier())
    , _cfg(cfg)
    , _cfg_keyspaces_observer(cfg.audit_keyspaces.observe([this] (sstring const& new_value){ update_config<audited_keyspaces_t>(new_value, parse_audit_keyspaces, _audited_keyspaces); }))
    , _cfg_tables_observer(cfg.audit_tables.observe([this] (sstring const& new_value){ update_config<audited_tables_t>(new_value, parse_audit_tables, _audited_tables); }))
    , _cfg_categories_observer(cfg.audit_categories.observe([this] (sstring const& new_value){ update_config<category_set>(new_value, parse_audit_categories, _audited_categories); }))
    , _rules_rebuild_action([this] { return rebuild_rules(); })
{
    _cfg_rules_observer.emplace(cfg.audit_rules.observe(_rules_rebuild_action.make_observer()));
    _storage_helper_ptr = create_storage_helper(audit_sinks, qp, mm);
}

audit::~audit() = default;

future<> audit::rebuild_rules() {
    auto new_rules = _cfg.audit_rules();
    logger.info("Updating audit rules: {} rules configured.", new_rules.size());
    warn_on_sink_mismatch(new_rules, _audit_sinks);
    co_await _preprocessed_rules.refresh_rules(std::move(new_rules));
    logger.info("Audit rules updated: {} rules configured.", _preprocessed_rules.rules().size());
}

future<> audit::start_audit(const db::config& cfg, sharded<locator::shared_token_metadata>& stm, sharded<cql3::query_processor>& qp, sharded<service::migration_manager>& mm) {
    audit_sink_set audit_sinks = parse_audit_sinks(cfg.audit());
    auto audit_rules = cfg.audit_rules();

    if (!audit_sinks) {
        if (!audit_rules.empty()) {
            logger.warn("Audit rules are configured but audit is disabled (audit='none'). "
                        "Set 'audit' to 'table' or 'syslog' to enable audit rules.");
        }
        logger.info("Audit is disabled");
        return make_ready_future<>();
    }
    category_set audited_categories = parse_audit_categories(cfg.audit_categories());
    audit::audited_tables_t audited_tables = parse_audit_tables(cfg.audit_tables());
    audit::audited_keyspaces_t audited_keyspaces = parse_audit_keyspaces(cfg.audit_keyspaces());

    logger.info("Audit is enabled. Auditing to: \"{}\", with the following categories: \"{}\", keyspaces: \"{}\", tables: \"{}\", rules: {}",
                cfg.audit(), cfg.audit_categories(), cfg.audit_keyspaces(), cfg.audit_tables(), audit_rules.size());

    warn_on_sink_mismatch(audit_rules, audit_sinks);

    return audit_instance().start(std::ref(stm),
                                  std::ref(qp),
                                  std::ref(mm),
                                  audit_sinks,
                                  std::move(audited_keyspaces),
                                  std::move(audited_tables),
                                  std::move(audited_categories),
                                  std::move(audit_rules),
                                  std::cref(cfg));
}

future<> audit::start_storage(const db::config& cfg) {
    if (!audit_instance().local_is_initialized()) {
        co_return;
    }
    co_await audit_instance().invoke_on_all([] (audit& local_audit) {
        local_audit._migration_notifier.register_listener(local_audit._schema_listener.get());
    });
    co_await audit_instance().invoke_on_all([&cfg] (audit& local_audit) -> future<> {
        co_await local_audit._storage_helper_ptr->start(cfg);
        local_audit._storage_started = true;
    });
}

future<> audit::stop_storage() {
    if (!audit_instance().local_is_initialized()) {
        return make_ready_future<>();
    }
    return audit_instance().invoke_on_all([] (audit& local_audit) -> future<> {
        // Closing the gate is the single shutdown signal: in-flight writes are
        // drained and any write entering afterwards is rejected by try_with_gate.
        // The storage-ready flag is not cleared here; it only guards startup.
        co_await local_audit._pending_writes.close();
        co_await local_audit._storage_helper_ptr->stop();
    });
}

future<> audit::stop_audit() {
    if (!audit_instance().local_is_initialized()) {
        return make_ready_future<>();
    }
    return audit::audit::audit_instance().invoke_on_all([] (auto& local_audit) {
        return local_audit.shutdown();
    }).then([] {
        return audit::audit::audit_instance().stop();
    });
}

audit_info_ptr audit::create_audit_info(statement_category cat, const sstring& keyspace, const sstring& table, bool batch) {
    if (!audit_instance().local_is_initialized()) {
        return nullptr;
    }
    return std::make_unique<audit_info>(cat, keyspace, table, batch);
}

future<> audit::shutdown() {
    _cfg_rules_observer.reset();
    co_await _migration_notifier.unregister_listener(_schema_listener.get());
    co_await _rules_rebuild_action.join();
}

future<> audit::write_to_storage(audit_sink_set sinks,
                                 const audit_info& audit_info,
                                 socket_address node_ip,
                                 socket_address client_ip,
                                 std::optional<db::consistency_level> cl,
                                 const sstring& username,
                                 bool error) {
    return try_with_gate(_pending_writes, [this, sinks, &audit_info, node_ip, client_ip, cl, &username, error] {
        return _storage_helper_ptr->write(sinks, &audit_info, node_ip, client_ip, cl, username, error);
    });
}

future<> audit::write_login_to_storage(audit_sink_set sinks,
                                       const sstring& username,
                                       socket_address node_ip,
                                       socket_address client_ip,
                                       bool error) {
    return try_with_gate(_pending_writes, [this, sinks, &username, node_ip, client_ip, error] {
        return _storage_helper_ptr->write_login(sinks, username, node_ip, client_ip, error);
    });
}

future<> audit::log(const audit_info& audit_info, const service::client_state& client_state, std::optional<db::consistency_level> cl, bool error) {
    std::string_view role;
    if (!_preprocessed_rules.rules().empty() && client_state.user() && client_state.user()->name) {
        role = *client_state.user()->name;
    }
    thread_local static sstring no_username("undefined");
    static const sstring anonymous_username("anonymous");
    const sstring& username = client_state.user() ? client_state.user()->name.value_or(anonymous_username) : no_username;
    socket_address client_ip = client_state.get_client_address().addr();
    socket_address node_ip = _token_metadata.get()->get_topology().my_address().addr();
    if (audit_info.alternator_batch_tables()) {
        return log_alternator_batch(audit_info, role, node_ip, client_ip, cl, username, error);
    }
    auto sinks = sinks_for(audit_info, role);
    if (!sinks) {
        return make_ready_future<>();
    }
    return log_with_sinks(sinks, audit_info, node_ip, client_ip, cl, username, error);
}

future<> audit::log_with_sinks(audit_sink_set sinks, const audit_info& audit_info, socket_address node_ip, socket_address client_ip,
        std::optional<db::consistency_level> cl, const sstring& username, bool error) {
    if (!sinks) {
        return make_ready_future<>();
    }
    if (!_storage_started) {
        on_internal_error_noexcept(logger, fmt::format("Audit log dropped (storage not ready): node_ip {} category {} cl {} error {} keyspace {} query '{}' client_ip {} table {} username {}",
            node_ip, audit_info.category_string(), cl, error, audit_info.keyspace(),
            audit_info.query(), client_ip, audit_info.table(), username));
        return make_ready_future<>();
    }
    if (logger.is_enabled(logging::log_level::debug)) {
        logger.debug("Log written: node_ip {} category {} cl {} error {} keyspace {} query '{}' client_ip {} table {} username {}",
            node_ip, audit_info.category_string(), cl, error, audit_info.keyspace(),
            audit_info.query(), client_ip, audit_info.table(), username);
    }
    return write_to_storage(sinks, audit_info, node_ip, client_ip, cl, username, error)
        .handle_exception([audit_info, node_ip, client_ip, cl, username, error] (std::exception_ptr ep) {
            try {
                std::rethrow_exception(ep);
            } catch (const seastar::gate_closed_exception&) {
                // The write raced with stop_storage() closing the gate. No audit
                // generating request should be in flight once storage stops.
                on_internal_error_noexcept(logger, fmt::format("Audit log dropped (storage stopped): node_ip {} category {} cl {} error {} keyspace {} query '{}' client_ip {} table {} username {}",
                    node_ip, audit_info.category_string(), cl, error, audit_info.keyspace(),
                    audit_info.query(), client_ip, audit_info.table(), username));
            } catch (...) {
                logger.error("Unexpected exception when writing log with: node_ip {} category {} cl {} error {} keyspace {} query '{}' client_ip {} table {} username {} exception {}",
                    node_ip, audit_info.category_string(), cl, error, audit_info.keyspace(),
                    audit_info.query(), client_ip, audit_info.table(), username, ep);
            }
    });
}

static sstring print_alternator_table_names(const audit_table_set& tables) {
    sstring res;
    for (const auto& [_, table] : tables) {
        if (!res.empty()) {
            res += "|";
        }
        res += table;
    }
    return res;
}

static sstring print_filtered_alternator_batch_query(const audit_info& audit_info, const audit_table_set& tables) {
    // Alternator audit query strings are built by audit_info::set_query_string()
    // as "<operation>|<serialized request JSON>".
    auto sep = audit_info.query().find('|');
    if (sep == sstring::npos) {
        return audit_info.query();
    }
    auto operation = audit_info.query().substr(0, sep);
    try {
        auto request = rjson::parse(std::string_view(audit_info.query()).substr(sep + 1));
        auto& request_items = request["RequestItems"];
        for (auto it = request_items.MemberBegin(); it != request_items.MemberEnd(); ) {
            std::string_view table_name = rjson::to_string_view(it->name);
            auto found = std::ranges::any_of(tables, [table_name] (const auto& table) {
                return table.second == table_name;
            });
            if (!found) {
                it = request_items.EraseMember(it);
            } else {
                ++it;
            }
        }
        return operation + "|" + rjson::print(request);
    } catch (...) {
        // Do not fall back to the unfiltered query: it may contain data for
        // tables that do not match this audit sink.
        return operation + "|<batch audit request omitted: unexpected audit query format>";
    }
}

void add_alternator_batch_sink_tables(std::vector<std::pair<audit_sink, audit_table_set>>& sink_tables,
        audit_sink sink, const std::pair<sstring, sstring>& table) {
    auto it = std::ranges::find_if(sink_tables, [sink] (const auto& entry) {
        return entry.first == sink;
    });
    if (it == sink_tables.end()) {
        sink_tables.emplace_back(sink, audit_table_set{table});
    } else {
        it->second.insert(table);
    }
}

future<> audit::log_alternator_batch(const audit_info& ai, std::string_view role, socket_address node_ip, socket_address client_ip,
        std::optional<db::consistency_level> cl, const sstring& username, bool error) {
    std::vector<std::pair<audit_sink, audit_table_set>> sink_tables;
    for (const auto& table : *ai.alternator_batch_tables()) {
        auto sinks = sinks_for_table(ai.category(), table.first, table.second, role);
        for (auto sink : sinks) {
            add_alternator_batch_sink_tables(sink_tables, sink, table);
        }
    }

    return do_with(std::move(sink_tables), [this, &ai, node_ip, client_ip, cl, &username, error] (auto& sink_tables) {
        return do_for_each(sink_tables, [this, &ai, node_ip, client_ip, cl, &username, error] (const auto& entry) {
            return do_with(::audit::audit_info(ai.category(), sstring(ai.keyspace()), print_alternator_table_names(entry.second), false),
                    [this, &ai, node_ip, client_ip, cl, &username, error, &entry] (::audit::audit_info& filtered_info) {
                filtered_info.set_query_string(print_filtered_alternator_batch_query(ai, entry.second));
                audit_sink_set sinks;
                sinks.set(entry.first);
                return log_with_sinks(sinks, filtered_info, node_ip, client_ip, cl, username, error);
            });
        });
    });
}

static future<> maybe_log(const audit_info& audit_info, const service::client_state& client_state, std::optional<db::consistency_level> cl, bool error) {
    if(audit::audit_instance().local_is_initialized()) {
        return audit::local_audit_instance().log(audit_info, client_state, cl, error);
    }
    return make_ready_future<>();
}

static future<> inspect(const audit_info& audit_info, const service::query_state& query_state, const cql3::query_options& options, bool error) {
    return maybe_log(audit_info, query_state.get_client_state(), options.get_consistency(), error);
}

future<> inspect(shared_ptr<cql3::cql_statement> statement, const service::query_state& query_state, const cql3::query_options& options, bool error) {
    const auto audit_info = statement->get_audit_info();
    if (audit_info == nullptr) {
        return make_ready_future<>();
    }
    if (audit_info->batch()) {
        cql3::statements::batch_statement* batch = static_cast<cql3::statements::batch_statement*>(statement.get());
        return do_for_each(batch->statements().begin(), batch->statements().end(), [&query_state, &options, error] (auto&& m) {
            return inspect(m.statement, query_state, options, error);
        });
    } else {
        return inspect(*audit_info, query_state, options, error);
    }
}

future<> inspect(const audit_info_alternator& ai, const service::client_state& client_state, bool error) {
    return maybe_log(static_cast<const audit_info&>(ai), client_state, ai.get_cl(), error);
}

future<> audit::log_login(const sstring& username, socket_address client_ip, bool error) noexcept {
    auto sinks = sinks_for_login(username);
    if (!sinks) {
        return make_ready_future<>();
    }
    socket_address node_ip = _token_metadata.get()->get_topology().my_address().addr();
    if (!_storage_started) {
        on_internal_error_noexcept(logger, fmt::format("Audit login log dropped (storage not ready): node_ip {} client_ip {} username {} error {}",
            node_ip, client_ip, username, error ? "true" : "false"));
        return make_ready_future<>();
    }
    if (logger.is_enabled(logging::log_level::debug)) {
        logger.debug("Login log written: node_ip {}, client_ip {}, username {}, error {}",
            node_ip, client_ip, username, error ? "true" : "false");
    }
    return write_login_to_storage(sinks, username, node_ip, client_ip, error)
        .handle_exception([username, node_ip, client_ip, error] (std::exception_ptr ep) {
            try {
                std::rethrow_exception(ep);
            } catch (const seastar::gate_closed_exception&) {
                // The write raced with stop_storage() closing the gate. No audit
                // generating request should be in flight once storage stops.
                on_internal_error_noexcept(logger, fmt::format("Audit login log dropped (storage stopped): node_ip {} client_ip {} username {} error {}",
                    node_ip, client_ip, username, error ? "true" : "false"));
            } catch (...) {
                logger.error("Unexpected exception when writing login log with: node_ip {} client_ip {} username {} error {} exception {}",
                    node_ip, client_ip, username, error, ep);
            }
    });
}

future<> inspect_login(const sstring& username, socket_address client_ip, bool error) {
    if (!audit::audit_instance().local_is_initialized() || !audit::local_audit_instance().should_log_login(username)) {
        return make_ready_future<>();
    }
    return audit::local_audit_instance().log_login(username, client_ip, error);
}

bool audit::should_log_table(std::string_view keyspace, std::string_view name) const {
    auto keyspace_it = _audited_tables.find(keyspace);
    return keyspace_it != _audited_tables.cend() && keyspace_it->second.find(name) != keyspace_it->second.cend();
}

audit_sink_set audit::sinks_for(const audit_info& audit_info, std::string_view role) const {
    audit_sink_set result;
    const auto category = audit_info.category();
    const auto& keyspace = audit_info.keyspace();
    const auto& table = audit_info.table();
    if (_audited_categories.contains(category)
            && (keyspace.empty()
                || _audited_keyspaces.find(keyspace) != _audited_keyspaces.cend()
                || should_log_table(keyspace, table)
                || category == statement_category::AUTH
                || category == statement_category::ADMIN
                || category == statement_category::DCL)) {
        result.add(_audit_sinks);
    }

    if (!_preprocessed_rules.rules().empty()) {
        result.add(_preprocessed_rules.matching_sinks(category,
                                                       audit_info.keyspace(), audit_info.table(),
                                                        role));
    }
    return result;
}

audit_sink_set audit::sinks_for_table(statement_category category, std::string_view keyspace, std::string_view table, std::string_view role) const {
    audit_sink_set result;
    if (_audited_categories.contains(category)
            && (keyspace.empty()
                || _audited_keyspaces.find(keyspace) != _audited_keyspaces.cend()
                || should_log_table(keyspace, table)
                || category == statement_category::AUTH
                || category == statement_category::ADMIN
                || category == statement_category::DCL)) {
        result.add(_audit_sinks);
    }

    if (!_preprocessed_rules.rules().empty()) {
        result.add(_preprocessed_rules.matching_sinks(category, keyspace, table, role));
    }
    return result;
}

audit_sink_set audit::sinks_for_login(const sstring& username) const {
    audit_sink_set result;
    if (_audited_categories.contains(statement_category::AUTH)) {
        result.add(_audit_sinks);
    }
    if (!_preprocessed_rules.rules().empty()) {
        result.add(_preprocessed_rules.matching_sinks(statement_category::AUTH, "", "", username));
    }
    return result;
}

bool audit::should_log_login(const sstring& username) const {
    return bool(sinks_for_login(username));
}

bool audit::rules_may_log(statement_category cat, std::string_view keyspace, std::string_view table) const {
    if (_preprocessed_rules.rules().empty()) {
        return false;
    }

    for (const auto& rule : _preprocessed_rules.rules()) {
        if (!matches_category(rule, cat)) {
            continue;
        }
        // If keyspace is empty (e.g., global operations with no table), skip
        // table matching and assume the rule may log.
        if (!is_table_scoped_category(cat) || keyspace.empty() || matches_table(rule, keyspace, table)) {
            return true;
        }
    }
    return false;
}

bool audit::will_log(statement_category cat, std::string_view keyspace, std::string_view table) const {
    // If keyspace is empty (e.g., ListTables, or batch operations spanning
    // multiple tables), the operation cannot be filtered by keyspace/table,
    // so it is logged whenever the category matches.
    return (_audited_categories.contains(cat)
           && (keyspace.empty()
                         || _audited_keyspaces.find(keyspace) != _audited_keyspaces.cend()
                         || should_log_table(keyspace, table)
                         || cat == statement_category::AUTH
                         || cat == statement_category::ADMIN
                         || cat == statement_category::DCL))
           || rules_may_log(cat, keyspace, table);
}

template<class T>
void audit::update_config(const sstring & new_value, std::function<T(const sstring&)> parse_func, T& cfg_parameter)
{
    try {
        cfg_parameter = parse_func(new_value);
    } catch (...) {
        logger.error("Audit configuration update failed because cannot parse value=\"{}\".", new_value);
        return;
    }

    // If update_config is called with an invalid new_value, this line is not reached.
    // But logging the invalid value must be avoided later, when a different configuration parameter is changed to a correct value.
    // That's why values from _audited_{categories, keyspaces, tables} are logged instead of _cfg.audit_{categories, keyspaces, tables}

    // Each table as "keyspace.table_name" like in the configuration file
    auto table_entries = _audited_tables | std::views::transform([](const auto& pair) {
        return pair.second | std::views::transform([&](const std::string& table_name) {
            return fmt::format("{}.{}", pair.first, table_name);
        });
    }) | std::views::join;

    logger.info(
        "Audit configuration is updated. Auditing to: \"{}\", with the following categories: \"{}\", keyspaces: \"{}\", and tables: \"{}\".",
        _cfg.audit(),
        fmt::join(std::views::transform(_audited_categories, category_to_string), ","),
        fmt::join(_audited_keyspaces, ","),
        fmt::join(table_entries, ","));
}

void audit::on_role_created(const sstring& role) {
    _preprocessed_rules.add_known_role(role);
    logger.debug("Audit: known role added: {}", role);
}

void audit::on_role_dropped(const sstring& role) {
    _preprocessed_rules.remove_known_role(role);
    logger.debug("Audit: known role removed: {}", role);
}

bool audit::wants_eager_known_tables(size_t table_count) const noexcept {
    return _preprocessed_rules.wants_eager_known_tables(table_count);
}

future<> audit::set_known_entities(std::unordered_set<sstring> roles,
                                    preprocessed_audit_rules::known_table_set tables) {
    logger.info("Audit: loading {} known roles and {} known tables into preprocessed rules cache",
                roles.size(), tables.size());
    co_await _preprocessed_rules.replace_known_entities(std::move(roles), std::move(tables));
}

}
