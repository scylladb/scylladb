/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "seastarx.hh"
#include "utils/log.hh"
#include "utils/observable.hh"
#include "service/client_state.hh"
#include "db/consistency_level_type.hh"
#include "utils/serialized_action.hh"
#include "audit/audit_rule.hh"
#include "audit/preprocessed_audit_rules.hh"
#include <seastar/core/sharded.hh>
#include <seastar/core/gate.hh>
#include <seastar/util/log.hh>

#include "enum_set.hh"

#include <memory>
#include <optional>

namespace db {

class config;

}

namespace cql3 {

class cql_statement;
class query_processor;
class query_options;

}

namespace service {

class migration_manager;
class migration_notifier;
class query_state;

}

namespace locator {

class shared_token_metadata;

}

namespace audit {

extern logging::logger logger;

class audit_exception : public std::exception {
    sstring _what;
public:
    explicit audit_exception(sstring&& what) : _what(std::move(what)) { }
    const char* what() const noexcept override {
        return _what.c_str();
    }
};

// Holds the audit metadata for a single request: the operation category,
// target keyspace/table, and the query string to be logged.
class audit_info {
protected:
    statement_category _category;
    sstring _keyspace;
    sstring _table;
    sstring _query;
    bool _batch; // used only for unpacking batches in CQL, not relevant for Alternator
public:
    audit_info(statement_category cat, sstring keyspace, sstring table, bool batch)
        : _category(cat)
        , _keyspace(std::move(keyspace))
        , _table(std::move(table))
        , _batch(batch)
    { }
    // 'operation' is for the cases where the query string does not contain it, like with Alternator
    audit_info& set_query_string(std::string_view query_string, std::string_view operation = {}) {
        return set_query_string(sstring(query_string), sstring(operation));
    }
    audit_info& set_query_string(const sstring& query_string, const sstring& operation = "") {
        if(!operation.empty()) {
            _query = operation + "|" + query_string;
        } else {
            _query = query_string;
        }
        return *this;
    }
    const sstring& keyspace() const { return _keyspace; }
    const sstring& table() const { return _table; }
    const sstring& query() const { return _query; }
    sstring category_string() const;
    statement_category category() const { return _category; }
    bool batch() const { return _batch; }
};

using audit_info_ptr = std::unique_ptr<audit_info>;

// Audit info for Alternator requests.
// Unlike CQL, where the consistency level is available from query_options and
// passed separately to audit::log(), Alternator has no query_options, so we
// store the CL inside the audit_info object.
// Consistency level is optional: only data read/write operations (GetItem,
// PutItem, Query, Scan, etc.) have a meaningful CL. Schema operations and
// metadata queries pass std::nullopt.
class audit_info_alternator final : public audit_info {
    std::optional<db::consistency_level> _cl;
public:
    audit_info_alternator(statement_category cat, sstring keyspace, sstring table, std::optional<db::consistency_level> cl = std::nullopt)
        : audit_info(cat, std::move(keyspace), std::move(table), false), _cl(cl)
    {}

    std::optional<db::consistency_level> get_cl() const { return _cl; }
};

class storage_helper;
class audit_schema_listener;

class audit final : public seastar::async_sharded_service<audit> {
public:
    // Transparent comparator (std::less<>) enables heterogeneous lookup with
    // string_view keys.
    using audited_keyspaces_t = std::set<sstring, std::less<>>;
    using audited_tables_t = std::map<sstring, std::set<sstring, std::less<>>, std::less<>>;
private:
    locator::shared_token_metadata& _token_metadata;
    audited_keyspaces_t _audited_keyspaces;
    audited_tables_t _audited_tables;
    category_set _audited_categories;

    audit_sink_set _audit_sinks;
    preprocessed_audit_rules _preprocessed_rules;

    std::unique_ptr<storage_helper> _storage_helper_ptr;
    bool _storage_running = false;
    seastar::named_gate _pending_writes;
    std::unique_ptr<audit_schema_listener> _schema_listener;
    service::migration_notifier& _migration_notifier;

    const db::config& _cfg;
    utils::observer<sstring> _cfg_keyspaces_observer;
    utils::observer<sstring> _cfg_tables_observer;
    utils::observer<sstring> _cfg_categories_observer;
    serialized_action _rules_rebuild_action;
    std::optional<utils::observer<std::vector<audit_rule>>> _cfg_rules_observer;

    template<class T>
    void update_config(const sstring & new_value, std::function<T(const sstring&)> parse_func, T& cfg_parameter);

    bool should_log_table(std::string_view keyspace, std::string_view name) const;
    bool rules_may_log(statement_category cat, std::string_view keyspace, std::string_view table) const;
    audit_sink_set sinks_for(const audit_info& audit_info, std::string_view role) const;
    audit_sink_set sinks_for_login(const sstring& username) const;
    future<> write_to_storage(audit_sink_set sinks,
                              const audit_info& audit_info,
                              socket_address node_ip,
                              socket_address client_ip,
                              std::optional<db::consistency_level> cl,
                              const sstring& username,
                              bool error);
    future<> write_login_to_storage(audit_sink_set sinks,
                                    const sstring& username,
                                    socket_address node_ip,
                                    socket_address client_ip,
                                    bool error);
    future<> rebuild_rules();
public:
    static seastar::sharded<audit>& audit_instance() {
        // FIXME: leaked intentionally to avoid shutdown problems, see #293
        static seastar::sharded<audit>* audit_inst = new seastar::sharded<audit>();

        return *audit_inst;
    }

    static audit& local_audit_instance() {
        return audit_instance().local();
    }
    static future<> start_audit(const db::config& cfg, sharded<locator::shared_token_metadata>& stm, sharded<cql3::query_processor>& qp, sharded<service::migration_manager>& mm);
    static future<> start_storage(const db::config& cfg);
    static future<> stop_storage();
    static future<> stop_audit();
    static audit_info_ptr create_audit_info(statement_category cat, const sstring& keyspace, const sstring& table, bool batch = false);
    audit(locator::shared_token_metadata& stm,
          cql3::query_processor& qp,
          service::migration_manager& mm,
          audit_sink_set audit_sinks,
          audited_keyspaces_t&& audited_keyspaces,
          audited_tables_t&& audited_tables,
          category_set&& audited_categories,
          std::vector<audit_rule>&& audit_rules,
          const db::config& cfg);
    ~audit();
    future<> shutdown();
    bool will_log(statement_category cat, std::string_view keyspace = {}, std::string_view table = {}) const;
    bool should_log_login(const sstring& username) const;
    future<> log(const audit_info& audit_info, const service::client_state& client_state, std::optional<db::consistency_level> cl, bool error);
    future<> log_login(const sstring& username, socket_address client_ip, bool error) noexcept;

    void on_role_created(const sstring& role);
    void on_role_dropped(const sstring& role);

    future<> set_known_entities(std::unordered_set<sstring> roles,
                                preprocessed_audit_rules::known_table_set tables);
};

future<> inspect(const audit_info_alternator& audit_info, const service::client_state& client_state, bool error);
future<> inspect(shared_ptr<cql3::cql_statement> statement, const service::query_state& query_state, const cql3::query_options& options, bool error);

future<> inspect_login(const sstring& username, socket_address client_ip, bool error);

}
