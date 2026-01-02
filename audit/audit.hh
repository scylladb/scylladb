/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "seastarx.hh"
#include "utils/log.hh"
#include "utils/observable.hh"
#include "service/client_state.hh"
#include <seastar/core/sharded.hh>
#include <seastar/util/log.hh>

#include "enum_set.hh"

#include <memory>

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

enum class statement_category {
    QUERY, DML, DDL, DCL, AUTH, ADMIN
};

using category_set = enum_set<super_enum<statement_category, statement_category::QUERY,
                                                             statement_category::DML,
                                                             statement_category::DDL,
                                                             statement_category::DCL,
                                                             statement_category::AUTH,
                                                             statement_category::ADMIN>>;

// Audit info part that is relevant to the context of the request
class audit_info {
protected:
    statement_category _category;
    sstring _keyspace;
    sstring _table;
    sstring _operation; // for Alternator, where operation is not present in request string (query)
    sstring _query;
public:
    audit_info(statement_category cat, sstring keyspace, sstring table)
        : _category(cat)
        , _keyspace(std::move(keyspace))
        , _table(std::move(table))
    { }
    // 'operation' is for the cases where the query string does not contain it, like with Alternator
    audit_info& set_query_string(std::string_view query_string, const sstring& operation = "") {
        return set_query_string(sstring(query_string), operation);
    }
    audit_info& set_query_string(const sstring& query_string, const sstring& operation = "") {
        _query = query_string;
        _operation = operation;
        return *this;
    }
    const sstring& keyspace() const { return _keyspace; }
    const sstring& table() const { return _table; }
    const sstring& query() const { return _query; }
    const sstring& operation() const { return _operation; }
    sstring category_string() const;
    statement_category category() const { return _category; }
};

using audit_info_ptr = std::unique_ptr<audit_info>;

// Audit info with consistency level, used in Alternator
class audit_info_cl final : public audit_info {
    db::consistency_level _cl;
public:
    audit_info_cl(statement_category cat, const sstring& keyspace, const sstring& table, db::consistency_level cl)
        : audit_info(cat, std::move(keyspace), std::move(table))
        , _cl(cl)
    { }

    db::consistency_level get_cl() const { return _cl; }
};

class storage_helper;

class audit final : public seastar::async_sharded_service<audit> {
    locator::shared_token_metadata& _token_metadata;
    std::set<sstring> _audited_keyspaces;
    // Maps keyspace name to set of table names in that keyspace
    std::map<sstring, std::set<sstring>> _audited_tables;
    category_set _audited_categories;

    std::unique_ptr<storage_helper> _storage_helper_ptr;

    const db::config& _cfg;
    utils::observer<sstring> _cfg_keyspaces_observer;
    utils::observer<sstring> _cfg_tables_observer;
    utils::observer<sstring> _cfg_categories_observer;

    template<class T>
    void update_config(const sstring & new_value, std::function<T(const sstring&)> parse_func, T& cfg_parameter);

    bool should_log_table(const sstring& keyspace, const sstring& name) const;
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
    static future<> stop_audit();
    static audit_info_ptr create_audit_info(statement_category cat, const sstring& keyspace, const sstring& table);
    static audit_info_ptr create_no_audit_info();
    audit(locator::shared_token_metadata& stm,
          cql3::query_processor& qp,
          service::migration_manager& mm,
          std::set<sstring>&& audit_modes,
          std::set<sstring>&& audited_keyspaces,
          std::map<sstring, std::set<sstring>>&& audited_tables,
          category_set&& audited_categories,
          const db::config& cfg);
    ~audit();
    future<> start(const db::config& cfg);
    future<> stop();
    future<> shutdown();
    bool should_log(const audit_info& audit_info) const;
    bool should_log_login() const { return _audited_categories.contains(statement_category::AUTH); }
    future<> log(const audit_info& audit_info, const service::client_state& client_state, db::consistency_level cl, bool error);
    future<> log_login(const sstring& username, socket_address client_ip, bool error) noexcept;
};

future<> inspect(const audit_info_cl& audit_info, const service::client_state& client_state, bool error);
future<> inspect(shared_ptr<cql3::cql_statement> statement, const service::query_state& query_state, const cql3::query_options& options, bool error);

future<> inspect_login(const sstring& username, socket_address client_ip, bool error);

}
