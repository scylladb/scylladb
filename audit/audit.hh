/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "seastarx.hh"
#include "utils/log.hh"
#include "db/consistency_level.hh"
#include "locator/token_metadata_fwd.hh"
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

class audit_info final {
    statement_category _category;
    sstring _keyspace;
    sstring _table;
    sstring _query;
public:
    audit_info(statement_category cat, sstring keyspace, sstring table)
        : _category(cat)
        , _keyspace(std::move(keyspace))
        , _table(std::move(table))
    { }
    void set_query_string(const std::string_view& query_string) {
        _query = sstring(query_string);
    }
    const sstring& keyspace() const { return _keyspace; }
    const sstring& table() const { return _table; }
    const sstring& query() const { return _query; }
    sstring category_string() const;
    statement_category category() const { return _category; }
};

using audit_info_ptr = std::unique_ptr<audit_info>;

class storage_helper;

class audit final : public seastar::async_sharded_service<audit> {
    locator::shared_token_metadata& _token_metadata;
    const std::set<sstring> _audited_keyspaces;
    // Maps keyspace name to set of table names in that keyspace
    const std::map<sstring, std::set<sstring>> _audited_tables;
    const category_set _audited_categories;
    sstring _storage_helper_class_name;
    std::unique_ptr<storage_helper> _storage_helper_ptr;

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
    static future<> create_audit(const db::config& cfg, sharded<locator::shared_token_metadata>& stm);
    static future<> start_audit(const db::config& cfg, sharded<cql3::query_processor>& qp, sharded<service::migration_manager>& mm);
    static future<> stop_audit();
    static audit_info_ptr create_audit_info(statement_category cat, const sstring& keyspace, const sstring& table);
    static audit_info_ptr create_no_audit_info();
    audit(locator::shared_token_metadata& stm, sstring&& storage_helper_name,
          std::set<sstring>&& audited_keyspaces,
          std::map<sstring, std::set<sstring>>&& audited_tables,
          category_set&& audited_categories);
    ~audit();
    future<> start(const db::config& cfg, cql3::query_processor& qp, service::migration_manager& mm);
    future<> stop();
    future<> shutdown();
    bool should_log(const audit_info* audit_info) const;
    bool should_log_login() const { return _audited_categories.contains(statement_category::AUTH); }
    future<> log(const audit_info* audit_info, service::query_state& query_state, const cql3::query_options& options, bool error);
    future<> log_login(const sstring& username, socket_address client_ip, bool error) noexcept;
};

future<> inspect(shared_ptr<cql3::cql_statement> statement, service::query_state& query_state, const cql3::query_options& options, bool error);

future<> inspect_login(const sstring& username, socket_address client_ip, bool error);

}
