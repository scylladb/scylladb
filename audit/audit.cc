/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/future-util.hh>
#include "audit/audit.hh"
#include "db/config.hh"
#include "cql3/cql_statement.hh"
#include "cql3/statements/batch_statement.hh"
#include "cql3/statements/modification_statement.hh"
#include "storage_helper.hh"
#include "audit.hh"
#include "../db/config.hh"
#include "utils/class_registrator.hh"

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/algorithm/string/classification.hpp>


namespace audit {

logging::logger logger("audit");

sstring audit_info::category_string() const {
    switch (_category) {
        case statement_category::QUERY: return "QUERY";
        case statement_category::DML: return "DML";
        case statement_category::DDL: return "DDL";
        case statement_category::DCL: return "DCL";
        case statement_category::AUTH: return "AUTH";
        case statement_category::ADMIN: return "ADMIN";
    }
    return "";
}

audit::audit(locator::shared_token_metadata& token_metadata,
             sstring&& storage_helper_name,
             std::set<sstring>&& audited_keyspaces,
             std::map<sstring, std::set<sstring>>&& audited_tables,
             category_set&& audited_categories)
    : _token_metadata(token_metadata)
    , _audited_keyspaces(std::move(audited_keyspaces))
    , _audited_tables(std::move(audited_tables))
    , _audited_categories(std::move(audited_categories))
    , _storage_helper_class_name(std::move(storage_helper_name))
{ }

audit::~audit() = default;

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

static std::map<sstring, std::set<sstring>> parse_audit_tables(const sstring& data) {
    std::map<sstring, std::set<sstring>> result;
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
            result[parts[0]].insert(std::move(parts[1]));
        }
    }
    return result;
}

static std::set<sstring> parse_audit_keyspaces(const sstring& data) {
    std::set<sstring> result;
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

future<> audit::create_audit(const db::config& cfg, sharded<locator::shared_token_metadata>& stm) {
    sstring storage_helper_name;
    if (cfg.audit() == "table") {
        storage_helper_name = "audit_cf_storage_helper";
    } else if (cfg.audit() == "syslog") {
        storage_helper_name = "audit_syslog_storage_helper";
    } else if (cfg.audit() == "none") {
        // Audit is off
        logger.info("Audit is disabled");

        return make_ready_future<>();
    } else {
        throw audit_exception(fmt::format("Bad configuration: invalid 'audit': {}", cfg.audit()));
    }
    category_set audited_categories = parse_audit_categories(cfg.audit_categories());
    if (!audited_categories) {
        return make_ready_future<>();
    }
    std::map<sstring, std::set<sstring>> audited_tables = parse_audit_tables(cfg.audit_tables());
    std::set<sstring> audited_keyspaces = parse_audit_keyspaces(cfg.audit_keyspaces());
    if (audited_tables.empty()
        && audited_keyspaces.empty()
        && !audited_categories.contains(statement_category::AUTH)
        && !audited_categories.contains(statement_category::ADMIN)
        && !audited_categories.contains(statement_category::DCL)) {
        return make_ready_future<>();
    }
    logger.info("Audit is enabled. Auditing to: \"{}\", with the following categories: \"{}\", keyspaces: \"{}\", and tables: \"{}\"",
                cfg.audit(), cfg.audit_categories(), cfg.audit_keyspaces(), cfg.audit_tables());

    return audit_instance().start(std::ref(stm),
                                  std::move(storage_helper_name),
                                  std::move(audited_keyspaces),
                                  std::move(audited_tables),
                                  std::move(audited_categories));
}

future<> audit::start_audit(const db::config& cfg, sharded<cql3::query_processor>& qp, sharded<service::migration_manager>& mm) {
    if (!audit_instance().local_is_initialized()) {
        return make_ready_future<>();
    }
    return audit_instance().invoke_on_all([&cfg, &qp, &mm] (audit& local_audit) {
        return local_audit.start(cfg, qp.local(), mm.local());
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

audit_info_ptr audit::create_audit_info(statement_category cat, const sstring& keyspace, const sstring& table) {
    if (!audit_instance().local_is_initialized()) {
        return nullptr;
    }
    return std::make_unique<audit_info>(cat, keyspace, table);
}

audit_info_ptr audit::create_no_audit_info() {
    return audit_info_ptr();
}

future<> audit::start(const db::config& cfg, cql3::query_processor& qp, service::migration_manager& mm) {
    try {
        _storage_helper_ptr = create_object<storage_helper>(_storage_helper_class_name, qp, mm);
    } catch (no_such_class& e) {
        logger.error("Can't create audit storage helper {}: not supported", _storage_helper_class_name);
        throw;
    } catch (...) {
        throw;
    }
    return _storage_helper_ptr->start(cfg);
}

future<> audit::stop() {
    return _storage_helper_ptr->stop();
}

future<> audit::shutdown() {
    return make_ready_future<>();
}

future<> audit::log(const audit_info* audit_info, service::query_state& query_state, const cql3::query_options& options, bool error) {
    const service::client_state& client_state = query_state.get_client_state();
    socket_address node_ip = _token_metadata.get()->get_topology().my_address().addr();
    db::consistency_level cl = options.get_consistency();
    thread_local static sstring no_username("undefined");
    static const sstring anonymous_username("anonymous");
    const sstring& username = client_state.user() ? client_state.user()->name.value_or(anonymous_username) : no_username;
    socket_address client_ip = client_state.get_client_address().addr();
    return futurize_invoke(std::mem_fn(&storage_helper::write), _storage_helper_ptr, audit_info, node_ip, client_ip, cl, username, error)
        .handle_exception([audit_info, node_ip, client_ip, cl, username, error] (auto ep) {
            logger.error("Unexpected exception when writing log with: node_ip {} category {} cl {} error {} keyspace {} query '{}' client_ip {} table {} username {} exception {}",
                node_ip, audit_info->category_string(), cl, error, audit_info->keyspace(),
                audit_info->query(), client_ip, audit_info->table(),username, ep);
    });
}

future<> audit::log_login(const sstring& username, socket_address client_ip, bool error) noexcept {
    socket_address node_ip = _token_metadata.get()->get_topology().my_address().addr();
    return futurize_invoke(std::mem_fn(&storage_helper::write_login), _storage_helper_ptr, username, node_ip, client_ip, error)
        .handle_exception([username, node_ip, client_ip, error] (auto ep) {
            logger.error("Unexpected exception when writing login log with: node_ip {} client_ip {} username {} error {} exception {}",
                node_ip, client_ip, username, error, ep);
    });
}

future<> inspect(shared_ptr<cql3::cql_statement> statement, service::query_state& query_state, const cql3::query_options& options, bool error) {
    cql3::statements::batch_statement* batch = dynamic_cast<cql3::statements::batch_statement*>(statement.get());
    if (batch != nullptr) {
        return do_for_each(batch->statements().begin(), batch->statements().end(), [&query_state, &options, error] (auto&& m) {
            return inspect(m.statement, query_state, options, error);
        });
    } else {
        auto audit_info = statement->get_audit_info();
        if (bool(audit_info) && audit::local_audit_instance().should_log(audit_info)) {
            return audit::local_audit_instance().log(audit_info, query_state, options, error);
        }
    }
    return make_ready_future<>();
}

future<> inspect_login(const sstring& username, socket_address client_ip, bool error) {
    if (!audit::audit_instance().local_is_initialized() || !audit::local_audit_instance().should_log_login()) {
        return make_ready_future<>();
    }
    return audit::local_audit_instance().log_login(username, client_ip, error);
}

bool audit::should_log_table(const sstring& keyspace, const sstring& name) const {
    auto keyspace_it = _audited_tables.find(keyspace);
    return keyspace_it != _audited_tables.cend() && keyspace_it->second.find(name) != keyspace_it->second.cend();
}

bool audit::should_log(const audit_info* audit_info) const {
    return _audited_categories.contains(audit_info->category())
           && (_audited_keyspaces.find(audit_info->keyspace()) != _audited_keyspaces.cend()
                         || should_log_table(audit_info->keyspace(), audit_info->table())
                         || audit_info->category() == statement_category::AUTH
                         || audit_info->category() == statement_category::ADMIN
                         || audit_info->category() == statement_category::DCL);
}

}
