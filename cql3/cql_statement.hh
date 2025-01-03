/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "timeout_config.hh"
#include "service/raft/raft_group0_client.hh"
#include "audit/audit.hh"

namespace service {

class storage_proxy;
class query_state;
class client_state;

}

namespace cql_transport {

namespace messages {

class result_message;

}

}

namespace cql3 {

class query_processor;

class metadata;
seastar::shared_ptr<const metadata> make_empty_metadata();

class query_options;

// A vector of CQL warnings generated during execution of a statement.
using cql_warnings_vec = std::vector<sstring>;

class cql_statement {
    timeout_config_selector _timeout_config_selector;
    audit::audit_info_ptr _audit_info;
public:
    // CQL statement text
    seastar::sstring raw_cql_statement;

    // Returns true for statements that needs guard to be taken before the execution
    virtual bool needs_guard(query_processor& qp, service::query_state& state) const {
        return false;
    }

    explicit cql_statement(timeout_config_selector timeout_selector) : _timeout_config_selector(timeout_selector) {}
    cql_statement(cql_statement&& o) = default;
    cql_statement(const cql_statement& o) : _timeout_config_selector(o._timeout_config_selector), _audit_info(o._audit_info ? std::make_unique<audit::audit_info>(*o._audit_info) : nullptr) { }
    virtual ~cql_statement()
    { }

    timeout_config_selector get_timeout_config_selector() const { return _timeout_config_selector; }

    virtual uint32_t get_bound_terms() const = 0;

    /**
     * Perform any access verification necessary for the statement.
     *
     * @param state the current client state
     */
    virtual seastar::future<> check_access(query_processor& qp, const service::client_state& state) const = 0;

    /**
     * Perform additional validation required by the statement.
     * To be overridden by subclasses if needed.
     *
     * @param state the current client state
     */
    virtual void validate(query_processor& qp, const service::client_state& state) const {}

    /**
     * Execute the statement and return the resulting result or null if there is no result.
     *
     * In case of a failure, it must return an exceptional future. It must not use
     * the result_message::exception to indicate failure.
     *
     * @param state the current query state
     * @param options options for this query (consistency, variables, pageSize, ...)
     */
    virtual seastar::future<seastar::shared_ptr<cql_transport::messages::result_message>>
        execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const = 0;

    /**
     * Execute the statement and return the resulting result or null if there is no result.
     *
     * Unlike execute(), it is allowed to return a result_message::exception which contains
     * an exception that needs to be explicitly handled.
     *
     * @param state the current query state
     * @param options options for this query (consistency, variables, pageSize, ...)
     */
    virtual seastar::future<seastar::shared_ptr<cql_transport::messages::result_message>>
            execute_without_checking_exception_message(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const {
        return execute(qp, state, options, std::move(guard));
    }

    virtual bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const = 0;

    virtual seastar::shared_ptr<const metadata> get_result_metadata() const = 0;

    virtual bool is_conditional() const {
        return false;
    }

    audit::audit_info* get_audit_info() { return _audit_info.get(); }
    void set_audit_info(audit::audit_info_ptr&& info) { _audit_info = std::move(info); }

    virtual void sanitize_audit_info() {}
};

class cql_statement_no_metadata : public cql_statement {
public:
    using cql_statement::cql_statement;
    virtual seastar::shared_ptr<const metadata> get_result_metadata() const override {
        return make_empty_metadata();
    }
};

// Conditional modification statements and batches
// return a result set and have metadata, while same
// statements without conditions do not.
class cql_statement_opt_metadata : public cql_statement {
protected:
    // Result set metadata, may be empty for simple updates and batches
    seastar::shared_ptr<metadata> _metadata;
public:
    using cql_statement::cql_statement;
    virtual seastar::shared_ptr<const metadata> get_result_metadata() const override {
        if (_metadata) {
            return _metadata;
        }
        return make_empty_metadata();
    }
};

}
