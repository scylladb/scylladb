/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include <optional>
#include <string_view>

#include "timeout_config.hh"
#include "service/pager/query_plan.hh"
#include "service/raft/raft_group0_client.hh"
#include "audit/audit.hh"
#include "utils/chunked_string.hh"

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
protected:
    // Result set metadata, unset for statements which return no result
    // set. E.g. conditional modification statements and batches return a
    // result set and have metadata, while the same statements without
    // conditions do not.
    seastar::shared_ptr<metadata> _metadata;
public:
    // CQL statement text
    utils::chunked_string raw_cql_statement;

    // Returns true for statements that needs guard to be taken before the execution
    virtual bool needs_guard(query_processor& qp, service::query_state& state) const {
        return false;
    }

    explicit cql_statement(timeout_config_selector timeout_selector) : _timeout_config_selector(timeout_selector) {}
    cql_statement(cql_statement&& o) = default;
    cql_statement(const cql_statement& o) : _timeout_config_selector(o._timeout_config_selector), _audit_info(o._audit_info ? std::make_unique<audit::audit_info>(*o._audit_info) : nullptr), _metadata(o._metadata) { }
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

    // What re-deriving this statement with the same plan pinned needs.
    // Disengaged for statements that are never paged. See #18992.
    struct paging_plan {
        service::pager::query_plan plan;
        // Borrowed from the statement, so only valid while it lives.
        std::string_view keyspace;
    };
    virtual std::optional<paging_plan> query_plan_for_paging() const {
        return std::nullopt;
    }

    // Statements which keep their result set metadata elsewhere, e.g. in a
    // selection, override this instead of setting _metadata.
    virtual seastar::shared_ptr<const metadata> get_result_metadata() const {
        if (_metadata) {
            return _metadata;
        }
        return make_empty_metadata();
    }

    virtual bool is_conditional() const {
        return false;
    }

    // A driver's control connection runs in a dedicated scheduling group and is
    // only meant to issue the system queries the driver needs to manage itself,
    // never user load. Returns true when executing this statement means the
    // connection is being misused for user load, so the CQL server can reclassify
    // it as a regular user connection and stop using the driver scheduling group.
    //
    // There is deliberately no default implementation: every statement type must
    // classify itself, so a newly introduced statement cannot silently fall
    // through with the wrong classification. Intermediate classes may implement it
    // once on behalf of all their sub-classes.
    virtual bool should_reclassify_control_connection() const = 0;

    audit::audit_info* get_audit_info() { return _audit_info.get(); }
    void set_audit_info(audit::audit_info_ptr&& info) { _audit_info = std::move(info); }

    virtual void sanitize_audit_info() {}
};

}
