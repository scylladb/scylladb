/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */
#pragma once

#include "cql3/cql_statement.hh"
#include "raw/batch_statement.hh"
#include "timestamp.hh"
#include "log.hh"
#include "service_permit.hh"
#include "exceptions/coordinator_result.hh"

namespace cql_transport::messages {
    class result_message;
}

namespace cql3 {

class query_processor;

namespace statements {

class modification_statement;

/**
 * A <code>BATCH</code> statement parsed from a CQL query.
 *
 */
class batch_statement : public cql_statement_opt_metadata {
    static logging::logger _logger;
public:
    using type = raw::batch_statement::type;

    struct single_statement {
        shared_ptr<modification_statement> statement;
        bool needs_authorization = true;

    public:
        single_statement(shared_ptr<modification_statement> s)
            : statement(std::move(s))
        {}
        single_statement(shared_ptr<modification_statement> s, bool na)
            : statement(std::move(s))
            , needs_authorization(na)
        {}
    };
private:
    int _bound_terms;
    type _type;
    std::vector<single_statement> _statements;
    std::unique_ptr<attributes> _attrs;
    // True if *any* statement of the batch has IF .. clause. In
    // this case entire batch is considered a CAS batch.
    bool _has_conditions;
    // If the BATCH has conditions, it must return columns which
    // are involved in condition expressions in its result set.
    // Unlike Cassandra, Scylla always returns all columns,
    // regardless of whether the batch succeeds or not - this
    // allows clients to prepare a CAS statement like any other
    // statement, and trust the returned statement metadata.
    // Cassandra returns a result set only if CAS succeeds. If
    // any statement in the batch has IF EXISTS, we must return
    // all columns of the table, including the primary key.
    column_set _columns_of_cas_result_set;
    cql_stats& _stats;
public:
    /**
     * Creates a new BatchStatement from a list of statements
     *
     * @param type type of the batch
     * @param statements a list of UpdateStatements
     * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
     */
    batch_statement(int bound_terms, type type_,
                    std::vector<single_statement> statements,
                    std::unique_ptr<attributes> attrs,
                    cql_stats& stats);

    batch_statement(type type_,
                    std::vector<single_statement> statements,
                    std::unique_ptr<attributes> attrs,
                    cql_stats& stats);

    virtual bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    virtual uint32_t get_bound_terms() const override;

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    // Validates a prepared batch statement without validating its nested statements.
    void validate();

    bool has_conditions() const { return _has_conditions; }

    void build_cas_result_set_metadata();

    // The batch itself will be validated in either Parsed#prepare() - for regular CQL3 batches,
    //   or in QueryProcessor.processBatch() - for native protocol batches.
    virtual void validate(query_processor& qp, const service::client_state& state) const override;

    const std::vector<single_statement>& get_statements();
private:
    future<std::vector<mutation>> get_mutations(query_processor& qp, const query_options& options, db::timeout_clock::time_point timeout,
            bool local, api::timestamp_type now, service::query_state& query_state) const;

public:
    /**
     * Checks batch size to ensure threshold is met. If not, a warning is logged.
     * @param cfs ColumnFamilies that will store the batch's mutations.
     */
    static void verify_batch_size(query_processor& qp, const std::vector<mutation>& mutations);

    virtual future<shared_ptr<cql_transport::messages::result_message>> execute(
            query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const override;

    virtual future<shared_ptr<cql_transport::messages::result_message>> execute_without_checking_exception_message(
            query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const override;
private:
    friend class batch_statement_executor;
    future<shared_ptr<cql_transport::messages::result_message>> do_execute(
            query_processor& qp,
            service::query_state& query_state, const query_options& options,
            bool local, api::timestamp_type now) const;

    future<exceptions::coordinator_result<>> execute_without_conditions(
            query_processor& qp,
            std::vector<mutation> mutations,
            db::consistency_level cl,
            db::timeout_clock::time_point timeout,
            tracing::trace_state_ptr tr_state,
            service_permit permit) const;

    future<shared_ptr<cql_transport::messages::result_message>> execute_with_conditions(
            query_processor& qp,
            const query_options& options,
            service::query_state& state) const;

    db::timeout_clock::duration get_timeout(const service::client_state& state, const query_options& options) const;
public:
    // FIXME: no cql_statement::to_string() yet
#if 0
    sstring to_string() const {
        return format("BatchStatement(type={}, statements={})", _type, join(", ", _statements));
    }
#endif
};

}
}
