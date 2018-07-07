/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Modified by ScyllaDB
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "cql3/cql_statement.hh"
#include "modification_statement.hh"
#include "raw/modification_statement.hh"
#include "raw/batch_statement.hh"
#include "service/storage_proxy.hh"
#include "transport/messages/result_message.hh"
#include "timestamp.hh"
#include "log.hh"
#include "to_string.hh"
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/uniqued.hpp>
#include <boost/iterator/counting_iterator.hpp>

#pragma once

namespace cql3 {

namespace statements {

/**
 * A <code>BATCH</code> statement parsed from a CQL query.
 *
 */
class batch_statement : public cql_statement_no_metadata {
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
    bool _has_conditions;
    cql_stats& _stats;
public:
    /**
     * Creates a new BatchStatement from a list of statements and a
     * Thrift consistency level.
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

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override;

    virtual bool depends_on_keyspace(const sstring& ks_name) const override;

    virtual bool depends_on_column_family(const sstring& cf_name) const override;

    virtual uint32_t get_bound_terms() override;

    virtual future<> check_access(const service::client_state& state) override;

    // Validates a prepared batch statement without validating its nested statements.
    void validate();

    // The batch itself will be validated in either Parsed#prepare() - for regular CQL3 batches,
    //   or in QueryProcessor.processBatch() - for native protocol batches.
    virtual void validate(service::storage_proxy& proxy, const service::client_state& state) override;

    const std::vector<single_statement>& get_statements();
private:
    future<std::vector<mutation>> get_mutations(service::storage_proxy& storage, const query_options& options, db::timeout_clock::time_point timeout, bool local, api::timestamp_type now, tracing::trace_state_ptr trace_state);

public:
    /**
     * Checks batch size to ensure threshold is met. If not, a warning is logged.
     * @param cfs ColumnFamilies that will store the batch's mutations.
     */
    static void verify_batch_size(const std::vector<mutation>& mutations);

    virtual future<shared_ptr<cql_transport::messages::result_message>> execute(
            service::storage_proxy& storage, service::query_state& state, const query_options& options) override;
private:
    friend class batch_statement_executor;
    future<shared_ptr<cql_transport::messages::result_message>> do_execute(
            service::storage_proxy& storage,
            service::query_state& query_state, const query_options& options,
            bool local, api::timestamp_type now);

    future<> execute_without_conditions(
            service::storage_proxy& storage,
            std::vector<mutation> mutations,
            db::consistency_level cl,
            db::timeout_clock::time_point timeout,
            tracing::trace_state_ptr tr_state);

    future<shared_ptr<cql_transport::messages::result_message>> execute_with_conditions(
            service::storage_proxy& storage,
            const query_options& options,
            service::query_state& state);
public:
    // FIXME: no cql_statement::to_string() yet
#if 0
    sstring to_string() const {
        return sprint("BatchStatement(type=%s, statements=%s)", _type, join(", ", _statements));
    }
#endif
};

}
}
