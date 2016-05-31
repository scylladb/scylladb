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
private:
    int _bound_terms;
public:
    type _type;
private:
    std::vector<shared_ptr<modification_statement>> _statements;
    std::unique_ptr<attributes> _attrs;
    bool _has_conditions;
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
                    std::vector<shared_ptr<modification_statement>> statements,
                    std::unique_ptr<attributes> attrs)
            : _bound_terms(bound_terms), _type(type_), _statements(std::move(statements))
            , _attrs(std::move(attrs))
            , _has_conditions(boost::algorithm::any_of(_statements, std::mem_fn(&modification_statement::has_conditions))) {
    }

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        return _attrs->uses_function(ks_name, function_name)
                || boost::algorithm::any_of(_statements, [&] (auto&& s) { return s->uses_function(ks_name, function_name); });
    }

    virtual bool depends_on_keyspace(const sstring& ks_name) const override;

    virtual bool depends_on_column_family(const sstring& cf_name) const override;

    virtual uint32_t get_bound_terms() override {
        return _bound_terms;
    }

    virtual future<> check_access(const service::client_state& state) override {
        return parallel_for_each(_statements.begin(), _statements.end(), [&state](auto&& s) {
           return s->check_access(state);
        });
    }

    // Validates a prepared batch statement without validating its nested statements.
    void validate() {
        if (_attrs->is_time_to_live_set()) {
            throw exceptions::invalid_request_exception("Global TTL on the BATCH statement is not supported.");
        }

        bool timestamp_set = _attrs->is_timestamp_set();
        if (timestamp_set) {
            if (_has_conditions) {
                throw exceptions::invalid_request_exception("Cannot provide custom timestamp for conditional BATCH");
            }
            if (_type == type::COUNTER) {
                throw exceptions::invalid_request_exception("Cannot provide custom timestamp for counter BATCH");
            }
        }

        bool has_counters = boost::algorithm::any_of(_statements, std::mem_fn(&modification_statement::is_counter));
        bool has_non_counters = !boost::algorithm::all_of(_statements, std::mem_fn(&modification_statement::is_counter));
        if (timestamp_set && has_counters) {
            throw exceptions::invalid_request_exception("Cannot provide custom timestamp for a BATCH containing counters");
        }
        if (timestamp_set && boost::algorithm::any_of(_statements, std::mem_fn(&modification_statement::is_timestamp_set))) {
            throw exceptions::invalid_request_exception("Timestamp must be set either on BATCH or individual statements");
        }
        if (_type == type::COUNTER && has_non_counters) {
            throw exceptions::invalid_request_exception("Cannot include non-counter statement in a counter batch");
        }
        if (_type == type::LOGGED && has_counters) {
            throw exceptions::invalid_request_exception("Cannot include a counter statement in a logged batch");
        }
        if (has_counters && has_non_counters) {
            throw exceptions::invalid_request_exception("Counter and non-counter mutations cannot exist in the same batch");
        }

        if (_has_conditions
                && !_statements.empty()
                && (boost::distance(_statements
                                | boost::adaptors::transformed(std::mem_fn(&modification_statement::keyspace))
                                | boost::adaptors::uniqued) != 1
                    || (boost::distance(_statements
                            | boost::adaptors::transformed(std::mem_fn(&modification_statement::column_family))
                            | boost::adaptors::uniqued) != 1))) {
            throw exceptions::invalid_request_exception("Batch with conditions cannot span multiple tables");
        }
    }

    // The batch itself will be validated in either Parsed#prepare() - for regular CQL3 batches,
    //   or in QueryProcessor.processBatch() - for native protocol batches.
    virtual void validate(distributed<service::storage_proxy>& proxy, const service::client_state& state) override {
        for (auto&& s : _statements) {
            s->validate(proxy, state);
        }
    }

    const std::vector<shared_ptr<modification_statement>>& get_statements() {
        return _statements;
    }
private:
    future<std::vector<mutation>> get_mutations(distributed<service::storage_proxy>& storage, const query_options& options, bool local, api::timestamp_type now) {
        // Do not process in parallel because operations like list append/prepend depend on execution order.
        return do_with(std::vector<mutation>(), [this, &storage, &options, now, local] (auto&& result) {
            return do_for_each(boost::make_counting_iterator<size_t>(0),
                               boost::make_counting_iterator<size_t>(_statements.size()),
                               [this, &storage, &options, now, local, &result] (size_t i) {
                auto&& statement = _statements[i];
                auto&& statement_options = options.for_statement(i);
                auto timestamp = _attrs->get_timestamp(now, statement_options);
                return statement->get_mutations(storage, statement_options, local, timestamp).then([&result] (auto&& more) {
                    std::move(more.begin(), more.end(), std::back_inserter(result));
                });
            }).then([&result] {
                return std::move(result);
            });
        });
    }

public:
    /**
     * Checks batch size to ensure threshold is met. If not, a warning is logged.
     * @param cfs ColumnFamilies that will store the batch's mutations.
     */
    static void verify_batch_size(const std::vector<mutation>& mutations);

    virtual future<shared_ptr<transport::messages::result_message>> execute(
            distributed<service::storage_proxy>& storage, service::query_state& state, const query_options& options) override {
        return execute(storage, state, options, false, options.get_timestamp(state));
    }
private:
    future<shared_ptr<transport::messages::result_message>> execute(
            distributed<service::storage_proxy>& storage,
            service::query_state& query_state, const query_options& options,
            bool local, api::timestamp_type now) {
        // FIXME: we don't support nulls here
#if 0
        if (options.get_consistency() == null)
            throw new InvalidRequestException("Invalid empty consistency level");
        if (options.getSerialConsistency() == null)
            throw new InvalidRequestException("Invalid empty serial consistency level");
#endif
        if (_has_conditions) {
            return execute_with_conditions(storage, options, query_state);
        }

        return get_mutations(storage, options, local, now).then([this, &storage, &options] (std::vector<mutation> ms) {
            return execute_without_conditions(storage, std::move(ms), options.get_consistency());
        }).then([] {
            return make_ready_future<shared_ptr<transport::messages::result_message>>(
                    make_shared<transport::messages::result_message::void_message>());
        });
    }

    future<> execute_without_conditions(
            distributed<service::storage_proxy>& storage,
            std::vector<mutation> mutations,
            db::consistency_level cl) {
        // FIXME: do we need to do this?
#if 0
        // Extract each collection of cfs from it's IMutation and then lazily concatenate all of them into a single Iterable.
        Iterable<ColumnFamily> cfs = Iterables.concat(Iterables.transform(mutations, new Function<IMutation, Collection<ColumnFamily>>()
        {
            public Collection<ColumnFamily> apply(IMutation im)
            {
                return im.getColumnFamilies();
            }
        }));
#endif
        verify_batch_size(mutations);

        bool mutate_atomic = _type == type::LOGGED && mutations.size() > 1;
        return storage.local().mutate_with_triggers(std::move(mutations), cl, mutate_atomic);
    }

    future<shared_ptr<transport::messages::result_message>> execute_with_conditions(
            distributed<service::storage_proxy>& storage,
            const query_options& options,
            service::query_state& state) {
        fail(unimplemented::cause::LWT);
#if 0
        auto now = state.get_timestamp();
        ByteBuffer key = null;
        String ksName = null;
        String cfName = null;
        CQL3CasRequest casRequest = null;
        Set<ColumnDefinition> columnsWithConditions = new LinkedHashSet<>();

        for (int i = 0; i < statements.size(); i++)
        {
            ModificationStatement statement = statements.get(i);
            QueryOptions statementOptions = options.forStatement(i);
            long timestamp = attrs.getTimestamp(now, statementOptions);
            List<ByteBuffer> pks = statement.buildPartitionKeyNames(statementOptions);
            if (pks.size() > 1)
                throw new IllegalArgumentException("Batch with conditions cannot span multiple partitions (you cannot use IN on the partition key)");
            if (key == null)
            {
                key = pks.get(0);
                ksName = statement.cfm.ksName;
                cfName = statement.cfm.cfName;
                casRequest = new CQL3CasRequest(statement.cfm, key, true);
            }
            else if (!key.equals(pks.get(0)))
            {
                throw new InvalidRequestException("Batch with conditions cannot span multiple partitions");
            }

            Composite clusteringPrefix = statement.createClusteringPrefix(statementOptions);
            if (statement.hasConditions())
            {
                statement.addConditions(clusteringPrefix, casRequest, statementOptions);
                // As soon as we have a ifNotExists, we set columnsWithConditions to null so that everything is in the resultSet
                if (statement.hasIfNotExistCondition() || statement.hasIfExistCondition())
                    columnsWithConditions = null;
                else if (columnsWithConditions != null)
                    Iterables.addAll(columnsWithConditions, statement.getColumnsWithConditions());
            }
            casRequest.addRowUpdate(clusteringPrefix, statement, statementOptions, timestamp);
        }

        ColumnFamily result = StorageProxy.cas(ksName, cfName, key, casRequest, options.getSerialConsistency(), options.getConsistency(), state.getClientState());

        return new ResultMessage.Rows(ModificationStatement.buildCasResultSet(ksName, key, cfName, result, columnsWithConditions, true, options.forStatement(0)));
#endif
    }

public:
    virtual future<shared_ptr<transport::messages::result_message>> execute_internal(
            distributed<service::storage_proxy>& proxy,
            service::query_state& query_state, const query_options& options) override {
        throw std::runtime_error(sprint("%s not implemented", __PRETTY_FUNCTION__));
#if 0
        assert !hasConditions;
        for (IMutation mutation : getMutations(BatchQueryOptions.withoutPerStatementVariables(options), true, queryState.getTimestamp()))
        {
            // We don't use counters internally.
            assert mutation instanceof Mutation;
            ((Mutation) mutation).apply();
        }
        return null;
#endif
    }

    // FIXME: no cql_statement::to_string() yet
#if 0
    sstring to_string() const {
        return sprint("BatchStatement(type=%s, statements=%s)", _type, join(", ", _statements));
    }
#endif
};

}
}
