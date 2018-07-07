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

#include "batch_statement.hh"
#include "raw/batch_statement.hh"
#include "db/config.hh"
#include <seastar/core/execution_stage.hh>

namespace {

struct mutation_equals_by_key {
    bool operator()(const mutation& m1, const mutation& m2) const {
        return m1.schema() == m2.schema()
                && m1.decorated_key().equal(*m1.schema(), m2.decorated_key());
    }
};

struct mutation_hash_by_key {
    size_t operator()(const mutation& m) const {
        auto dk_hash = std::hash<dht::decorated_key>();
        return dk_hash(m.decorated_key());
    }
};

}


namespace cql3 {

namespace statements {

logging::logger batch_statement::_logger("BatchStatement");

timeout_config_selector
timeout_for_type(batch_statement::type t) {
    return t == batch_statement::type::COUNTER
            ? &timeout_config::counter_write_timeout
            : &timeout_config::write_timeout;
}

batch_statement::batch_statement(int bound_terms, type type_,
                                 std::vector<single_statement> statements,
                                 std::unique_ptr<attributes> attrs,
                                 cql_stats& stats)
    : cql_statement_no_metadata(timeout_for_type(type_))
    , _bound_terms(bound_terms), _type(type_), _statements(std::move(statements))
    , _attrs(std::move(attrs))
    , _has_conditions(boost::algorithm::any_of(_statements, [] (auto&& s) { return s.statement->has_conditions(); }))
    , _stats(stats)
{
}

batch_statement::batch_statement(type type_,
                                 std::vector<single_statement> statements,
                                 std::unique_ptr<attributes> attrs,
                                 cql_stats& stats)
    : batch_statement(-1, type_, std::move(statements), std::move(attrs), stats)
{
}

bool batch_statement::uses_function(const sstring& ks_name, const sstring& function_name) const
{
    return _attrs->uses_function(ks_name, function_name)
            || boost::algorithm::any_of(_statements, [&] (auto&& s) { return s.statement->uses_function(ks_name, function_name); });
}

bool batch_statement::depends_on_keyspace(const sstring& ks_name) const
{
    return false;
}

bool batch_statement::depends_on_column_family(const sstring& cf_name) const
{
    return false;
}

uint32_t batch_statement::get_bound_terms()
{
    return _bound_terms;
}

future<> batch_statement::check_access(const service::client_state& state)
{
    return parallel_for_each(_statements.begin(), _statements.end(), [&state](auto&& s) {
        if (s.needs_authorization) {
            return s.statement->check_access(state);
        } else {
            return make_ready_future<>();
        }
    });
}

void batch_statement::validate()
{
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

    bool has_counters = boost::algorithm::any_of(_statements, [] (auto&& s) { return s.statement->is_counter(); });
    bool has_non_counters = !boost::algorithm::all_of(_statements, [] (auto&& s) { return s.statement->is_counter(); });
    if (timestamp_set && has_counters) {
        throw exceptions::invalid_request_exception("Cannot provide custom timestamp for a BATCH containing counters");
    }
    if (timestamp_set && boost::algorithm::any_of(_statements, [] (auto&& s) { return s.statement->is_timestamp_set(); })) {
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
                            | boost::adaptors::transformed([] (auto&& s) { return s.statement->keyspace(); })
                            | boost::adaptors::uniqued) != 1
                || (boost::distance(_statements
                        | boost::adaptors::transformed([] (auto&& s) { return s.statement->column_family(); })
                        | boost::adaptors::uniqued) != 1))) {
        throw exceptions::invalid_request_exception("Batch with conditions cannot span multiple tables");
    }
    std::experimental::optional<bool> raw_counter;
    for (auto& s : _statements) {
        if (raw_counter && s.statement->is_raw_counter_shard_write() != *raw_counter) {
            throw exceptions::invalid_request_exception("Cannot mix raw and regular counter statements in batch");
        }
        raw_counter = s.statement->is_raw_counter_shard_write();
    }
}

void batch_statement::validate(service::storage_proxy& proxy, const service::client_state& state)
{
    for (auto&& s : _statements) {
        s.statement->validate(proxy, state);
    }
}

const std::vector<batch_statement::single_statement>& batch_statement::get_statements()
{
    return _statements;
}

future<std::vector<mutation>> batch_statement::get_mutations(service::storage_proxy& storage, const query_options& options, db::timeout_clock::time_point timeout, bool local, api::timestamp_type now, tracing::trace_state_ptr trace_state) {
    // Do not process in parallel because operations like list append/prepend depend on execution order.
    using mutation_set_type = std::unordered_set<mutation, mutation_hash_by_key, mutation_equals_by_key>;
    return do_with(mutation_set_type(), [this, &storage, &options, timeout, now, local, trace_state] (auto& result) {
        result.reserve(_statements.size());
        _stats.statements_in_batches += _statements.size();
        return do_for_each(boost::make_counting_iterator<size_t>(0),
                           boost::make_counting_iterator<size_t>(_statements.size()),
                           [this, &storage, &options, now, local, &result, timeout, trace_state] (size_t i) {
            auto&& statement = _statements[i].statement;
            statement->inc_cql_stats();
            auto&& statement_options = options.for_statement(i);
            auto timestamp = _attrs->get_timestamp(now, statement_options);
            return statement->get_mutations(storage, statement_options, timeout, local, timestamp, trace_state).then([&result] (auto&& more) {
                for (auto&& m : more) {
                    // We want unordered_set::try_emplace(), but we don't have it
                    auto pos = result.find(m);
                    if (pos == result.end()) {
                        result.emplace(std::move(m));
                    } else {
                        const_cast<mutation&>(*pos).apply(std::move(m)); // Won't change key
                    }
                }
            });
        }).then([&result] {
            // can't use range adaptors, because we want to move
            auto vresult = std::vector<mutation>();
            vresult.reserve(result.size());
            for (auto&& m : result) {
                vresult.push_back(std::move(m));
            }
            return vresult;
        });
    });
}

void batch_statement::verify_batch_size(const std::vector<mutation>& mutations) {
    if (mutations.size() <= 1) {
        return;     // We only warn for batch spanning multiple mutations
    }

    size_t warn_threshold = service::get_local_storage_proxy().get_db().local().get_config().batch_size_warn_threshold_in_kb() * 1024;
    size_t fail_threshold = service::get_local_storage_proxy().get_db().local().get_config().batch_size_fail_threshold_in_kb() * 1024;

    size_t size = 0;
    for (auto&m : mutations) {
        size += m.partition().external_memory_usage(*m.schema());
    }

    if (size > warn_threshold) {
        auto error = [&] (const char* type, size_t threshold) -> sstring {
            std::unordered_set<sstring> ks_cf_pairs;
            for (auto&& m : mutations) {
                ks_cf_pairs.insert(m.schema()->ks_name() + "." + m.schema()->cf_name());
            }
            return sprint("Batch of prepared statements for %s is of size %d, exceeding specified %s threshold of %d by %d.",
                    join(", ", ks_cf_pairs), size, type, threshold, size - threshold);
        };
        if (size > fail_threshold) {
            _logger.error(error("FAIL", fail_threshold).c_str());
            throw exceptions::invalid_request_exception("Batch too large");
        } else {
            _logger.warn(error("WARN", warn_threshold).c_str());
        }
    }
}

struct batch_statement_executor {
    static auto get() { return &batch_statement::do_execute; }
};
static thread_local inheriting_concrete_execution_stage<
        future<shared_ptr<cql_transport::messages::result_message>>,
        batch_statement*,
        service::storage_proxy&,
        service::query_state&,
        const query_options&,
        bool,
        api::timestamp_type> batch_stage{"cql3_batch", batch_statement_executor::get()};

future<shared_ptr<cql_transport::messages::result_message>> batch_statement::execute(
        service::storage_proxy& storage, service::query_state& state, const query_options& options) {
    ++_stats.batches;
    return batch_stage(this, seastar::ref(storage), seastar::ref(state),
                       seastar::cref(options), false, options.get_timestamp(state));
}

future<shared_ptr<cql_transport::messages::result_message>> batch_statement::do_execute(
        service::storage_proxy& storage,
        service::query_state& query_state, const query_options& options,
        bool local, api::timestamp_type now)
{
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

    auto timeout = db::timeout_clock::now() + options.get_timeout_config().*get_timeout_config_selector();
    return get_mutations(storage, options, timeout, local, now, query_state.get_trace_state()).then([this, &storage, &options, timeout, tr_state = query_state.get_trace_state()] (std::vector<mutation> ms) mutable {
        return execute_without_conditions(storage, std::move(ms), options.get_consistency(), timeout, std::move(tr_state));
    }).then([] {
        return make_ready_future<shared_ptr<cql_transport::messages::result_message>>(
                make_shared<cql_transport::messages::result_message::void_message>());
    });
}

future<> batch_statement::execute_without_conditions(
        service::storage_proxy& storage,
        std::vector<mutation> mutations,
        db::consistency_level cl,
        db::timeout_clock::time_point timeout,
        tracing::trace_state_ptr tr_state)
{
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

    bool mutate_atomic = true;
    if (_type != type::LOGGED) {
        _stats.batches_pure_unlogged += 1;
        mutate_atomic = false;
    } else {
        if (mutations.size() > 1) {
            _stats.batches_pure_logged += 1;
        } else {
            _stats.batches_unlogged_from_logged += 1;
            mutate_atomic = false;
        }
    }
    return storage.mutate_with_triggers(std::move(mutations), cl, timeout, mutate_atomic, std::move(tr_state));
}

future<shared_ptr<cql_transport::messages::result_message>> batch_statement::execute_with_conditions(
        service::storage_proxy& storage,
        const query_options& options,
        service::query_state& state)
{
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

namespace raw {

std::unique_ptr<prepared_statement>
batch_statement::prepare(database& db, cql_stats& stats) {
    auto&& bound_names = get_bound_variables();

    stdx::optional<sstring> first_ks;
    stdx::optional<sstring> first_cf;
    bool have_multiple_cfs = false;

    std::vector<cql3::statements::batch_statement::single_statement> statements;
    statements.reserve(_parsed_statements.size());

    for (auto&& parsed : _parsed_statements) {
        if (!first_ks) {
            first_ks = parsed->keyspace();
            first_cf = parsed->column_family();
        } else {
            have_multiple_cfs = first_ks.value() != parsed->keyspace() || first_cf.value() != parsed->column_family();
        }
        statements.emplace_back(parsed->prepare(db, bound_names, stats));
    }

    auto&& prep_attrs = _attrs->prepare(db, "[batch]", "[batch]");
    prep_attrs->collect_marker_specification(bound_names);

    cql3::statements::batch_statement batch_statement_(bound_names->size(), _type, std::move(statements), std::move(prep_attrs), stats);
    batch_statement_.validate();

    std::vector<uint16_t> partition_key_bind_indices;
    if (!have_multiple_cfs && batch_statement_.get_statements().size() > 0) {
        partition_key_bind_indices = bound_names->get_partition_key_bind_indexes(batch_statement_.get_statements()[0].statement->s);
    }
    return std::make_unique<prepared>(make_shared(std::move(batch_statement_)),
                                                     bound_names->get_specifications(),
                                                     std::move(partition_key_bind_indices));
}

}


}

}


