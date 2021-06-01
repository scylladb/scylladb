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
#include "cql3/util.hh"
#include "raw/batch_statement.hh"
#include "db/config.hh"
#include "db/consistency_level_validations.hh"
#include "database.hh"
#include <seastar/core/execution_stage.hh>
#include "cas_request.hh"
#include "cql3/query_processor.hh"
#include "service/storage_proxy.hh"

#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/range/adaptor/uniqued.hpp>

namespace cql3 {

namespace statements {

logging::logger batch_statement::_logger("BatchStatement");

timeout_config_selector
timeout_for_type(batch_statement::type t) {
    return t == batch_statement::type::COUNTER
            ? &timeout_config::counter_write_timeout
            : &timeout_config::write_timeout;
}

db::timeout_clock::duration batch_statement::get_timeout(const service::client_state& state, const query_options& options) const {
    return _attrs->is_timeout_set() ? _attrs->get_timeout(options) : state.get_timeout_config().*get_timeout_config_selector();
}

batch_statement::batch_statement(int bound_terms, type type_,
                                 std::vector<single_statement> statements,
                                 std::unique_ptr<attributes> attrs,
                                 cql_stats& stats)
    : cql_statement_opt_metadata(timeout_for_type(type_))
    , _bound_terms(bound_terms), _type(type_), _statements(std::move(statements))
    , _attrs(std::move(attrs))
    , _has_conditions(boost::algorithm::any_of(_statements, [] (auto&& s) { return s.statement->has_conditions(); }))
    , _stats(stats)
{
    validate();
    if (has_conditions()) {
        // A batch can be created not only by raw::batch_statement::prepare, but also by
        // cql_server::connection::process_batch, which doesn't call any methods of
        // cql3::statements::batch_statement, only constructs it. So let's call
        // build_cas_result_set_metadata right from the constructor to avoid crash trying to access
        // uninitialized batch metadata.
        build_cas_result_set_metadata();
    }
}

batch_statement::batch_statement(type type_,
                                 std::vector<single_statement> statements,
                                 std::unique_ptr<attributes> attrs,
                                 cql_stats& stats)
    : batch_statement(-1, type_, std::move(statements), std::move(attrs), stats)
{
}

bool batch_statement::depends_on_keyspace(const sstring& ks_name) const
{
    return false;
}

bool batch_statement::depends_on_column_family(const sstring& cf_name) const
{
    return false;
}

uint32_t batch_statement::get_bound_terms() const
{
    return _bound_terms;
}

future<> batch_statement::check_access(service::storage_proxy& proxy, const service::client_state& state) const
{
    return parallel_for_each(_statements.begin(), _statements.end(), [&proxy, &state](auto&& s) {
        if (s.needs_authorization) {
            return s.statement->check_access(proxy, state);
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
        throw exceptions::invalid_request_exception("BATCH with conditions cannot span multiple tables");
    }
    std::optional<bool> raw_counter;
    for (auto& s : _statements) {
        if (raw_counter && s.statement->is_raw_counter_shard_write() != *raw_counter) {
            throw exceptions::invalid_request_exception("Cannot mix raw and regular counter statements in batch");
        }
        raw_counter = s.statement->is_raw_counter_shard_write();
    }
}

void batch_statement::validate(service::storage_proxy& proxy, const service::client_state& state) const
{
    for (auto&& s : _statements) {
        s.statement->validate(proxy, state);
    }
}

const std::vector<batch_statement::single_statement>& batch_statement::get_statements()
{
    return _statements;
}

future<std::vector<mutation>> batch_statement::get_mutations(service::storage_proxy& storage, const query_options& options,
        db::timeout_clock::time_point timeout, bool local, api::timestamp_type now, service::query_state& query_state) const {
    // Do not process in parallel because operations like list append/prepend depend on execution order.
    using mutation_set_type = std::unordered_set<mutation, mutation_hash_by_key, mutation_equals_by_key>;
    return do_with(mutation_set_type(), [this, &storage, &options, timeout, now, local, &query_state] (auto& result) mutable {
        result.reserve(_statements.size());
        return do_for_each(boost::make_counting_iterator<size_t>(0),
                           boost::make_counting_iterator<size_t>(_statements.size()),
                           [this, &storage, &options, now, local, &result, timeout, &query_state] (size_t i) {
            auto&& statement = _statements[i].statement;
            statement->inc_cql_stats(query_state.get_client_state().is_internal());
            auto&& statement_options = options.for_statement(i);
            auto timestamp = _attrs->get_timestamp(now, statement_options);
            return statement->get_mutations(storage, statement_options, timeout, local, timestamp, query_state).then([&result] (auto&& more) {
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

void batch_statement::verify_batch_size(service::storage_proxy& proxy, const std::vector<mutation>& mutations) {
    if (mutations.size() <= 1) {
        return;     // We only warn for batch spanning multiple mutations
    }

    size_t warn_threshold = proxy.get_db().local().get_config().batch_size_warn_threshold_in_kb() * 1024;
    size_t fail_threshold = proxy.get_db().local().get_config().batch_size_fail_threshold_in_kb() * 1024;

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
            return format("Batch modifying {:d} partitions in {} is of size {:d} bytes, exceeding specified {} threshold of {:d} by {:d}.",
                    mutations.size(), join(", ", ks_cf_pairs), size, type, threshold, size - threshold);
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
        const batch_statement*,
        service::storage_proxy&,
        service::query_state&,
        const query_options&,
        bool,
        api::timestamp_type> batch_stage{"cql3_batch", batch_statement_executor::get()};

future<shared_ptr<cql_transport::messages::result_message>> batch_statement::execute(
        query_processor& qp, service::query_state& state, const query_options& options) const {
    service::storage_proxy& storage = qp.proxy();
    cql3::util::validate_timestamp(options, _attrs);
    return batch_stage(this, seastar::ref(storage), seastar::ref(state),
                       seastar::cref(options), false, options.get_timestamp(state));
}

future<shared_ptr<cql_transport::messages::result_message>> batch_statement::do_execute(
        service::storage_proxy& storage,
        service::query_state& query_state, const query_options& options,
        bool local, api::timestamp_type now) const
{
    // FIXME: we don't support nulls here
#if 0
    if (options.get_consistency() == null)
        throw new InvalidRequestException("Invalid empty consistency level");
    if (options.getSerialConsistency() == null)
        throw new InvalidRequestException("Invalid empty serial consistency level");
#endif
    if (_has_conditions) {
        ++_stats.cas_batches;
        _stats.statements_in_cas_batches += _statements.size();
        return execute_with_conditions(storage, options, query_state);
    }

    ++_stats.batches;
    _stats.statements_in_batches += _statements.size();

    auto timeout = db::timeout_clock::now() + get_timeout(query_state.get_client_state(), options);
    return get_mutations(storage, options, timeout, local, now, query_state).then([this, &storage, &options, timeout, tr_state = query_state.get_trace_state(),
                                                                                                                               permit = query_state.get_permit()] (std::vector<mutation> ms) mutable {
        return execute_without_conditions(storage, std::move(ms), options.get_consistency(), timeout, std::move(tr_state), std::move(permit));
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
        tracing::trace_state_ptr tr_state,
        service_permit permit) const
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
    verify_batch_size(storage, mutations);

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
    return storage.mutate_with_triggers(std::move(mutations), cl, timeout, mutate_atomic, std::move(tr_state), std::move(permit));
}

future<shared_ptr<cql_transport::messages::result_message>> batch_statement::execute_with_conditions(
        service::storage_proxy& proxy,
        const query_options& options,
        service::query_state& qs) const {

    auto cl_for_learn = options.get_consistency();
    auto cl_for_paxos = options.check_serial_consistency();
    seastar::shared_ptr<cas_request> request;
    schema_ptr schema;

    db::timeout_clock::time_point now = db::timeout_clock::now();
    const timeout_config& cfg = qs.get_client_state().get_timeout_config();
    auto batch_timeout = now + cfg.write_timeout; // Statement timeout.
    auto cas_timeout = now + cfg.cas_timeout;     // Ballot contention timeout.
    auto read_timeout = now + cfg.read_timeout;   // Query timeout.

    for (size_t i = 0; i < _statements.size(); ++i) {

        modification_statement& statement = *_statements[i].statement;
        const query_options& statement_options = options.for_statement(i);

        statement.inc_cql_stats(qs.get_client_state().is_internal());
        modification_statement::json_cache_opt json_cache = statement.maybe_prepare_json_cache(statement_options);
        // At most one key
        std::vector<dht::partition_range> keys = statement.build_partition_keys(statement_options, json_cache);
        if (keys.empty()) {
            continue;
        }
        if (request.get() == nullptr) {
            schema = statement.s;
            request = seastar::make_shared<cas_request>(schema, std::move(keys));
        } else if (keys.size() != 1 || keys.front().equal(request->key().front(), dht::ring_position_comparator(*schema)) == false) {
            throw exceptions::invalid_request_exception("BATCH with conditions cannot span multiple partitions");
        }
        std::vector<query::clustering_range> ranges = statement.create_clustering_ranges(statement_options, json_cache);

        request->add_row_update(statement, std::move(ranges), std::move(json_cache), statement_options);
    }
    if (request.get() == nullptr) {
        throw exceptions::invalid_request_exception(format("Unrestricted partition key in a conditional BATCH"));
    }

    auto shard = service::storage_proxy::cas_shard(*_statements[0].statement->s, request->key()[0].start()->value().as_decorated_key().token());
    if (shard != this_shard_id()) {
        proxy.get_stats().replica_cross_shard_ops++;
        return make_ready_future<shared_ptr<cql_transport::messages::result_message>>(
                make_shared<cql_transport::messages::result_message::bounce_to_shard>(shard));
    }

    return proxy.cas(schema, request, request->read_command(proxy), request->key(),
            {read_timeout, qs.get_permit(), qs.get_client_state(), qs.get_trace_state()},
            cl_for_paxos, cl_for_learn, batch_timeout, cas_timeout).then([this, request] (bool is_applied) {
        return request->build_cas_result_set(_metadata, _columns_of_cas_result_set, is_applied);
    });
}

void batch_statement::build_cas_result_set_metadata() {
    if (_statements.empty()) {
        return;
    }
    const auto& schema = *_statements.front().statement->s;

    _columns_of_cas_result_set.resize(schema.all_columns_count());

    // Add the mandatory [applied] column to result set metadata
    std::vector<lw_shared_ptr<column_specification>> columns;

    auto applied = make_lw_shared<cql3::column_specification>(schema.ks_name(), schema.cf_name(),
            ::make_shared<cql3::column_identifier>("[applied]", false), boolean_type);
    columns.push_back(applied);

    for (const auto& def : boost::range::join(schema.partition_key_columns(), schema.clustering_key_columns())) {
        _columns_of_cas_result_set.set(def.ordinal_id);
    }
    for (const auto& s : _statements) {
        _columns_of_cas_result_set.union_with(s.statement->columns_of_cas_result_set());
    }
    columns.reserve(_columns_of_cas_result_set.count());
    for (const auto& def : schema.all_columns()) {
        if (_columns_of_cas_result_set.test(def.ordinal_id)) {
            columns.emplace_back(def.column_specification);
        }
    }
    _metadata = seastar::make_shared<cql3::metadata>(std::move(columns));
}

namespace raw {

std::unique_ptr<prepared_statement>
batch_statement::prepare(database& db, cql_stats& stats) {
    auto&& bound_names = get_bound_variables();

    std::optional<sstring> first_ks;
    std::optional<sstring> first_cf;
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

    cql3::statements::batch_statement batch_statement_(bound_names.size(), _type, std::move(statements), std::move(prep_attrs), stats);

    std::vector<uint16_t> partition_key_bind_indices;
    if (!have_multiple_cfs && batch_statement_.get_statements().size() > 0) {
        partition_key_bind_indices = bound_names.get_partition_key_bind_indexes(*batch_statement_.get_statements()[0].statement->s);
    }
    return std::make_unique<prepared_statement>(make_shared<cql3::statements::batch_statement>(std::move(batch_statement_)),
                                                     bound_names.get_specifications(),
                                                     std::move(partition_key_bind_indices));
}

}


}

}


