/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "batch_statement.hh"
#include "cql3/util.hh"
#include "raw/batch_statement.hh"
#include "db/config.hh"
#include "db/consistency_level_validations.hh"
#include "data_dictionary/data_dictionary.hh"
#include <seastar/core/execution_stage.hh>
#include "cas_request.hh"
#include "cql3/query_processor.hh"
#include "service/storage_proxy.hh"
#include "tracing/trace_state.hh"
#include "utils/unique_view.hh"

template<typename T = void>
using coordinator_result = exceptions::coordinator_result<T>;

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
    , _has_conditions(std::ranges::any_of(_statements, [] (auto&& s) { return s.statement->has_conditions(); }))
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

bool batch_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const
{
    return std::ranges::any_of(_statements, [&ks_name, &cf_name] (auto&& s) { return s.statement->depends_on(ks_name, cf_name); });
}

uint32_t batch_statement::get_bound_terms() const
{
    return _bound_terms;
}

future<> batch_statement::check_access(query_processor& qp, const service::client_state& state) const
{
    return parallel_for_each(_statements.begin(), _statements.end(), [&qp, &state](auto&& s) {
        if (s.needs_authorization) {
            return s.statement->check_access(qp, state);
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

    bool has_counters = std::ranges::any_of(_statements, [] (auto&& s) { return s.statement->is_counter(); });
    bool has_non_counters = !std::ranges::all_of(_statements, [] (auto&& s) { return s.statement->is_counter(); });
    if (timestamp_set && has_counters) {
        throw exceptions::invalid_request_exception("Cannot provide custom timestamp for a BATCH containing counters");
    }
    if (timestamp_set && std::ranges::any_of(_statements, [] (auto&& s) { return s.statement->is_timestamp_set(); })) {
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
            && (std::ranges::distance(_statements
                            | std::views::transform([] (auto&& s) { return s.statement->keyspace(); })
                            | utils::views::unique) != 1
                || (std::ranges::distance(_statements
                        | std::views::transform([] (auto&& s) { return s.statement->column_family(); })
                        | utils::views::unique) != 1))) {
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

void batch_statement::validate(query_processor& qp, const service::client_state& state) const
{
    for (auto&& s : _statements) {
        s.statement->validate(qp, state);
    }
}

const std::vector<batch_statement::single_statement>& batch_statement::get_statements()
{
    return _statements;
}

future<std::vector<mutation>> batch_statement::get_mutations(query_processor& qp, const query_options& options,
        db::timeout_clock::time_point timeout, bool local, api::timestamp_type now, service::query_state& query_state) const {
    // Do not process in parallel because operations like list append/prepend depend on execution order.
    using mutation_set_type = std::unordered_set<mutation, mutation_hash_by_key, mutation_equals_by_key>;
    mutation_set_type result;
    result.reserve(_statements.size());
    for (size_t i = 0; i != _statements.size(); ++i) {
        auto&& statement = _statements[i].statement;
        statement->inc_cql_stats(query_state.get_client_state().is_internal());
        auto&& statement_options = options.for_statement(i);
        auto timestamp = _attrs->get_timestamp(now, statement_options);
        modification_statement::json_cache_opt json_cache = statement->maybe_prepare_json_cache(statement_options);
        std::vector<dht::partition_range> keys = statement->build_partition_keys(statement_options, json_cache);
        auto more = co_await statement->get_mutations(qp, statement_options, timeout, local, timestamp, query_state, json_cache, std::move(keys));

        for (auto&& m : more) {
            // We want unordered_set::try_emplace(), but we don't have it
            auto pos = result.find(m);
            if (pos == result.end()) {
                result.emplace(std::move(m));
            } else {
                const_cast<mutation&>(*pos).apply(std::move(m)); // Won't change key
            }
        }
    }

    // can't use range adaptors, because we want to move
    auto vresult = std::vector<mutation>();
    vresult.reserve(result.size());
    for (auto&& m : result) {
        vresult.push_back(std::move(m));
    }
    co_return vresult;
}

void batch_statement::verify_batch_size(query_processor& qp, const std::vector<mutation>& mutations) {
    if (mutations.size() <= 1) {
        return;     // We only warn for batch spanning multiple mutations
    }

    size_t warn_threshold = qp.db().get_config().batch_size_warn_threshold_in_kb() * 1024;
    size_t fail_threshold = qp.db().get_config().batch_size_fail_threshold_in_kb() * 1024;

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
            return seastar::format("Batch modifying {:d} partitions in {} is of size {:d} bytes, exceeding specified {} threshold of {:d} by {:d}.",
                    mutations.size(), fmt::join(ks_cf_pairs, ", "), size, type, threshold, size - threshold);
        };
        if (size > fail_threshold) {
            _logger.error("{}", error("FAIL", fail_threshold).c_str());
            throw exceptions::invalid_request_exception("Batch too large");
        } else {
            _logger.warn("{}", error("WARN", warn_threshold).c_str());
        }
    }
}

struct batch_statement_executor {
    static auto get() { return &batch_statement::do_execute; }
};
static thread_local inheriting_concrete_execution_stage<
        future<shared_ptr<cql_transport::messages::result_message>>,
        const batch_statement*,
        query_processor&,
        service::query_state&,
        const query_options&,
        bool,
        api::timestamp_type> batch_stage{"cql3_batch", batch_statement_executor::get()};

future<shared_ptr<cql_transport::messages::result_message>> batch_statement::execute(
        query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const {
    return execute_without_checking_exception_message(qp, state, options, std::move(guard))
            .then(cql_transport::messages::propagate_exception_as_future<shared_ptr<cql_transport::messages::result_message>>);
}

future<shared_ptr<cql_transport::messages::result_message>> batch_statement::execute_without_checking_exception_message(
        query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const {
    cql3::util::validate_timestamp(qp.db().get_config(), options, _attrs);
    return batch_stage(this, seastar::ref(qp), seastar::ref(state),
                       seastar::cref(options), false, options.get_timestamp(state));
}

future<shared_ptr<cql_transport::messages::result_message>> batch_statement::do_execute(
        query_processor& qp,
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
    for (size_t i = 0; i < _statements.size(); ++i) {
        _statements[i].statement->restrictions().validate_primary_key(options.for_statement(i));
    }

    if (_has_conditions) {
        ++_stats.cas_batches;
        _stats.statements_in_cas_batches += _statements.size();
        return execute_with_conditions(qp, options, query_state);
    }

    ++_stats.batches;
    _stats.statements_in_batches += _statements.size();

    auto timeout = db::timeout_clock::now() + get_timeout(query_state.get_client_state(), options);
    return get_mutations(qp, options, timeout, local, now, query_state).then([this, &qp, &options, timeout, tr_state = query_state.get_trace_state(),
                                                                                                                               permit = query_state.get_permit()] (std::vector<mutation> ms) mutable {
        return execute_without_conditions(qp, std::move(ms), options.get_consistency(), timeout, std::move(tr_state), std::move(permit));
    }).then([] (coordinator_result<> res) {
        if (!res) {
            return make_ready_future<shared_ptr<cql_transport::messages::result_message>>(
                    seastar::make_shared<cql_transport::messages::result_message::exception>(std::move(res).assume_error()));
        }
        return make_ready_future<shared_ptr<cql_transport::messages::result_message>>(
                make_shared<cql_transport::messages::result_message::void_message>());
    });
}

future<coordinator_result<>> batch_statement::execute_without_conditions(
        query_processor& qp,
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
    verify_batch_size(qp, mutations);

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
    return qp.proxy().mutate_with_triggers(std::move(mutations), cl, timeout, mutate_atomic, std::move(tr_state), std::move(permit), db::allow_per_partition_rate_limit::yes);
}

future<shared_ptr<cql_transport::messages::result_message>> batch_statement::execute_with_conditions(
        query_processor& qp,
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

    computed_function_values cached_fn_calls;

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
        cached_fn_calls.merge(std::move(const_cast<cql3::query_options&>(statement_options).take_cached_pk_function_calls()));

        std::vector<query::clustering_range> ranges = statement.create_clustering_ranges(statement_options, json_cache);

        request->add_row_update(statement, std::move(ranges), std::move(json_cache), statement_options);
    }
    if (request.get() == nullptr) {
        throw exceptions::invalid_request_exception(format("Unrestricted partition key in a conditional BATCH"));
    }

    auto shard = service::storage_proxy::cas_shard(*_statements[0].statement->s, request->key()[0].start()->value().as_decorated_key().token());
    if (shard != this_shard_id()) {
        return make_ready_future<shared_ptr<cql_transport::messages::result_message>>(
                qp.bounce_to_shard(shard, std::move(cached_fn_calls))
            );
    }

    return qp.proxy().cas(schema, request, request->read_command(qp), request->key(),
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

    for (const auto& def : schema.primary_key_columns()) {
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
batch_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    auto&& meta = get_prepare_context();

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
        statements.emplace_back(parsed->prepare(db, meta, stats));
        auto audit_info = statements.back().statement->get_audit_info();
        if (audit_info) {
            audit_info->set_query_string(parsed->get_raw_cql());
        }
    }

    auto&& prep_attrs = _attrs->prepare(db, "[batch]", "[batch]");
    prep_attrs->fill_prepare_context(meta);

    cql3::statements::batch_statement batch_statement_(meta.bound_variables_size(), _type, std::move(statements), std::move(prep_attrs), stats);

    std::vector<uint16_t> partition_key_bind_indices;
    if (!have_multiple_cfs && batch_statement_.get_statements().size() > 0) {
        partition_key_bind_indices = meta.get_partition_key_bind_indexes(*batch_statement_.get_statements()[0].statement->s);
    }
    return std::make_unique<prepared_statement>(audit_info(), make_shared<cql3::statements::batch_statement>(std::move(batch_statement_)),
                                                     meta.get_variable_specifications(),
                                                     std::move(partition_key_bind_indices));
}

audit::statement_category batch_statement::category() const {
    return audit::statement_category::DML;
}

}


}

}


