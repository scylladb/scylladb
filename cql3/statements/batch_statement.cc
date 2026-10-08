/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include "batch_statement.hh"
#include "cql3/statements/modification_statement.hh"
#include "cql3/util.hh"
#include "raw/batch_statement.hh"
#include "cql3/cql_config.hh"
#include "db/consistency_level_validations.hh"
#include "data_dictionary/data_dictionary.hh"
#include <ranges>
#include <seastar/core/execution_stage.hh>
#include "cql3/query_processor.hh"
#include "tracing/trace_state.hh"
#include "cql3/statements/modification_executor.hh"
#include "cql3/statements/eventual_consistency/batch_executor.hh"

namespace cql3 {

namespace statements {

logging::logger batch_statement::_logger("BatchStatement");

timeout_config_selector
timeout_for_type(batch_statement::type t) {
    return t == batch_statement::type::COUNTER
            ? &timeout_config::counter_write_timeout
            : &timeout_config::write_timeout;
}

int64_t batch_statement::get_timestamp(int64_t now, const query_options& options) const {
    return _attrs->get_timestamp(now, options);
}

db::timeout_clock::duration batch_statement::get_timeout(const service::client_state& state, const query_options& options) const {
    return _attrs->is_timeout_set() ? _attrs->get_timeout(options) : state.get_timeout_config().*get_timeout_config_selector();
}

// A batch commits its modifications together, so they must all reach storage
// the same way, and the way they do picks the batch's. An empty batch writes
// nothing, and any executor would do.
static const batch_executor& executor_for(const std::vector<batch_statement::single_statement>& statements) {
    if (statements.empty()) {
        return eventual_consistency::batch_executor::instance();
    }
    const modification_executor& executor = statements.front().statement->executor();
    for (const auto& s : statements) {
        if (&s.statement->executor() != &executor) {
            throw exceptions::invalid_request_exception("Cannot mix strongly consistent and eventually consistent statements in a batch");
        }
    }
    return executor.for_batch();
}

batch_statement::batch_statement(int bound_terms, type type_,
                                 std::vector<single_statement> statements,
                                 std::unique_ptr<attributes> attrs,
                                 cql_stats& stats)
    : cql_statement(timeout_for_type(type_))
    , _bound_terms(bound_terms), _type(type_), _statements(std::move(statements))
    , _attrs(std::move(attrs))
    , _has_conditions(std::ranges::any_of(_statements, [] (auto&& s) { return s.statement->has_conditions(); }))
    , _stats(stats)
    , _executor(&executor_for(_statements))
{
    // What a batch may contain depends on how it is committed, so the executor
    // says: a strongly consistent one is stricter about counters, timestamps and
    // how many tables it spans.
    _executor->validate(*this);
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

void batch_statement::validate(query_processor& qp, const service::client_state& state) const
{
    for (auto&& s : _statements) {
        s.statement->validate(qp, state);
    }
}

const std::vector<batch_statement::single_statement>& batch_statement::get_statements() const
{
    return _statements;
}

void batch_statement::verify_batch_size(query_processor& qp, const utils::chunked_vector<mutation>& mutations) const {
    if (mutations.size() <= 1) {
        return;     // We only warn for batch spanning multiple mutations
    }

    size_t warn_threshold = qp.get_cql_config().batch_size_warn_threshold_in_kb() * 1024;
    size_t fail_threshold = qp.get_cql_config().batch_size_fail_threshold_in_kb() * 1024;

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
            const auto batch_type = _type == type::LOGGED ? "Logged" : "Unlogged";
            return seastar::format("{} batch modifying {:d} partitions in {} is of size {:d} bytes, exceeding specified {} threshold of {:d} by {:d}.",
                    batch_type, mutations.size(), fmt::join(ks_cf_pairs, ", "), size, type, threshold, size - threshold);
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
    cql3::util::validate_timestamp(qp.get_cql_config(), options, _attrs);
    return batch_stage(this, seastar::ref(qp), seastar::ref(state),
                       seastar::cref(options), false, options.get_timestamp(state));
}

future<shared_ptr<cql_transport::messages::result_message>> batch_statement::do_execute(
        query_processor& qp,
        service::query_state& query_state, const query_options& options,
        bool local, api::timestamp_type now) const
{
    return executor().commit(*this, qp, query_state, options, local, now);
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
batch_statement::prepare(data_dictionary::database db, cql_stats& stats, const cql_config& cfg) {
    auto&& meta = get_prepare_context();

    std::optional<sstring> first_ks;
    std::optional<sstring> first_cf;
    bool have_multiple_cfs = false;

    std::vector<cql3::statements::batch_statement::single_statement> statements;
    statements.reserve(_parsed_statements.size());
    std::vector<std::reference_wrapper<const audit::audit_info>> batch_audit_infos;
    batch_audit_infos.reserve(_parsed_statements.size());

    for (auto&& parsed : _parsed_statements) {
        if (!first_ks) {
            first_ks = parsed->keyspace();
            first_cf = parsed->column_family();
        } else {
            have_multiple_cfs |= first_ks.value() != parsed->keyspace();
            have_multiple_cfs |= first_cf.value() != parsed->column_family();
        }
        auto statement = parsed->prepare(db, meta, stats);
        if (auto* audit_info = statement->get_audit_info()) {
            audit_info->set_query_string(parsed->get_raw_cql());
            batch_audit_infos.emplace_back(*audit_info);
        }
        statements.emplace_back(std::move(statement));
    }
    auto&& prep_attrs = _attrs->prepare(db, "[batch]", "[batch]");
    prep_attrs->fill_prepare_context(meta);

    std::vector<uint16_t> partition_key_bind_indices;
    if (!have_multiple_cfs && !statements.empty()) {
        partition_key_bind_indices = meta.get_partition_key_bind_indexes(*statements[0].statement->s);
    }

    shared_ptr<cql_statement> statement = ::make_shared<cql3::statements::batch_statement>(
            meta.bound_variables_size(), _type, std::move(statements), std::move(prep_attrs), stats);

    auto ai = audit_info();
    if (ai) {
        ai->set_batch_infos(std::move(batch_audit_infos));
    }

    return std::make_unique<prepared_statement>(std::move(ai), std::move(statement),
                                                      meta.get_variable_specifications(),
                                                      std::move(partition_key_bind_indices));
}

audit::statement_category batch_statement::category() const {
    return audit::statement_category::DML;
}

}


}

}


