/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "utils/assert.hh"
#include "cql3/cql_statement.hh"
#include "cql3/statements/modification_statement.hh"
#include "cql3/statements/strongly_consistent_modification_statement.hh"
#include "cql3/statements/raw/modification_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/util.hh"
#include "validation.hh"
#include "db/consistency_level_validations.hh"
#include <optional>
#include <seastar/core/shared_ptr.hh>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/indirected.hpp>
#include "transport/messages/result_message.hh"
#include "data_dictionary/data_dictionary.hh"
#include "replica/database.hh"
#include <seastar/core/execution_stage.hh>
#include "cas_request.hh"
#include "cql3/query_processor.hh"
#include "service/storage_proxy.hh"
#include "service/broadcast_tables/experimental/lang.hh"

template<typename T = void>
using coordinator_result = exceptions::coordinator_result<T>;

bool is_internal_keyspace(std::string_view name);

namespace cql3 {

namespace statements {

timeout_config_selector
modification_statement_timeout(const schema& s) {
    if (s.is_counter()) {
        return &timeout_config::counter_write_timeout;
    } else {
        return &timeout_config::write_timeout;
    }
}

db::timeout_clock::duration modification_statement::get_timeout(const service::client_state& state, const query_options& options) const {
    return attrs->is_timeout_set() ? attrs->get_timeout(options) : state.get_timeout_config().*get_timeout_config_selector();
}

modification_statement::modification_statement(statement_type type_, uint32_t bound_terms, schema_ptr schema_, std::unique_ptr<attributes> attrs_, cql_stats& stats_)
    : cql_statement_opt_metadata(modification_statement_timeout(*schema_))
    , type{type_}
    , _bound_terms{bound_terms}
    , _columns_to_read(schema_->all_columns_count())
    , _columns_of_cas_result_set(schema_->all_columns_count())
    , s{schema_}
    , attrs{std::move(attrs_)}
    , _column_operations{}
    , _stats(stats_)
    , _ks_sel(::is_internal_keyspace(schema_->ks_name()) ? ks_selector::SYSTEM : ks_selector::NONSYSTEM)
{ }

modification_statement::~modification_statement() = default;

uint32_t modification_statement::get_bound_terms() const {
    return _bound_terms;
}

const sstring& modification_statement::keyspace() const {
    return s->ks_name();
}

const sstring& modification_statement::column_family() const {
    return s->cf_name();
}

bool modification_statement::is_counter() const {
    return s->is_counter();
}

bool modification_statement::is_view() const {
    return s->is_view();
}

int64_t modification_statement::get_timestamp(int64_t now, const query_options& options) const {
    return attrs->get_timestamp(now, options);
}

bool modification_statement::is_timestamp_set() const {
    return attrs->is_timestamp_set();
}

std::optional<gc_clock::duration> modification_statement::get_time_to_live(const query_options& options) const {
    std::optional<int32_t> ttl = attrs->get_time_to_live(options);
    return ttl ? std::make_optional<gc_clock::duration>(*ttl) : std::nullopt;
}

future<> modification_statement::check_access(query_processor& qp, const service::client_state& state) const {
    auto f = state.has_column_family_access(keyspace(), column_family(), auth::permission::MODIFY);
    if (has_conditions()) {
        f = f.then([this, &state] {
           return state.has_column_family_access(keyspace(), column_family(), auth::permission::SELECT);
        });
    }
    return f;
}

future<std::vector<mutation>>
modification_statement::get_mutations(query_processor& qp, const query_options& options, db::timeout_clock::time_point timeout, bool local, int64_t now, service::query_state& qs, json_cache_opt& json_cache, std::vector<dht::partition_range> keys) const {
    auto cl = options.get_consistency();
    auto ranges = create_clustering_ranges(options, json_cache);
    auto f = make_ready_future<update_parameters::prefetch_data>(s);

    if (is_counter()) {
        db::validate_counter_for_write(*s, cl);
    } else {
        db::validate_for_write(cl);
    }

    if (requires_read()) {
        lw_shared_ptr<query::read_command> cmd = read_command(qp, ranges, cl);
        // FIXME: ignoring "local"
        f = qp.proxy().query(s, cmd, dht::partition_range_vector(keys), cl,
                {timeout, qs.get_permit(), qs.get_client_state(), qs.get_trace_state()}).then(

                [this, cmd] (auto cqr) {

            return update_parameters::build_prefetch_data(s, *cqr.query_result, cmd->slice);
        });
    }

    return f.then([this, keys = std::move(keys), ranges = std::move(ranges), json_cache = std::move(json_cache), &options, now]
            (auto rows) {

        update_parameters params(s, options, this->get_timestamp(now, options),
                this->get_time_to_live(options), std::move(rows));

        std::vector<mutation> mutations = apply_updates(keys, ranges, params, json_cache);

        return make_ready_future<std::vector<mutation>>(std::move(mutations));
    });
}

bool modification_statement::applies_to(const selection::selection* selection,
        const update_parameters::prefetch_data::row* row,
        const query_options& options) const {

    // Assume the row doesn't exist if it has no static columns and the statement is only interested
    // in static column values. Needed for EXISTS checks to work correctly. For example, the following
    // conditional INSERT must apply, because there's no static row in the partition although there's
    // a regular row, which is fetched by the read:
    //   CREATE TABLE t(p int, c int, s int static, PRIMARY KEY(p, c));
    //   INSERT INTO t(p, c) VALUES(1, 1);
    //   INSERT INTO t(p, s) VALUES(1, 1) IF NOT EXISTS;
    if (has_only_static_column_conditions() && row && !row->has_static_columns(*s)) {
        row = nullptr;
    }

    if (_if_exists) {
        return row != nullptr;
    }
    if (_if_not_exists) {
        return row == nullptr;
    }

    // Fake out an all-null static_and_regular_columns if we didn't find a row
    auto fake_static_and_regular_columns = std::vector<managed_bytes_opt>();
    auto static_and_regular_columns = std::invoke([&] () -> const std::vector<managed_bytes_opt>* {
        if (row) {
            return &row->cells;
        } else {
            fake_static_and_regular_columns.resize(selection->get_column_count());
            return &fake_static_and_regular_columns;
        }
    });

    auto inputs = expr::evaluation_inputs{
        .static_and_regular_columns = *static_and_regular_columns,
        .selection = selection,
        .options = &options,
    };

    static auto true_value = raw_value::make_value(data_value(true).serialize());
    return expr::evaluate(_condition, inputs) == true_value;
}

std::vector<mutation> modification_statement::apply_updates(
        const std::vector<dht::partition_range>& keys,
        const std::vector<query::clustering_range>& ranges,
        const update_parameters& params,
        const json_cache_opt& json_cache) const {

    std::vector<mutation> mutations;
    mutations.reserve(keys.size());
    for (auto key : keys) {
        // We know key.start() must be defined since we only allow EQ relations on the partition key.
        mutations.emplace_back(s, std::move(*key.start()->value().key()));
        auto& m = mutations.back();
        for (auto&& r : ranges) {
            this->add_update_for_key(m, r, params, json_cache);
        }
    }
    return mutations;
}

lw_shared_ptr<query::read_command>
modification_statement::read_command(query_processor& qp, query::clustering_row_ranges ranges, db::consistency_level cl) const {
    try {
        validate_for_read(cl);
    } catch (exceptions::invalid_request_exception& e) {
        throw exceptions::invalid_request_exception(format("Write operation require a read but consistency {} is not supported on reads", cl));
    }
    query::partition_slice ps(std::move(ranges), *s, columns_to_read(), update_parameters::options);
    const auto max_result_size = qp.proxy().get_max_result_size(ps);
    return make_lw_shared<query::read_command>(s->id(), s->version(), std::move(ps), query::max_result_size(max_result_size), query::tombstone_limit::max);
}

std::vector<query::clustering_range>
modification_statement::create_clustering_ranges(const query_options& options, const json_cache_opt& json_cache) const {
    return _restrictions->get_clustering_bounds(options);
}

dht::partition_range_vector
modification_statement::build_partition_keys(const query_options& options, const json_cache_opt& json_cache) const {
    auto keys = _restrictions->get_partition_key_ranges(options);
    for (auto const& k : keys) {
        validation::validate_cql_key(*s, *k.start()->value().key());
    }
    return keys;
}

struct modification_statement_executor {
    static auto get() { return &modification_statement::do_execute; }
};
static thread_local inheriting_concrete_execution_stage<
        future<::shared_ptr<cql_transport::messages::result_message>>,
        const modification_statement*,
        query_processor&,
        service::query_state&,
        const query_options&> modify_stage{"cql3_modification", modification_statement_executor::get()};

future<::shared_ptr<cql_transport::messages::result_message>>
modification_statement::execute(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const {
    return execute_without_checking_exception_message(qp, qs, options, std::move(guard))
            .then(cql_transport::messages::propagate_exception_as_future<shared_ptr<cql_transport::messages::result_message>>);
}

future<::shared_ptr<cql_transport::messages::result_message>>
modification_statement::execute_without_checking_exception_message(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const {
    cql3::util::validate_timestamp(qp.db().get_config(), options, attrs);
    return modify_stage(this, seastar::ref(qp), seastar::ref(qs), seastar::cref(options));
}

future<::shared_ptr<cql_transport::messages::result_message>>
modification_statement::do_execute(query_processor& qp, service::query_state& qs, const query_options& options) const {
    (void)validation::validate_column_family(qp.db(), keyspace(), column_family());

    tracing::add_table_name(qs.get_trace_state(), keyspace(), column_family());

    inc_cql_stats(qs.get_client_state().is_internal());

    _restrictions->validate_primary_key(options);

    if (has_conditions()) {
        co_return co_await execute_with_condition(qp, qs, options);
    }

    json_cache_opt json_cache = maybe_prepare_json_cache(options);
    std::vector<dht::partition_range> keys = build_partition_keys(options, json_cache);

    bool keys_size_one = keys.size() == 1;
    auto token = dht::token();
    if (keys_size_one) {
        token = keys[0].start()->value().token();
    } 

    auto res = co_await execute_without_condition(qp, qs, options, json_cache, std::move(keys));
    
    if (!res) {
        co_return seastar::make_shared<cql_transport::messages::result_message::exception>(std::move(res).assume_error());
    }

    auto result = seastar::make_shared<cql_transport::messages::result_message::void_message>();
    if (keys_size_one) {
        auto&& table = s->table();
        if (_may_use_token_aware_routing && table.uses_tablets() && qs.get_client_state().is_protocol_extension_set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V1)) {
            auto erm = table.get_effective_replication_map();
            auto tablet_info = erm->check_locality(token);
            if (tablet_info.has_value()) {
                result->add_tablet_info(tablet_info->tablet_replicas, tablet_info->token_range);
            }
        }
    }

    co_return std::move(result);
}

future<coordinator_result<>>
modification_statement::execute_without_condition(query_processor& qp, service::query_state& qs, const query_options& options, json_cache_opt& json_cache, std::vector<dht::partition_range> keys) const {
    auto cl = options.get_consistency();
    auto timeout = db::timeout_clock::now() + get_timeout(qs.get_client_state(), options);
    return get_mutations(qp, options, timeout, false, options.get_timestamp(qs), qs, json_cache, std::move(keys)).then([this, cl, timeout, &qp, &qs] (auto mutations) {
        if (mutations.empty()) {
            return make_ready_future<coordinator_result<>>(bo::success());
        }
        
        return qp.proxy().mutate_with_triggers(std::move(mutations), cl, timeout, false, qs.get_trace_state(), qs.get_permit(), db::allow_per_partition_rate_limit::yes, this->is_raw_counter_shard_write());
    });
}

future<::shared_ptr<cql_transport::messages::result_message>>
modification_statement::execute_with_condition(query_processor& qp, service::query_state& qs, const query_options& options) const {

    auto cl_for_learn = options.get_consistency();
    auto cl_for_paxos = options.check_serial_consistency();
    db::timeout_clock::time_point now = db::timeout_clock::now();
    const timeout_config& cfg = qs.get_client_state().get_timeout_config();

    auto statement_timeout = now + cfg.write_timeout; // All CAS networking operations run with write timeout.
    auto cas_timeout = now + cfg.cas_timeout;         // When to give up due to contention.
    auto read_timeout = now + cfg.read_timeout;       // When to give up on query.

    json_cache_opt json_cache = maybe_prepare_json_cache(options);
    std::vector<dht::partition_range> keys = build_partition_keys(options, json_cache);
    std::vector<query::clustering_range> ranges = create_clustering_ranges(options, json_cache);

    if (keys.empty()) {
        throw exceptions::invalid_request_exception(format("Unrestricted partition key in a conditional {}",
                    type.is_update() ? "update" : "deletion"));
    }
    if (ranges.empty()) {
        throw exceptions::invalid_request_exception(format("Unrestricted clustering key in a conditional {}",
                    type.is_update() ? "update" : "deletion"));
    }

    auto request = seastar::make_shared<cas_request>(s, std::move(keys));
    // cas_request can be used for batches as well single statements; Here we have just a single
    // modification in the list of CAS commands, since we're handling single-statement execution.
    request->add_row_update(*this, std::move(ranges), std::move(json_cache), options);

    auto token = request->key()[0].start()->value().as_decorated_key().token();

    auto shard = service::storage_proxy::cas_shard(*s, token);
    if (shard != this_shard_id()) {
        return make_ready_future<shared_ptr<cql_transport::messages::result_message>>(
                qp.bounce_to_shard(shard, std::move(const_cast<cql3::query_options&>(options).take_cached_pk_function_calls()))
            );
    }

    std::optional<locator::tablet_routing_info> tablet_info = locator::tablet_routing_info{locator::tablet_replica_set(), std::pair<dht::token, dht::token>()};

    auto&& table = s->table();
    if (_may_use_token_aware_routing && table.uses_tablets() && qs.get_client_state().is_protocol_extension_set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V1)) {
        auto erm = table.get_effective_replication_map();
        tablet_info = erm->check_locality(token);
    }

    return qp.proxy().cas(s, request, request->read_command(qp), request->key(),
            {read_timeout, qs.get_permit(), qs.get_client_state(), qs.get_trace_state()},
            cl_for_paxos, cl_for_learn, statement_timeout, cas_timeout).then([this, request, tablet_replicas = std::move(tablet_info->tablet_replicas), token_range = tablet_info->token_range] (bool is_applied) {
        auto result = request->build_cas_result_set(_metadata, _columns_of_cas_result_set, is_applied);
        result->add_tablet_info(tablet_replicas, token_range);
        return result;
    });
}

void modification_statement::build_cas_result_set_metadata() {

    std::vector<lw_shared_ptr<column_specification>> columns;
    // Add the mandatory [applied] column to result set metadata
    auto applied = make_lw_shared<cql3::column_specification>(s->ks_name(), s->cf_name(),
            make_shared<cql3::column_identifier>("[applied]", false), boolean_type);

    columns.push_back(applied);

    const auto& all_columns = s->all_columns();
    if (_if_exists || _if_not_exists) {
        // If all our conditions are columns conditions (IF x = ?), then it's enough to query
        // the columns from the conditions. If we have a IF EXISTS or IF NOT EXISTS however,
        // we need to query all columns for the row since if the condition fails, we want to
        // return everything to the user.
        // XXX Static columns make this a bit more complex, in that if an insert only static
        // columns, then the existence condition applies only to the static columns themselves, and
        // so we don't want to include regular columns in that case.
        for (const auto& def : all_columns) {
            _columns_of_cas_result_set.set(def.ordinal_id);
        }
    } else {
        expr::for_each_expression<expr::column_value>(_condition, [&] (const expr::column_value& col) {
            _columns_of_cas_result_set.set(col.col->ordinal_id);
        });
    }
    columns.reserve(columns.size() + all_columns.size());
    // We must filter conditions using the _columns_of_cas_result_set, since
    // the same column can be used twice in the condition list:
    // if a > 0 and a < 3.
    for (const auto& def : all_columns) {
        if (_columns_of_cas_result_set.test(def.ordinal_id)) {
            columns.emplace_back(def.column_specification);
        }
    }
    // Ensure we prefetch all of the columns of the result set. This is also
    // necessary to check conditions.
    _columns_to_read.union_with(_columns_of_cas_result_set);
    _metadata = seastar::make_shared<cql3::metadata>(std::move(columns));
}

void
modification_statement::process_where_clause(data_dictionary::database db, expr::expression where_clause, prepare_context& ctx) {
    _restrictions = restrictions::statement_restrictions(db, s, type, where_clause, ctx,
            applies_only_to_static_columns(), _selects_a_collection, false);
    /*
     * If there's no clustering columns restriction, we may assume that EXISTS
     * check only selects static columns and hence we can use any row from the
     * partition to check conditions.
     */
    if (_if_exists || _if_not_exists) {
        SCYLLA_ASSERT(!_has_static_column_conditions && !_has_regular_column_conditions);
        if (s->has_static_columns() && !_restrictions->has_clustering_columns_restriction()) {
            _has_static_column_conditions = true;
        } else {
            _has_regular_column_conditions = true;
        }
    }
    if (_restrictions->has_token_restrictions()) {
        throw exceptions::invalid_request_exception(format("The token function cannot be used in WHERE clauses for UPDATE and DELETE statements: {}",
                to_string(_restrictions->get_partition_key_restrictions())));
    }
    if (!_restrictions->get_non_pk_restriction().empty()) {
        auto column_names = fmt::join(_restrictions->get_non_pk_restriction()
                                         | boost::adaptors::map_keys
                                         | boost::adaptors::indirected
                                         | boost::adaptors::transformed(std::mem_fn(&column_definition::name_as_text)), ", ");
        throw exceptions::invalid_request_exception(format("Invalid where clause contains non PRIMARY KEY columns: {}", column_names));
    }
    const expr::expression& ck_restrictions = _restrictions->get_clustering_columns_restrictions();
    if (has_slice(ck_restrictions) && !allow_clustering_key_slices()) {
        throw exceptions::invalid_request_exception(
                format("Invalid operator in where clause {}", to_string(ck_restrictions)));
    }
    if (_restrictions->has_unrestricted_clustering_columns() && !applies_only_to_static_columns() && !s->is_dense()) {
        // Tomek: Origin had "&& s->comparator->is_composite()" in the condition below.
        // Comparator is a thrift concept, not CQL concept, and we want to avoid
        // using thrift concepts here. I think it's safe to drop this here because the only
        // case in which we would get a non-composite comparator here would be if the cell
        // name type is SimpleSparse, which means:
        //   (a) CQL compact table without clustering columns
        //   (b) thrift static CF with non-composite comparator
        // Those tables don't have clustering columns so we wouldn't reach this code, thus
        // the check seems redundant.
        if (require_full_clustering_key()) {
            throw exceptions::invalid_request_exception(format("Missing mandatory PRIMARY KEY part {}",
                _restrictions->unrestricted_column(column_kind::clustering_key).name_as_text()));
        }
        // In general, we can't modify specific columns if not all clustering columns have been specified.
        // However, if we modify only static columns, it's fine since we won't really use the prefix anyway.
        if (!has_slice(ck_restrictions)) {
            for (auto&& op : _column_operations) {
                if (!op->column.is_static()) {
                    throw exceptions::invalid_request_exception(format("Primary key column '{}' must be specified in order to modify column '{}'",
                        _restrictions->unrestricted_column(column_kind::clustering_key).name_as_text(), op->column.name_as_text()));
                }
            }
        }
    }
    if (_restrictions->has_partition_key_unrestricted_components()) {
        throw exceptions::invalid_request_exception(format("Missing mandatory PRIMARY KEY part {}",
            _restrictions->unrestricted_column(column_kind::partition_key).name_as_text()));
    }
    if (has_conditions()) {
        validate_where_clause_for_conditions();
    }
}

::shared_ptr<strongly_consistent_modification_statement>
modification_statement::prepare_for_broadcast_tables() const {
    // FIXME: implement for every type of `modification_statement`.
    throw service::broadcast_tables::unsupported_operation_error{};
}

namespace raw {

::shared_ptr<cql_statement_opt_metadata>
modification_statement::prepare_statement(data_dictionary::database db, prepare_context& ctx, cql_stats& stats) {
    ::shared_ptr<cql3::statements::modification_statement> statement = prepare(db, ctx, stats);

    if (service::broadcast_tables::is_broadcast_table_statement(keyspace(), column_family())) {
        return statement->prepare_for_broadcast_tables();
    } else {
        return statement;
    }
}

std::unique_ptr<prepared_statement>
modification_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());
    auto meta = get_prepare_context();
    auto statement = prepare_statement(db, meta, stats);
    auto partition_key_bind_indices = meta.get_partition_key_bind_indexes(*schema);
    return std::make_unique<prepared_statement>(std::move(statement), meta, std::move(partition_key_bind_indices));
}

::shared_ptr<cql3::statements::modification_statement>
modification_statement::prepare(data_dictionary::database db, prepare_context& ctx, cql_stats& stats) const {
    schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());

    auto prepared_attributes = _attrs->prepare(db, keyspace(), column_family());
    prepared_attributes->fill_prepare_context(ctx);

    auto prepared_stmt = prepare_internal(db, schema, ctx, std::move(prepared_attributes), stats);
    // At this point the prepare context instance should have a list of
    // `function_call` AST nodes corresponding to non-pure functions that
    // evaluate partition key constraints.
    //
    // These calls can affect partition key ranges computation and target shard
    // selection for LWT statements.
    // For such cases we need to forward the computed execution result of the
    // function when redirecting the query execution to another shard.
    // Otherwise, it's possible that we end up bouncing indefinitely between
    // various shards when evaluating a non-deterministic function each time on
    // each shard.
    //
    // Prepare context is used to keep track of such AST nodes and also modifies
    // them to include an id, that will be used for caching the results.
    // At this point we don't yet know if it's an LWT query or not, because the
    // prepared statement object is constructed later.
    //
    // Since this cache is only meaningful for LWT queries, just clear the ids
    // if it's not a conditional statement so that the AST nodes don't
    // participate in the caching mechanism later.
    if (!prepared_stmt->has_conditions() && prepared_stmt->_restrictions.has_value()) {
        ctx.clear_pk_function_calls_cache();
    }
    prepared_stmt->_may_use_token_aware_routing = ctx.get_partition_key_bind_indexes(*schema).size() != 0;
    return prepared_stmt;
}

static
expr::expression
update_for_lwt_null_equality_rules(const expr::expression& e) {
    using namespace expr;

    return search_and_replace(e, [] (const expression& e) -> std::optional<expression> {
        if (auto* binop = as_if<binary_operator>(&e)) {
            auto new_binop = *binop;
            new_binop.null_handling = expr::null_handling_style::lwt_nulls;
            return new_binop;
        }
        return std::nullopt;
    });
}

static
expr::expression
column_condition_prepare(const expr::expression& expr, data_dictionary::database db, const sstring& keyspace, const schema& schema){
    auto prepared = expr::prepare_expression(expr, db, keyspace, &schema, make_lw_shared<column_specification>("", "", make_shared<column_identifier>("IF condition", true), boolean_type));
    expr::verify_no_aggregate_functions(prepared, "IF clause");

    expr::for_each_expression<expr::column_value>(prepared, [] (const expr::column_value& cval) {
      auto def = cval.col;
      if (def->is_primary_key()) {
        throw exceptions::invalid_request_exception(format("PRIMARY KEY column '{}' cannot have IF conditions", def->name_as_text()));
      }
    });

    // If a collection is multi-cell and not frozen, it is returned as a map even if the
    // underlying data type is "set" or "list". This is controlled by
    // partition_slice::collections_as_maps enum, which is set when preparing a read command
    // object. Representing a list as a map<timeuuid, listval> is necessary to identify the list field
    // being updated, e.g. in case of UPDATE t SET list[3] = null WHERE a = 1 IF list[3]
    // = 'key'
    //
    // We adjust for it by reinterpreting the returned value as a list, since the map
    // representation is not needed here.
    prepared = expr::adjust_for_collection_as_maps(prepared);

    prepared = expr::optimize_like(prepared);

    prepared = update_for_lwt_null_equality_rules(prepared);


    return prepared;
}


void
modification_statement::prepare_conditions(data_dictionary::database db, const schema& schema, prepare_context& ctx,
        cql3::statements::modification_statement& stmt) const
{
    if (_if_not_exists || _if_exists || _conditions) {
        if (stmt.is_counter()) {
            throw exceptions::invalid_request_exception("Conditional updates are not supported on counter tables");
        }
        if (_attrs->timestamp) {
            throw exceptions::invalid_request_exception("Cannot provide custom timestamp for conditional updates");
        }

        if (_if_not_exists) {
            // To have both 'IF NOT EXISTS' and some other conditions doesn't make sense.
            // So far this is enforced by the parser, but let's SCYLLA_ASSERT it for sanity if ever the parse changes.
            SCYLLA_ASSERT(!_conditions);
            SCYLLA_ASSERT(!_if_exists);
            stmt.set_if_not_exist_condition();
        } else if (_if_exists) {
            SCYLLA_ASSERT(!_conditions);
            SCYLLA_ASSERT(!_if_not_exists);
            stmt.set_if_exist_condition();
        } else {
            stmt._condition = column_condition_prepare(*_conditions, db, keyspace(), schema);
            expr::fill_prepare_context(stmt._condition, ctx);
            stmt.analyze_condition(stmt._condition);
        }
        stmt.build_cas_result_set_metadata();
    }
}

}  // namespace raw

void
modification_statement::validate(query_processor&, const service::client_state& state) const {
    if (has_conditions() && attrs->is_timestamp_set()) {
        throw exceptions::invalid_request_exception("Cannot provide custom timestamp for conditional updates");
    }

    if (is_counter() && attrs->is_timestamp_set() && !is_raw_counter_shard_write()) {
        throw exceptions::invalid_request_exception("Cannot provide custom timestamp for counter updates");
    }

    if (is_counter() && attrs->is_time_to_live_set()) {
        throw exceptions::invalid_request_exception("Cannot provide custom TTL for counter updates");
    }

    if (is_view()) {
        throw exceptions::invalid_request_exception("Cannot directly modify a materialized view");
    }
}

bool modification_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return keyspace() == ks_name && (!cf_name || column_family() == *cf_name);
}

void modification_statement::add_operation(::shared_ptr<operation> op) {
    if (op->column.is_static()) {
        _sets_static_columns = true;
    } else {
        _sets_regular_columns = true;
        _selects_a_collection |= op->column.type->is_collection();
    }
    if (op->requires_read()) {
        _requires_read = true;
        _columns_to_read.set(op->column.ordinal_id);
        if (op->column.type->is_collection() ) {
            auto ctype = static_pointer_cast<const collection_type_impl>(op->column.type);
            if (!ctype->is_multi_cell()) {
                throw std::logic_error(format("cannot prefetch frozen collection: {}", op->column.name_as_text()));
            }
        }
    }

    if (op->column.is_counter()) {
        auto is_raw_counter_shard_write = op->is_raw_counter_shard_write();
        if (_is_raw_counter_shard_write && _is_raw_counter_shard_write != is_raw_counter_shard_write) {
            throw exceptions::invalid_request_exception("Cannot mix regular and raw counter updates");
        }
        _is_raw_counter_shard_write = is_raw_counter_shard_write;
    }

    _column_operations.push_back(std::move(op));
}

void modification_statement::inc_cql_stats(bool is_internal) const {
    const source_selector src_sel = is_internal
            ? source_selector::INTERNAL : source_selector::USER;
    const cond_selector cond_sel = has_conditions()
            ? cond_selector::WITH_CONDITIONS : cond_selector::NO_CONDITIONS;
    ++_stats.query_cnt(src_sel, _ks_sel, cond_sel, type);
}

bool modification_statement::is_conditional() const {
    return has_conditions();
}

void modification_statement::analyze_condition(expr::expression cond) {
  expr::for_each_expression<expr::column_value>(cond, [&] (const expr::column_value& col) {
    if (col.col->is_static()) {
        _has_static_column_conditions = true;
    } else {
        _has_regular_column_conditions = true;
        _selects_a_collection |=  col.col->type->is_collection();
    }
  });
}

void modification_statement::set_if_not_exist_condition() {
    // We don't know yet if we need to select only static columns to check this
    // condition or we need regular columns as well. So we postpone setting
    // _has_regular_column_conditions/_has_static_column_conditions flag until
    // we process WHERE clause, see process_where_clause().
    _if_not_exists = true;
}

bool modification_statement::has_if_not_exist_condition() const {
    return _if_not_exists;
}

void modification_statement::set_if_exist_condition() {
    // See a comment in set_if_not_exist_condition().
    _if_exists = true;
}

bool modification_statement::has_if_exist_condition() const {
    return _if_exists;
}

void modification_statement::validate_where_clause_for_conditions() const {
    // We don't support IN for CAS operation so far
    if (_restrictions->key_is_in_relation()) {
        throw exceptions::invalid_request_exception(
                format("IN on the partition key is not supported with conditional {}",
                    type.is_update() ? "updates" : "deletions"));
    }

    if (_restrictions->clustering_key_restrictions_has_IN()) {
        throw exceptions::invalid_request_exception(
                format("IN on the clustering key columns is not supported with conditional {}",
                    type.is_update() ? "updates" : "deletions"));
    }
    if (type.is_delete() && (_restrictions->has_unrestricted_clustering_columns() ||
                !_restrictions->clustering_key_restrictions_has_only_eq())) {

        bool deletes_regular_columns = _column_operations.empty() ||
            std::any_of(_column_operations.begin(), _column_operations.end(), [] (auto&& op) {
                return !op->column.is_static();
            });
        // For example, primary key is (a, b, c), only a and b are restricted
        if (deletes_regular_columns) {
            throw exceptions::invalid_request_exception(
                    "DELETE statements must restrict all PRIMARY KEY columns with equality relations"
                    " in order to delete non static columns");
        }

        // All primary key parts must be specified, unless this statement has only static column conditions
        if (_has_regular_column_conditions) {
            throw exceptions::invalid_request_exception(
                    "DELETE statements must restrict all PRIMARY KEY columns with equality relations"
                    " in order to use IF condition on non static columns");
        }
    }
}

modification_statement::json_cache_opt modification_statement::maybe_prepare_json_cache(const query_options& options) const {
    return {};
}

const statement_type statement_type::INSERT = statement_type(statement_type::type::insert);
const statement_type statement_type::UPDATE = statement_type(statement_type::type::update);
const statement_type statement_type::DELETE = statement_type(statement_type::type::del);
const statement_type statement_type::SELECT = statement_type(statement_type::type::select);

namespace raw {

modification_statement::modification_statement(cf_name name, std::unique_ptr<attributes::raw> attrs, std::optional<expr::expression> conditions, bool if_not_exists, bool if_exists)
    : cf_statement{std::move(name)}
    , _attrs{std::move(attrs)}
    , _conditions{std::move(conditions)}
    , _if_not_exists{if_not_exists}
    , _if_exists{if_exists}
{ }

}

}

}
