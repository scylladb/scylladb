/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "cql3/query_processor.hh"

#include <seastar/core/metrics.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/parallel_for_each.hh>

#include "service/storage_proxy.hh"
#include "service/migration_manager.hh"
#include "service/mapreduce_service.hh"
#include "service/raft/raft_group0_client.hh"
#include "service/storage_service.hh"
#include "cql3/CqlParser.hpp"
#include "cql3/statements/batch_statement.hh"
#include "cql3/statements/modification_statement.hh"
#include "cql3/util.hh"
#include "cql3/untyped_result_set.hh"
#include "db/config.hh"
#include "data_dictionary/data_dictionary.hh"
#include "utils/hashers.hh"
#include "utils/error_injection.hh"
#include "service/migration_manager.hh"

namespace cql3 {

using namespace statements;
using namespace cql_transport::messages;

logging::logger log("query_processor");
logging::logger prep_cache_log("prepared_statements_cache");
logging::logger authorized_prepared_statements_cache_log("authorized_prepared_statements_cache");

const sstring query_processor::CQL_VERSION = "3.3.1";

const std::chrono::minutes prepared_statements_cache::entry_expiry = std::chrono::minutes(60);

struct query_processor::remote {
    remote(service::migration_manager& mm, service::mapreduce_service& fwd,
           service::storage_service& ss, service::raft_group0_client& group0_client)
            : mm(mm), mapreducer(fwd), ss(ss), group0_client(group0_client) {}

    service::migration_manager& mm;
    service::mapreduce_service& mapreducer;
    service::storage_service& ss;
    service::raft_group0_client& group0_client;

    seastar::gate gate;
};

bool query_processor::topology_global_queue_empty() {
    return remote().first.get().ss.topology_global_queue_empty();
}

static service::query_state query_state_for_internal_call() {
    return {service::client_state::for_internal_calls(), empty_service_permit()};
}

query_processor::query_processor(service::storage_proxy& proxy, data_dictionary::database db, service::migration_notifier& mn, query_processor::memory_config mcfg, cql_config& cql_cfg, utils::loading_cache_config auth_prep_cache_cfg, lang::manager& langm)
        : _migration_subscriber{std::make_unique<migration_subscriber>(this)}
        , _proxy(proxy)
        , _db(db)
        , _mnotifier(mn)
        , _mcfg(mcfg)
        , _cql_config(cql_cfg)
        , _prepared_cache(prep_cache_log, _mcfg.prepared_statment_cache_size)
        , _authorized_prepared_cache(std::move(auth_prep_cache_cfg), authorized_prepared_statements_cache_log)
        , _auth_prepared_cache_cfg_cb([this] (uint32_t) { (void) _authorized_prepared_cache_config_action.trigger_later(); })
        , _authorized_prepared_cache_config_action([this] { update_authorized_prepared_cache_config(); return make_ready_future<>(); })
        , _authorized_prepared_cache_update_interval_in_ms_observer(_db.get_config().permissions_update_interval_in_ms.observe(_auth_prepared_cache_cfg_cb))
        , _authorized_prepared_cache_validity_in_ms_observer(_db.get_config().permissions_validity_in_ms.observe(_auth_prepared_cache_cfg_cb))
        , _lang_manager(langm)
        {
    namespace sm = seastar::metrics;
    namespace stm = statements;
    using clevel = db::consistency_level;
    sm::label cl_label("consistency_level");

    sm::label who_label("who");  // Who queried system tables
    const auto user_who_label_instance = who_label("user");
    const auto internal_who_label_instance = who_label("internal");

    sm::label ks_label("ks");
    const auto system_ks_label_instance = ks_label("system");

    std::vector<sm::metric_definition> qp_group;
    qp_group.push_back(sm::make_counter(
        "statements_prepared",
        _stats.prepare_invocations,
        sm::description("Counts the total number of parsed CQL requests.")));
    for (auto cl = size_t(clevel::MIN_VALUE); cl <= size_t(clevel::MAX_VALUE); ++cl) {
        qp_group.push_back(
            sm::make_counter(
                "queries",
                _stats.queries_by_cl[cl],
                sm::description("Counts queries by consistency level."),
                {cl_label(clevel(cl))}));
    }
    _metrics.add_group("query_processor", qp_group);

    sm::label cas_label("conditional");
    auto cas_label_instance = cas_label("yes");
    auto non_cas_label_instance = cas_label("no");

    _metrics.add_group(
            "cql",
            {
                    sm::make_counter(
                            "reads",
                            sm::description("Counts the total number of CQL SELECT requests."),
                            [this] {
                                // Reads fall into `cond_selector::NO_CONDITIONS' pigeonhole
                                return _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::SELECT)
                                        + _cql_stats.query_cnt(source_selector::USER, ks_selector::NONSYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::SELECT)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::SELECT)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::NONSYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::SELECT);
                            }),

                    sm::make_counter(
                            "inserts",
                            sm::description("Counts the total number of CQL INSERT requests with/without conditions."),
                            {non_cas_label_instance},
                            [this] {
                                return _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::INSERT)
                                        + _cql_stats.query_cnt(source_selector::USER, ks_selector::NONSYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::INSERT)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::INSERT)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::NONSYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::INSERT);
                            }),
                    sm::make_counter(
                            "inserts",
                            sm::description("Counts the total number of CQL INSERT requests with/without conditions."),
                            {cas_label_instance},
                            [this] {
                                return _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::INSERT)
                                        + _cql_stats.query_cnt(source_selector::USER, ks_selector::NONSYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::INSERT)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::INSERT)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::NONSYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::INSERT);
                            }).set_skip_when_empty(),

                    sm::make_counter(
                            "updates",
                            sm::description("Counts the total number of CQL UPDATE requests with/without conditions."),
                            {non_cas_label_instance},
                            [this] {
                                return _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::UPDATE)
                                        + _cql_stats.query_cnt(source_selector::USER, ks_selector::NONSYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::UPDATE)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::UPDATE)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::NONSYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::UPDATE);
                            }),
                    sm::make_counter(
                            "updates",
                            sm::description("Counts the total number of CQL UPDATE requests with/without conditions."),
                            {cas_label_instance},
                            [this] {
                                return _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::UPDATE)
                                        + _cql_stats.query_cnt(source_selector::USER, ks_selector::NONSYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::UPDATE)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::UPDATE)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::NONSYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::UPDATE);
                            }).set_skip_when_empty(),

                    sm::make_counter(
                            "deletes",
                            sm::description("Counts the total number of CQL DELETE requests with/without conditions."),
                            {non_cas_label_instance},
                            [this] {
                                return _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::DELETE)
                                        + _cql_stats.query_cnt(source_selector::USER, ks_selector::NONSYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::DELETE)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::DELETE)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::NONSYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::DELETE);
                            }),
                    sm::make_counter(
                            "deletes",
                            sm::description("Counts the total number of CQL DELETE requests with/without conditions."),
                            {cas_label_instance},
                            [this] {
                                return _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::DELETE)
                                        + _cql_stats.query_cnt(source_selector::USER, ks_selector::NONSYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::DELETE)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::DELETE)
                                        + _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::NONSYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::DELETE);
                            }).set_skip_when_empty(),

                    sm::make_counter(
                            "reads_per_ks",
                            // Reads fall into `cond_selector::NO_CONDITIONS' pigeonhole
                            _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::SELECT),
                            sm::description("Counts the number of CQL SELECT requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {user_who_label_instance, system_ks_label_instance}),
                    sm::make_counter(
                            "reads_per_ks",
                            _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::SELECT),
                            sm::description("Counts the number of CQL SELECT requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {internal_who_label_instance, system_ks_label_instance}),

                    sm::make_counter(
                            "inserts_per_ks",
                            _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::INSERT),
                            sm::description("Counts the number of CQL INSERT requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)."),
                            {user_who_label_instance, system_ks_label_instance, non_cas_label_instance}),
                    sm::make_counter(
                            "inserts_per_ks",
                            _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::INSERT),
                            sm::description("Counts the number of CQL INSERT requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)."),
                            {internal_who_label_instance, system_ks_label_instance, non_cas_label_instance}),
                    sm::make_counter(
                            "inserts_per_ks",
                            _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::INSERT),
                            sm::description("Counts the number of CQL INSERT requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)."),
                            {user_who_label_instance, system_ks_label_instance, cas_label_instance}).set_skip_when_empty(),
                    sm::make_counter(
                            "inserts_per_ks",
                            _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::INSERT),
                            sm::description("Counts the number of CQL INSERT requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)."),
                            {internal_who_label_instance, system_ks_label_instance, cas_label_instance}).set_skip_when_empty(),

                    sm::make_counter(
                            "updates_per_ks",
                            _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::UPDATE),
                            sm::description("Counts the number of CQL UPDATE requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {user_who_label_instance, system_ks_label_instance, non_cas_label_instance}),
                    sm::make_counter(
                            "updates_per_ks",
                            _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::UPDATE),
                            sm::description("Counts the number of CQL UPDATE requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {internal_who_label_instance, system_ks_label_instance, non_cas_label_instance}),
                    sm::make_counter(
                            "updates_per_ks",
                            _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::UPDATE),
                            sm::description("Counts the number of CQL UPDATE requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {user_who_label_instance, system_ks_label_instance, cas_label_instance}).set_skip_when_empty(),
                    sm::make_counter(
                            "updates_per_ks",
                            _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::UPDATE),
                            sm::description("Counts the number of CQL UPDATE requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {internal_who_label_instance, system_ks_label_instance, cas_label_instance}).set_skip_when_empty(),

                    sm::make_counter(
                            "deletes_per_ks",
                            _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::DELETE),
                            sm::description("Counts the number of CQL DELETE requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {user_who_label_instance, system_ks_label_instance, non_cas_label_instance}),
                    sm::make_counter(
                            "deletes_per_ks",
                            _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::NO_CONDITIONS, stm::statement_type::DELETE),
                            sm::description("Counts the number of CQL DELETE requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {internal_who_label_instance, system_ks_label_instance, non_cas_label_instance}),
                    sm::make_counter(
                            "deletes_per_ks",
                            _cql_stats.query_cnt(source_selector::USER, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::DELETE),
                            sm::description("Counts the number of CQL DELETE requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {user_who_label_instance, system_ks_label_instance, cas_label_instance}).set_skip_when_empty(),
                    sm::make_counter(
                            "deletes_per_ks",
                            _cql_stats.query_cnt(source_selector::INTERNAL, ks_selector::SYSTEM, cond_selector::WITH_CONDITIONS, stm::statement_type::DELETE),
                            sm::description("Counts the number of CQL DELETE requests executed on particular keyspaces. "
                                            "Label `who' indicates where the reqs come from (clients or DB internals)"),
                            {internal_who_label_instance, system_ks_label_instance, cas_label_instance}).set_skip_when_empty(),

                    sm::make_counter(
                            "batches",
                            _cql_stats.batches,
                            sm::description("Counts the total number of CQL BATCH requests without conditions."),
                            {non_cas_label_instance}),

                    sm::make_counter(
                            "batches",
                            _cql_stats.cas_batches,
                            sm::description("Counts the total number of CQL BATCH requests with conditions."),
                            {cas_label_instance}),

                    sm::make_counter(
                            "statements_in_batches",
                            _cql_stats.statements_in_batches,
                            sm::description("Counts the total number of sub-statements in CQL BATCH requests without conditions."),
                            {non_cas_label_instance}),

                    sm::make_counter(
                            "statements_in_batches",
                            _cql_stats.statements_in_cas_batches,
                            sm::description("Counts the total number of sub-statements in CQL BATCH requests with conditions."),
                            {cas_label_instance}),

                    sm::make_counter(
                            "batches_pure_logged",
                            _cql_stats.batches_pure_logged,
                            sm::description(
                                    "Counts the total number of LOGGED batches that were executed as LOGGED batches.")),

                    sm::make_counter(
                            "batches_pure_unlogged",
                            _cql_stats.batches_pure_unlogged,
                            sm::description(
                                    "Counts the total number of UNLOGGED batches that were executed as UNLOGGED "
                                    "batches.")),

                    sm::make_counter(
                            "batches_unlogged_from_logged",
                            _cql_stats.batches_unlogged_from_logged,
                            sm::description("Counts the total number of LOGGED batches that were executed as UNLOGGED "
                                            "batches.")),

                    sm::make_counter(
                            "rows_read",
                            _cql_stats.rows_read,
                            sm::description("Counts the total number of rows read during CQL requests.")),

                    sm::make_counter(
                            "prepared_cache_evictions",
                            [] { return prepared_statements_cache::shard_stats().prepared_cache_evictions; },
                            sm::description("Counts the number of prepared statements cache entries evictions.")),

                    sm::make_counter(
                            "unprivileged_entries_evictions_on_size",
                            [] { return prepared_statements_cache::shard_stats().unprivileged_entries_evictions_on_size; },
                            sm::description("Counts a number of evictions of prepared statements from the prepared statements cache after they have been used only once. An increasing counter suggests the user may be preparing a different statement for each request instead of reusing the same prepared statement with parameters.")),

                    sm::make_gauge(
                            "prepared_cache_size",
                            [this] { return _prepared_cache.size(); },
                            sm::description("A number of entries in the prepared statements cache.")),

                    sm::make_gauge(
                            "prepared_cache_memory_footprint",
                            [this] { return _prepared_cache.memory_footprint(); },
                            sm::description("Size (in bytes) of the prepared statements cache.")),

                    sm::make_counter(
                            "secondary_index_creates",
                            _cql_stats.secondary_index_creates,
                            sm::description("Counts the total number of CQL CREATE INDEX requests.")),

                    sm::make_counter(
                            "secondary_index_drops",
                            _cql_stats.secondary_index_drops,
                            sm::description("Counts the total number of CQL DROP INDEX requests.")),

                    // secondary_index_reads total count is also included in all cql reads
                    sm::make_counter(
                            "secondary_index_reads",
                            _cql_stats.secondary_index_reads,
                            sm::description("Counts the total number of CQL read requests performed using secondary indexes.")),

                    // secondary_index_rows_read total count is also included in all cql rows read
                    sm::make_counter(
                            "secondary_index_rows_read",
                            _cql_stats.secondary_index_rows_read,
                            sm::description("Counts the total number of rows read during CQL requests performed using secondary indexes.")),

                    // read requests that required ALLOW FILTERING
                    sm::make_counter(
                            "filtered_read_requests",
                            _cql_stats.filtered_reads,
                            sm::description("Counts the total number of CQL read requests that required ALLOW FILTERING. See filtered_rows_read_total to compare how many rows needed to be filtered.")),

                    // rows read with filtering enabled (because ALLOW FILTERING was required)
                    sm::make_counter(
                            "filtered_rows_read_total",
                            _cql_stats.filtered_rows_read_total,
                            sm::description("Counts the total number of rows read during CQL requests that required ALLOW FILTERING. See filtered_rows_matched_total and filtered_rows_dropped_total for information how accurate filtering queries are.")),

                    // rows read with filtering enabled and accepted by the filter
                    sm::make_counter(
                            "filtered_rows_matched_total",
                            _cql_stats.filtered_rows_matched_total,
                            sm::description("Counts the number of rows read during CQL requests that required ALLOW FILTERING and accepted by the filter. Number similar to filtered_rows_read_total indicates that filtering is accurate.")),

                    // rows read with filtering enabled and rejected by the filter
                    sm::make_counter(
                            "filtered_rows_dropped_total",
                            [this]() {return _cql_stats.filtered_rows_read_total - _cql_stats.filtered_rows_matched_total;},
                            sm::description("Counts the number of rows read during CQL requests that required ALLOW FILTERING and dropped by the filter. Number similar to filtered_rows_read_total indicates that filtering is not accurate and might cause performance degradation.")),

                    sm::make_counter(
                            "select_bypass_caches",
                            _cql_stats.select_bypass_caches,
                            sm::description("Counts the number of SELECT query executions with BYPASS CACHE option.")),

                    sm::make_counter(
                            "select_allow_filtering",
                            _cql_stats.select_allow_filtering,
                            sm::description("Counts the number of SELECT query executions with ALLOW FILTERING option.")),

                    sm::make_counter(
                            "select_partition_range_scan",
                            _cql_stats.select_partition_range_scan,
                            sm::description("Counts the number of SELECT query executions requiring partition range scan.")),

                    sm::make_counter(
                            "select_partition_range_scan_no_bypass_cache",
                            _cql_stats.select_partition_range_scan_no_bypass_cache,
                            sm::description("Counts the number of SELECT query executions requiring partition range scan without BYPASS CACHE option.")),

                    sm::make_counter(
                            "select_parallelized",
                            _cql_stats.select_parallelized,
                            sm::description("Counts the number of parallelized aggregation SELECT query executions.")),

                    sm::make_counter(
                            "authorized_prepared_statements_cache_evictions",
                            [] { return authorized_prepared_statements_cache::shard_stats().authorized_prepared_statements_cache_evictions; },
                            sm::description("Counts the number of authenticated prepared statements cache entries evictions.")),

                    sm::make_counter(
                            "authorized_prepared_statements_privileged_entries_evictions_on_size",
                            [] { return authorized_prepared_statements_cache::shard_stats().authorized_prepared_statements_privileged_entries_evictions_on_size; },
                            sm::description("Counts a number of evictions of prepared statements from the authorized prepared statements cache after they have been used more than once.")),

                    sm::make_counter(
                            "authorized_prepared_statements_unprivileged_entries_evictions_on_size",
                            [] { return authorized_prepared_statements_cache::shard_stats().authorized_prepared_statements_unprivileged_entries_evictions_on_size; },
                            sm::description("Counts a number of evictions of prepared statements from the authorized prepared statements cache after they have been used only once. An increasing counter suggests the user may be preparing a different statement for each request instead of reusing the same prepared statement with parameters.")),

                    sm::make_gauge(
                            "authorized_prepared_statements_cache_size",
                            [this] { return _authorized_prepared_cache.size(); },
                            sm::description("Number of entries in the authenticated prepared statements cache.")),

                    sm::make_gauge(
                            "user_prepared_auth_cache_footprint",
                            [this] { return _authorized_prepared_cache.memory_footprint(); },
                            sm::description("Size (in bytes) of the authenticated prepared statements cache.")),

                    sm::make_counter(
                            "reverse_queries",
                            _cql_stats.reverse_queries,
                            sm::description("Counts the number of CQL SELECT requests with reverse ORDER BY order.")),

                    sm::make_counter(
                            "unpaged_select_queries",
                            [this] {
                                return _cql_stats.unpaged_select_queries(ks_selector::NONSYSTEM)
                                        + _cql_stats.unpaged_select_queries(ks_selector::SYSTEM);
                            },
                            sm::description("Counts the total number of unpaged CQL SELECT requests.")),

                    sm::make_counter(
                            "unpaged_select_queries_per_ks",
                            _cql_stats.unpaged_select_queries(ks_selector::SYSTEM),
                            sm::description("Counts the number of unpaged CQL SELECT requests against particular keyspaces."),
                            {system_ks_label_instance}),

                    sm::make_counter(
                            "minimum_replication_factor_fail_violations",
                            _cql_stats.minimum_replication_factor_fail_violations,
                            sm::description("Counts the number of minimum_replication_factor_fail_threshold guardrail violations, "
                                            "i.e. attempts to create a keyspace with RF on one of the DCs below the set guardrail.")),

                    sm::make_counter(
                            "minimum_replication_factor_warn_violations",
                            _cql_stats.minimum_replication_factor_warn_violations,
                            sm::description("Counts the number of minimum_replication_factor_warn_threshold guardrail violations, "
                                            "i.e. attempts to create a keyspace with RF on one of the DCs below the set guardrail.")),

                    sm::make_counter(
                            "maximum_replication_factor_warn_violations",
                            _cql_stats.maximum_replication_factor_warn_violations,
                            sm::description("Counts the number of maximum_replication_factor_warn_threshold guardrail violations, "
                                            "i.e. attempts to create a keyspace with RF on one of the DCs above the set guardrail.")),

                    sm::make_counter(
                            "maximum_replication_factor_fail_violations",
                            _cql_stats.maximum_replication_factor_fail_violations,
                            sm::description("Counts the number of maximum_replication_factor_fail_threshold guardrail violations, "
                                            "i.e. attempts to create a keyspace with RF on one of the DCs above the set guardrail.")),

                    sm::make_counter(
                            "replication_strategy_warn_list_violations",
                            _cql_stats.replication_strategy_warn_list_violations,
                            sm::description("Counts the number of replication_strategy_warn_list guardrail violations, "
                                            "i.e. attempts to set a discouraged replication strategy in a keyspace via CREATE/ALTER KEYSPACE.")),

                    sm::make_counter(
                            "replication_strategy_fail_list_violations",
                            _cql_stats.replication_strategy_fail_list_violations,
                            sm::description("Counts the number of replication_strategy_fail_list guardrail violations, "
                                            "i.e. attempts to set a forbidden replication strategy in a keyspace via CREATE/ALTER KEYSPACE.")),
            });

    _mnotifier.register_listener(_migration_subscriber.get());
}

query_processor::~query_processor() {
    if (_remote) {
        on_internal_error_noexcept(log, "`remote` not stopped before `query_processor` destruction");
    }
}

void query_processor::start_remote(service::migration_manager& mm, service::mapreduce_service& mapreducer,
                                   service::storage_service& ss, service::raft_group0_client& group0_client) {
    _remote = std::make_unique<struct remote>(mm, mapreducer, ss, group0_client);
}

future<> query_processor::stop_remote() {
    if (!_remote) {
        on_internal_error(log, "`remote` already gone in `stop_remote()`");
    }

    co_await _remote->gate.close();
    _remote = nullptr;
}

future<> query_processor::stop() {
    co_await _mnotifier.unregister_listener(_migration_subscriber.get());
    co_await _authorized_prepared_cache.stop();
    co_await _prepared_cache.stop();
}

future<::shared_ptr<cql_transport::messages::result_message>> query_processor::execute_with_guard(
        std::function<future<::shared_ptr<cql_transport::messages::result_message>>(service::query_state&, ::shared_ptr<cql_statement>, const query_options&, std::optional<service::group0_guard>)> fn,
        ::shared_ptr<cql_statement> statement, service::query_state& query_state, const query_options& options) {
    // execute all statements that need group0 guard on shard0
    if (this_shard_id() != 0) {
        co_return ::make_shared<cql_transport::messages::result_message::bounce_to_shard>(0,
                    std::move(const_cast<cql3::query_options&>(options).take_cached_pk_function_calls()));
    }

    auto [remote_, holder] = remote();
    size_t retries = remote_.get().mm.get_concurrent_ddl_retries();
    while (true)  {
        try {
            auto guard = co_await remote_.get().mm.start_group0_operation();
            co_return co_await fn(query_state, statement, options, std::move(guard));
        } catch (const service::group0_concurrent_modification& ex) {
            log.warn("Failed to execute statement \"{}\" due to guard conflict.{}.",
                    statement->raw_cql_statement, retries ? " Retrying" : " Number of retries exceeded, giving up");
            if (retries--) {
                continue;
            }
            throw;
        }
    }
}

template<typename... Args>
future<::shared_ptr<result_message>>
query_processor::execute_maybe_with_guard(service::query_state& query_state, ::shared_ptr<cql_statement> statement, const query_options& options,
    future<::shared_ptr<result_message>>(query_processor::*fn)(service::query_state&, ::shared_ptr<cql_statement>, const query_options&, std::optional<service::group0_guard>, Args...), Args... args) {
    if (!statement->needs_guard(*this, query_state)) {
        return (this->*fn)(query_state, std::move(statement), options, std::nullopt, std::forward<Args>(args)...);
    }
    static auto exec = [fn] (query_processor& qp, Args... args, service::query_state& query_state, ::shared_ptr<cql_statement> statement, const query_options& options, std::optional<service::group0_guard> guard) {
        return (qp.*fn)(query_state, std::move(statement), options, std::move(guard), std::forward<Args>(args)...);
    };
    return execute_with_guard(std::bind_front(exec, std::ref(*this), std::forward<Args>(args)...), std::move(statement), query_state, options);
}

future<::shared_ptr<result_message>>
query_processor::execute_direct_without_checking_exception_message(const sstring_view& query_string, service::query_state& query_state, query_options& options) {
    log.trace("execute_direct: \"{}\"", query_string);
    tracing::trace(query_state.get_trace_state(), "Parsing a statement");
    auto p = get_statement(query_string, query_state.get_client_state());
    auto statement = p->statement;
    const auto warnings = std::move(p->warnings);
    if (statement->get_bound_terms() != options.get_values_count()) {
        const auto msg = format("Invalid amount of bind variables: expected {:d} received {:d}",
                statement->get_bound_terms(),
                options.get_values_count());
        throw exceptions::invalid_request_exception(msg);
    }
    options.prepare(p->bound_names);

    warn(unimplemented::cause::METRICS);
#if 0
        if (!queryState.getClientState().isInternal)
            metrics.regularStatementsExecuted.inc();
#endif
    auto user = query_state.get_client_state().user();
    tracing::trace(query_state.get_trace_state(), "Processing a statement for authenticated user: {}", user ? (user->name ? *user->name : "anonymous") : "no user authenticated");
    return execute_maybe_with_guard(query_state, std::move(statement), options, &query_processor::do_execute_direct, std::move(warnings));
}

future<::shared_ptr<result_message>>
query_processor::do_execute_direct(
         service::query_state& query_state,
        shared_ptr<cql_statement> statement,
        const query_options& options,
        std::optional<service::group0_guard> guard,
        cql3::cql_warnings_vec warnings) {
    co_await statement->check_access(*this, query_state.get_client_state());
    auto m = co_await process_authorized_statement(statement, query_state, options, std::move(guard));
    for (const auto& w : warnings) {
        m->add_warning(w);
    }
    co_return std::move(m);
}

future<::shared_ptr<result_message>>
query_processor::execute_prepared_without_checking_exception_message(
        service::query_state& query_state,
        shared_ptr<cql_statement> statement,
        const query_options& options,
        statements::prepared_statement::checked_weak_ptr prepared,
        cql3::prepared_cache_key_type cache_key,
        bool needs_authorization) {
    return execute_maybe_with_guard(query_state, std::move(statement), options, &query_processor::do_execute_prepared, std::move(prepared), std::move(cache_key), needs_authorization);
}

future<::shared_ptr<result_message>>
query_processor::do_execute_prepared(
        service::query_state& query_state,
        shared_ptr<cql_statement> statement,
        const query_options& options,
        std::optional<service::group0_guard> guard,
        statements::prepared_statement::checked_weak_ptr prepared,
        cql3::prepared_cache_key_type cache_key,
        bool needs_authorization) {
    if (needs_authorization) {
        co_await statement->check_access(*this, query_state.get_client_state());
        try {
            co_await _authorized_prepared_cache.insert(*query_state.get_client_state().user(), std::move(cache_key), std::move(prepared));
        } catch (...) {
            log.error("failed to cache the entry: {}", std::current_exception());
        }
    }
    co_return co_await process_authorized_statement(std::move(statement), query_state, options, std::move(guard));
}

future<::shared_ptr<result_message>>
query_processor::process_authorized_statement(const ::shared_ptr<cql_statement> statement, service::query_state& query_state, const query_options& options, std::optional<service::group0_guard> guard) {
    auto& client_state = query_state.get_client_state();

    ++_stats.queries_by_cl[size_t(options.get_consistency())];

    statement->validate(*this, client_state);

    auto msg = co_await statement->execute_without_checking_exception_message(*this, query_state, options, std::move(guard));

    if (msg) {
       co_return std::move(msg);
    }
    co_return ::make_shared<result_message::void_message>();
}

future<::shared_ptr<cql_transport::messages::result_message::prepared>>
query_processor::prepare(sstring query_string, service::query_state& query_state) {
    auto& client_state = query_state.get_client_state();
    return prepare(std::move(query_string), client_state);
}

future<::shared_ptr<cql_transport::messages::result_message::prepared>>
query_processor::prepare(sstring query_string, const service::client_state& client_state) {
    using namespace cql_transport::messages;
    return prepare_one<result_message::prepared::cql>(
            std::move(query_string),
            client_state,
            compute_id,
            prepared_cache_key_type::cql_id);
}

static std::string hash_target(std::string_view query_string, std::string_view keyspace) {
    std::string ret(keyspace);
    ret += query_string;
    return ret;
}

prepared_cache_key_type query_processor::compute_id(
        std::string_view query_string,
        std::string_view keyspace) {
    return prepared_cache_key_type(md5_hasher::calculate(hash_target(query_string, keyspace)));
}

std::unique_ptr<prepared_statement>
query_processor::get_statement(const sstring_view& query, const service::client_state& client_state) {
    std::unique_ptr<raw::parsed_statement> statement = parse_statement(query);

    // Set keyspace for statement that require login
    auto cf_stmt = dynamic_cast<raw::cf_statement*>(statement.get());
    if (cf_stmt) {
        cf_stmt->prepare_keyspace(client_state);
    }
    ++_stats.prepare_invocations;
    auto p = statement->prepare(_db, _cql_stats);
    p->statement->raw_cql_statement = sstring(query);
    return p;
}

std::unique_ptr<raw::parsed_statement>
query_processor::parse_statement(const sstring_view& query) {
    try {
        {
            const char* error_injection_key = "query_processor-parse_statement-test_failure";
            utils::get_local_injector().inject(error_injection_key, [&]() {
                if (query.find(error_injection_key) != sstring_view::npos) {
                    throw std::runtime_error(error_injection_key);
                }
            });
        }
        auto statement = util::do_with_parser(query,  std::mem_fn(&cql3_parser::CqlParser::query));
        if (!statement) {
            throw exceptions::syntax_exception("Parsing failed");
        }
        return statement;
    } catch (const exceptions::recognition_exception& e) {
        throw exceptions::syntax_exception(format("Invalid or malformed CQL query string: {}", e.what()));
    } catch (const exceptions::cassandra_exception& e) {
        throw;
    } catch (const std::exception& e) {
        log.error("The statement: {} could not be parsed: {}", query, e.what());
        throw exceptions::syntax_exception(format("Failed parsing statement: [{}] reason: {}", query, e.what()));
    }
}

std::vector<std::unique_ptr<raw::parsed_statement>>
query_processor::parse_statements(std::string_view queries) {
    try {
        auto statements = util::do_with_parser(queries, std::mem_fn(&cql3_parser::CqlParser::queries));
        if (statements.empty()) {
            throw exceptions::syntax_exception("Parsing failed");
        }
        return statements;
    } catch (const exceptions::recognition_exception& e) {
        throw exceptions::syntax_exception(format("Invalid or malformed CQL query string: {}", e.what()));
    } catch (const exceptions::cassandra_exception& e) {
        throw;
    } catch (const std::exception& e) {
        log.error("The statements: {} could not be parsed: {}", queries, e.what());
        throw exceptions::syntax_exception(format("Failed parsing statements: [{}] reason: {}", queries, e.what()));
    }
}

std::pair<std::reference_wrapper<struct query_processor::remote>, gate::holder> query_processor::remote() {
    if (_remote) {
        auto holder = _remote->gate.hold();
        return {*_remote, std::move(holder)};
    }

    // This error should not appear because the user should not be able to send distributed queries
    // before `remote` is initialized, and user queries should be drained before `remote` is destroyed.
    // See `storage_proxy::remote()` for a similar comment with more details.
    on_internal_error(log, "attempted to perform distributed query when `query_processor::remote` is unavailable");
}

query_options query_processor::make_internal_options(
        const statements::prepared_statement::checked_weak_ptr& p,
        const std::vector<data_value_or_unset>& values,
        db::consistency_level cl,
        int32_t page_size) const {
    if (p->bound_names.size() != values.size()) {
        throw std::invalid_argument(
                format("Invalid number of values. Expecting {:d} but got {:d}", p->bound_names.size(), values.size()));
    }
    auto ni = p->bound_names.begin();
    raw_value_vector_with_unset bound_values;
    bound_values.values.reserve(values.size());
    bound_values.unset.resize(values.size());
    for (auto& var : values) {
        auto& n = *ni;
        std::visit(overloaded_functor {
            [&] (const data_value& v) {
                if (v.type() == bytes_type) {
                    bound_values.values.emplace_back(cql3::raw_value::make_value(value_cast<bytes>(v)));
                } else if (v.is_null()) {
                    bound_values.values.emplace_back(cql3::raw_value::make_null());
                } else {
                    bound_values.values.emplace_back(cql3::raw_value::make_value(n->type->decompose(v)));
                }
            }, [&] (const unset_value&) {
                bound_values.values.emplace_back(cql3::raw_value::make_null());
                bound_values.unset[std::distance(p->bound_names.begin(), ni)] = true;
            }
        }, var);
        ++ni;
    }
    if (page_size > 0) {
        lw_shared_ptr<service::pager::paging_state> paging_state;
        db::consistency_level serial_consistency = db::consistency_level::SERIAL;
        api::timestamp_type ts = api::missing_timestamp;
        return query_options(
                cl,
                std::move(bound_values),
                cql3::query_options::specific_options{page_size, std::move(paging_state), serial_consistency, ts});
    }
    return query_options(cl, std::move(bound_values));
}

statements::prepared_statement::checked_weak_ptr query_processor::prepare_internal(const sstring& query_string) {
    auto& p = _internal_statements[query_string];
    if (p == nullptr) {
        auto np = parse_statement(query_string)->prepare(_db, _cql_stats);
        np->statement->raw_cql_statement = query_string;
        p = std::move(np); // inserts it into map
    }
    return p->checked_weak_from_this();
}

struct internal_query_state {
    sstring query_string;
    std::unique_ptr<query_options> opts;
    statements::prepared_statement::checked_weak_ptr p;
    bool more_results = true;
};

internal_query_state query_processor::create_paged_state(
        const sstring& query_string,
        db::consistency_level cl,
        const data_value_list& values,
        int32_t page_size) {
    auto p = prepare_internal(query_string);
    auto opts = make_internal_options(p, values, cl, page_size);
    return internal_query_state{query_string, std::make_unique<cql3::query_options>(std::move(opts)), std::move(p), true};
}

bool query_processor::has_more_results(cql3::internal_query_state& state) const {
    return state.more_results;
}

future<> query_processor::for_each_cql_result(
        cql3::internal_query_state& state,
        noncopyable_function<future<stop_iteration>(const cql3::untyped_result_set::row&)> f) {
    do {
        auto msg = co_await execute_paged_internal(state);
        for (auto& row : *msg) {
            if ((co_await f(row)) == stop_iteration::yes) {
                co_return;
            }
        }
    } while (has_more_results(state));
}

future<::shared_ptr<untyped_result_set>>
query_processor::execute_paged_internal(internal_query_state& state) {
    state.p->statement->validate(*this, service::client_state::for_internal_calls());
    auto qs = query_state_for_internal_call();
    ::shared_ptr<cql_transport::messages::result_message> msg =
      co_await state.p->statement->execute(*this, qs, *state.opts, std::nullopt);

    class visitor : public result_message::visitor_base {
        internal_query_state& _state;
        query_processor& _qp;
    public:
        visitor(internal_query_state& state, query_processor& qp) : _state(state), _qp(qp) {
        }
        virtual ~visitor() = default;
        void visit(const result_message::rows& rmrs) override {
            auto& rs = rmrs.rs();
            if (rs.get_metadata().paging_state()) {
                bool done = !rs.get_metadata().flags().contains<cql3::metadata::flag::HAS_MORE_PAGES>();

                if (done) {
                    _state.more_results = false;
                } else {
                    const service::pager::paging_state& st = *rs.get_metadata().paging_state();
                    lw_shared_ptr<service::pager::paging_state> shrd = make_lw_shared<service::pager::paging_state>(st);
                    _state.opts = std::make_unique<query_options>(std::move(_state.opts), shrd);
                    _state.p = _qp.prepare_internal(_state.query_string);
                }
            } else {
                _state.more_results = false;
            }
        }
    };
    visitor v(state, *this);
    if (msg != nullptr) {
        msg->accept(v);
    }

    co_return ::make_shared<untyped_result_set>(msg);
}

future<::shared_ptr<untyped_result_set>>
query_processor::execute_internal(
        const sstring& query_string,
        db::consistency_level cl,
        const data_value_list& values,
        cache_internal cache) {
    auto qs = query_state_for_internal_call();
    co_return co_await execute_internal(query_string, cl, qs, values, cache);
}

future<::shared_ptr<untyped_result_set>>
query_processor::execute_internal(
        const sstring& query_string,
        db::consistency_level cl,
        service::query_state& query_state,
        const data_value_list& values,
        cache_internal cache) {

    if (log.is_enabled(logging::log_level::trace)) {
        log.trace("execute_internal: {}\"{}\" ({})", cache ? "(cached) " : "", query_string, fmt::join(values, ", "));
    }
    if (cache) {
        auto p = prepare_internal(query_string);
        return execute_with_params(std::move(p), cl, query_state, values);
    } else {
        auto p = parse_statement(query_string)->prepare(_db, _cql_stats);
        p->statement->raw_cql_statement = query_string;
        auto checked_weak_ptr = p->checked_weak_from_this();
        return execute_with_params(std::move(checked_weak_ptr), cl, query_state, values).finally([p = std::move(p)] {});
    }
}

future<std::vector<mutation>> query_processor::get_mutations_internal(
        const sstring query_string,
        service::query_state& query_state,
        api::timestamp_type timestamp,
        std::vector<data_value_or_unset> values) {
    log.debug("get_mutations_internal: \"{}\" ({})", query_string, fmt::join(values, ", "));
    auto stmt = prepare_internal(query_string);
    auto mod_stmt = dynamic_pointer_cast<cql3::statements::modification_statement>(stmt->statement);
    if (!mod_stmt) {
        on_internal_error(log, "Only modification statement is supported in get_mutations_internal");
    }
    auto opts = make_internal_options(stmt, values, db::consistency_level::LOCAL_ONE);
    auto json_cache = mod_stmt->maybe_prepare_json_cache(opts);
    auto keys = mod_stmt->build_partition_keys(opts, json_cache);
    // timeout only applies when modification requires read
    auto timeout = db::timeout_clock::now() + query_state.get_client_state().get_timeout_config().read_timeout;
    if (mod_stmt->requires_read()) {
        on_internal_error(log, "Read-modified-write queries forbidden in get_mutations_internal");
    }
    co_return co_await mod_stmt->get_mutations(*this, opts, timeout, true, timestamp, query_state, json_cache, std::move(keys));
}

future<::shared_ptr<untyped_result_set>>
query_processor::execute_with_params(
        statements::prepared_statement::checked_weak_ptr p,
        db::consistency_level cl,
        service::query_state& query_state,
        const data_value_list& values) {
    auto opts = make_internal_options(p, values, cl);
    auto statement = p->statement;

    auto msg = co_await execute_maybe_with_guard(query_state, std::move(statement), opts, &query_processor::do_execute_with_params);
    co_return ::make_shared<untyped_result_set>(msg);
}

future<::shared_ptr<result_message>>
query_processor::do_execute_with_params(
        service::query_state& query_state,
        shared_ptr<cql_statement> statement,
        const query_options& options, std::optional<service::group0_guard> guard) {
    statement->validate(*this, service::client_state::for_internal_calls());
    co_return co_await statement->execute(*this, query_state, options, std::move(guard));
}


future<::shared_ptr<cql_transport::messages::result_message>>
query_processor::execute_batch_without_checking_exception_message(
        ::shared_ptr<statements::batch_statement> batch,
        service::query_state& query_state,
        query_options& options,
        std::unordered_map<prepared_cache_key_type, authorized_prepared_statements_cache::value_type> pending_authorization_entries) {
    co_await batch->check_access(*this, query_state.get_client_state());
    co_await coroutine::parallel_for_each(pending_authorization_entries, [this, &query_state] (auto& e) -> future<> {
            try {
                co_await _authorized_prepared_cache.insert(*query_state.get_client_state().user(), e.first, std::move(e.second));
            } catch (...) {
                log.error("failed to cache the entry: {}", std::current_exception());
            }
        });
    batch->validate();
    batch->validate(*this, query_state.get_client_state());
    _stats.queries_by_cl[size_t(options.get_consistency())] += batch->get_statements().size();
   if (log.is_enabled(logging::log_level::trace)) {
        std::ostringstream oss;
        for (const auto& s: batch->get_statements()) {
            oss << std::endl <<  s.statement->raw_cql_statement;
        }
        log.trace("execute_batch({}): {}", batch->get_statements().size(), oss.str());
    }
    co_return co_await batch->execute(*this, query_state, options, std::nullopt);
}

future<service::broadcast_tables::query_result>
query_processor::execute_broadcast_table_query(const service::broadcast_tables::query& query) {
    auto [remote_, holder] = remote();
    co_return co_await service::broadcast_tables::execute(remote_.get().group0_client, query);
}

future<query::mapreduce_result>
query_processor::mapreduce(query::mapreduce_request req, tracing::trace_state_ptr tr_state) {
    auto [remote_, holder] = remote();
    co_return co_await remote_.get().mapreducer.dispatch(std::move(req), std::move(tr_state));
}

future<::shared_ptr<messages::result_message>>
query_processor::execute_schema_statement(const statements::schema_altering_statement& stmt, service::query_state& state, const query_options& options, service::group0_batch& mc) {
    if (this_shard_id() != 0) {
        on_internal_error(log, "DDL must be executed on shard 0");
    }

    auto [ce, warnings] = co_await stmt.prepare_schema_mutations(*this, state, options, mc);
    // We are creating something.
    if (!mc.empty()) {
        // Internal queries don't trigger auto-grant. Statements which don't require
        // auto-grant use default nop grant_permissions_to_creator implementation.
        // Additional implicit assumption is that prepare_schema_mutations returns
        // empty mutations vector when resource doesn't need to be created (e.g. already exists).
        auto& client_state = state.get_client_state();
        if (!client_state.is_internal()) {
            co_await stmt.grant_permissions_to_creator(client_state, mc);
        }
    }
    ::shared_ptr<messages::result_message> result;
    if (!ce) {
        result = ::make_shared<messages::result_message::void_message>();
    } else {
        result = ::make_shared<messages::result_message::schema_change>(ce);
    }

    for (const sstring& warning : warnings) {
        result->add_warning(warning);
    }

    co_return result;
}

future<> query_processor::announce_schema_statement(const statements::schema_altering_statement& stmt, service::group0_batch& mc) {
    auto description = format("CQL DDL statement: \"{}\"", stmt.raw_cql_statement);
    auto [remote_, holder] = remote();
    auto [m, guard] = co_await std::move(mc).extract();
    if (m.empty()) {
        co_return;
    }
    auto alter_ks_stmt_ptr = dynamic_cast<const statements::alter_keyspace_statement*>(&stmt);
    if (alter_ks_stmt_ptr && alter_ks_stmt_ptr->changes_tablets(*this)) {
        auto request_id = guard.new_group0_state_id();
        co_await remote_.get().mm.announce<service::topology_change>(std::move(m), std::move(guard), description);
        // TODO: eliminate timeout from alter ks statement on the cqlsh/driver side
        auto error = co_await remote_.get().ss.wait_for_topology_request_completion(request_id);
        co_await remote_.get().ss.wait_for_topology_not_busy();
        if (!error.empty()) {
            log.error("CQL statement \"{}\" with topology request_id \"{}\" failed with error: \"{}\"", stmt.raw_cql_statement, request_id, error);
            throw exceptions::request_execution_exception(exceptions::exception_code::INVALID, error);
        }
        co_return;
    }
    co_await remote_.get().mm.announce(std::move(m), std::move(guard), description);
}

query_processor::migration_subscriber::migration_subscriber(query_processor* qp) : _qp{qp} {
}

void query_processor::migration_subscriber::on_create_keyspace(const sstring& ks_name) {
}

void query_processor::migration_subscriber::on_create_column_family(const sstring& ks_name, const sstring& cf_name) {
}

void query_processor::migration_subscriber::on_create_user_type(const sstring& ks_name, const sstring& type_name) {
}

void query_processor::migration_subscriber::on_create_function(const sstring& ks_name, const sstring& function_name) {
    log.warn("{} event ignored", __func__);
}

void query_processor::migration_subscriber::on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name) {
    log.warn("{} event ignored", __func__);
}

void query_processor::migration_subscriber::on_create_view(const sstring& ks_name, const sstring& view_name) {
}

void query_processor::migration_subscriber::on_update_keyspace(const sstring& ks_name) {
}

void query_processor::migration_subscriber::on_update_column_family(
        const sstring& ks_name,
        const sstring& cf_name,
        bool columns_changed) {
    // #1255: Ignoring columns_changed deliberately.
    log.info("Column definitions for {}.{} changed, invalidating related prepared statements", ks_name, cf_name);
    remove_invalid_prepared_statements(ks_name, cf_name);
}

void query_processor::migration_subscriber::on_update_user_type(const sstring& ks_name, const sstring& type_name) {
}

void query_processor::migration_subscriber::on_update_function(const sstring& ks_name, const sstring& function_name) {
}

void query_processor::migration_subscriber::on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name) {
}

void query_processor::migration_subscriber::on_update_view(
        const sstring& ks_name,
        const sstring& view_name, bool columns_changed) {
    // scylladb/scylladb#16392 - Materialized views are also tables so we need at least handle
    // them as such when changed.
    on_update_column_family(ks_name, view_name, columns_changed);
}

void query_processor::migration_subscriber::on_update_tablet_metadata(const locator::tablet_metadata_change_hint&) {
}

void query_processor::migration_subscriber::on_drop_keyspace(const sstring& ks_name) {
    remove_invalid_prepared_statements(ks_name, std::nullopt);
}

void query_processor::migration_subscriber::on_drop_column_family(const sstring& ks_name, const sstring& cf_name) {
    remove_invalid_prepared_statements(ks_name, cf_name);
}

void query_processor::migration_subscriber::on_drop_user_type(const sstring& ks_name, const sstring& type_name) {
}

void query_processor::migration_subscriber::on_drop_function(const sstring& ks_name, const sstring& function_name) {
    log.warn("{} event ignored", __func__);
}

void query_processor::migration_subscriber::on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) {
    log.warn("{} event ignored", __func__);
}

void query_processor::migration_subscriber::on_drop_view(const sstring& ks_name, const sstring& view_name) {
    remove_invalid_prepared_statements(ks_name, view_name);
}

void query_processor::migration_subscriber::remove_invalid_prepared_statements(
        sstring ks_name,
        std::optional<sstring> cf_name) {
    _qp->_prepared_cache.remove_if([&] (::shared_ptr<cql_statement> stmt) {
        return this->should_invalidate(ks_name, cf_name, stmt);
    });
}

bool query_processor::migration_subscriber::should_invalidate(
        sstring ks_name,
        std::optional<sstring> cf_name,
        ::shared_ptr<cql_statement> statement) {
    return statement->depends_on(ks_name, cf_name);
}

future<> query_processor::query_internal(
        const sstring& query_string,
        db::consistency_level cl,
        const data_value_list& values,
        int32_t page_size,
        noncopyable_function<future<stop_iteration>(const cql3::untyped_result_set_row&)> f) {
    auto query_state = create_paged_state(query_string, cl, values, page_size);
    co_return co_await for_each_cql_result(query_state, std::move(f));
}

future<> query_processor::query_internal(
        const sstring& query_string,
        noncopyable_function<future<stop_iteration>(const cql3::untyped_result_set_row&)> f) {
    return query_internal(query_string, db::consistency_level::ONE, {}, 1000, std::move(f));
}

shared_ptr<cql_transport::messages::result_message> query_processor::bounce_to_shard(unsigned shard, cql3::computed_function_values cached_fn_calls) {
    _proxy.get_stats().replica_cross_shard_ops++;
    return ::make_shared<cql_transport::messages::result_message::bounce_to_shard>(shard, std::move(cached_fn_calls));
}

void query_processor::update_authorized_prepared_cache_config() {
    utils::loading_cache_config cfg;
    cfg.max_size = _mcfg.authorized_prepared_cache_size;
    cfg.expiry = std::min(std::chrono::milliseconds(_db.get_config().permissions_validity_in_ms()),
                          std::chrono::duration_cast<std::chrono::milliseconds>(prepared_statements_cache::entry_expiry));
    cfg.refresh = std::chrono::milliseconds(_db.get_config().permissions_update_interval_in_ms());

    if (!_authorized_prepared_cache.update_config(std::move(cfg))) {
        log.error("Failed to apply authorized prepared statements cache changes. Please read the documentation of these parameters");
    }
}

void query_processor::reset_cache() {
    _authorized_prepared_cache.reset();
}

}
