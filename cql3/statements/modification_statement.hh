/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "cql3/stats.hh"
#include "cql3/update_parameters.hh"
#include "cql3/cql_statement.hh"
#include "cql3/statements/modification_spec.hh"
#include "cql3/statements/statement_type.hh"
#include "exceptions/coordinator_result.hh"

#include <seastar/core/shared_ptr.hh>

#include <memory>
#include <optional>

namespace db {
enum class large_data_violation_type : uint8_t;
}

namespace cql3 {

class query_processor;
class attributes;
class operation;

namespace statements {


namespace raw { class modification_statement; }

class modification_executor;

/*
 * Abstract parent class of individual modifications, i.e. INSERT, UPDATE and DELETE.
 *
 * Knows what to write and deliberately not how to commit it. The
 * modification_executor below does that, chosen when the statement is prepared
 * from the keyspace it addresses.
 */
class modification_statement : public cql_statement {
public:
    const statement_type type;
    bool _may_use_token_aware_routing;
private:
    const uint32_t _bound_terms;
    // If we have operation on list entries, such as adding or
    // removing an entry, the modification statement must prefetch
    // the old values of the list to create an idempotent mutation.
    // If the statement has conditions, conditional columns must
    // also be prefetched, to evaluate conditions. If the
    // statement has IF EXISTS/IF NOT EXISTS, we prefetch all
    // columns, to match Cassandra behaviour.
    // This bitset contains a mask of ordinal_id identifiers
    // of the required columns.
    column_set _columns_to_read;
    // A CAS statement returns a result set with the columns
    // used in condition expression. This is a mask of ordinal_id
    // identifiers of the required columns. Contains all columns
    // of a schema if we have IF EXISTS/IF NOT EXISTS. Does *not*
    // contain LIST columns prefetched to apply updates, unless
    // these columns are also used in conditions.
    column_set _columns_of_cas_result_set;
public:
    const schema_ptr s;
    const std::unique_ptr<attributes> attrs;

protected:
    std::vector<std::unique_ptr<operation>> _column_operations;
    cql_stats& _stats;

    expr::expression _condition = expr::conjunction{{}}; // TRUE
private:
    const ks_selector _ks_sel;

    // True if this statement has _if_exists or _if_not_exists or other
    // conditions that apply to static/regular columns, respectively.
    // Pre-computed during statement prepare.
    bool _has_static_column_conditions = false;
    bool _has_regular_column_conditions = false;
    // True if any of update operations requires a prefetch.
    // Pre-computed during statement prepare.
    bool _requires_read = false;
    // True if any of the update operations requires LWT (an IF condition) for
    // atomicity, e.g. SET col = col + 1 on a non-counter column.
    bool _requires_lwt = false;
    bool _if_not_exists = false;
    bool _if_exists = false;

    // True if this statement has column operations that apply to static/regular
    // columns, respectively.
    bool _sets_static_columns = false;
    bool _sets_regular_columns = false;

    std::optional<bool> _is_raw_counter_shard_write;

public:
    using json_cache_opt = modification_spec::json_cache_opt;

    modification_statement(
            statement_type type_,
            uint32_t bound_terms,
            schema_ptr schema_,
            std::unique_ptr<attributes> attrs_,
            cql_stats& stats_);

    virtual ~modification_statement() override;

    uint32_t get_bound_terms() const override;

    const sstring& keyspace() const;

    const sstring& column_family() const;

    bool is_counter() const;

    bool is_view() const;

    int64_t get_timestamp(int64_t now, const query_options& options) const;

    bool is_timestamp_set() const;

    std::optional<gc_clock::duration> get_time_to_live(const query_options& options) const;

    future<> check_access(query_processor& qp, const service::client_state& state) const override;

    // Validate before execute, using client state and current schema
    void validate(query_processor&, const service::client_state& state) const override;

    bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    bool should_reclassify_control_connection() const override;

    void add_operation(std::unique_ptr<operation> op);

    void inc_cql_stats(bool is_internal) const;

    bool is_conditional() const override;

public:
    void analyze_condition(expr::expression cond);

    void set_if_not_exist_condition();

    bool has_if_not_exist_condition() const;

    void set_if_exist_condition();

    bool has_if_exist_condition() const;

    bool is_raw_counter_shard_write() const {
        return _is_raw_counter_shard_write.value_or(false);
    }

    /// Decides whether an IF EXISTS / IF NOT EXISTS condition is about the static
    /// row or about a clustering row.  Must run before the checks that read
    /// applies_only_to_static_columns(), which this can change.
    void classify_exists_condition(bool restricts_clustering_columns);

    /// Checks that the primary key the statement names has no null values, throwing
    /// invalid_request_exception otherwise.
    virtual void validate_primary_key(const query_options& options) const = 0;

    // CAS statement returns a result set. Prepare result set metadata
    // so that get_result_metadata() returns a meaningful value.
    void build_cas_result_set_metadata();

public:
    virtual dht::partition_range_vector build_partition_keys(const query_options& options, const json_cache_opt& json_cache) const = 0;
    virtual query::clustering_row_ranges create_clustering_ranges(const query_options& options, const json_cache_opt& json_cache) const = 0;

protected:
    // Return true if this statement doesn't update or read any regular rows, only static rows.
    // Note, it isn't enough to just check !_sets_regular_columns && _regular_conditions.empty(),
    // because a DELETE statement that deletes whole rows (DELETE FROM ...) technically doesn't
    // have any column operations and hence doesn't have _sets_regular_columns set. It doesn't
    // have _sets_static_columns set either so checking the latter flag too here guarantees that
    // this function works as expected in all cases.
    bool applies_only_to_static_columns() const {
        return _sets_static_columns && !_sets_regular_columns && !_has_regular_column_conditions;
    }
public:
    // True if any of update operations of this statement requires
    // a prefetch of the old cell.
    bool requires_read() const { return _requires_read; }
    bool has_column_operations() const { return !_column_operations.empty(); }

    // True if any of the update operations requires LWT for atomicity.
    bool requires_lwt() const { return _requires_lwt; }

    // Columns used in this statement conditions or operations.
    const column_set& columns_to_read() const { return _columns_to_read; }

    // Columns of the statement result set (only CAS statement
    // returns a result set).
    const column_set& columns_of_cas_result_set() const { return _columns_of_cas_result_set; }

    // The result set metadata a conditional modification answers with, for a
    // modification_executor building that result set.
    const seastar::shared_ptr<metadata>& cas_result_metadata() const { return _metadata; }

    // Build a read_command instance to fetch the previous mutation from storage. The mutation is
    // fetched if we need to check LWT conditions or apply updates to non-frozen list elements.
    lw_shared_ptr<query::read_command> read_command(query_processor& qp, query::clustering_row_ranges ranges, db::consistency_level cl) const;
    // Create a mutation object for the update operation represented by this modification statement.
    // A single mutation object for lightweight transactions, which can only span one partition, or a vector
    // of mutations, one per partition key, for statements which affect multiple partition keys,
    // e.g. DELETE FROM table WHERE pk  IN (1, 2, 3).
    virtual utils::chunked_vector<mutation> apply_updates(
            const modification_spec& spec,
            const update_parameters& params) const = 0;

protected:
    // One empty mutation per partition the statement addresses, for apply_updates()
    // to write rows into.
    utils::chunked_vector<mutation> make_mutations(const std::vector<dht::partition_range>& keys) const;

public:

    /**
     * Checks whether the conditions represented by this statement apply provided the current state of the row on
     * which those conditions are.
     *
     * @param row the row with current data corresponding to these conditions. Can be null if there
     * is no matching row.
     * @return whether the conditions represented by this statement apply or not.
     */
    bool applies_to(const selection::selection* selection, const update_parameters::prefetch_data::row* row, const query_options& options) const;

private:
    future<::shared_ptr<cql_transport::messages::result_message>>
    do_execute(query_processor& qp, service::query_state& qs, const query_options& options) const;
    friend class modification_statement_executor;
public:
    // True if the statement has IF conditions. Pre-computed during prepare.
    bool has_conditions() const { return _has_regular_column_conditions || _has_static_column_conditions; }
    // True if the statement has IF conditions that apply to static columns.
    bool has_static_column_conditions() const { return _has_static_column_conditions; }
    // True if this statement needs to read only static column values to check if it can be applied.
    bool has_only_static_column_conditions() const { return !_has_regular_column_conditions && _has_static_column_conditions; }

    bool has_regular_column_conditions() const { return _has_regular_column_conditions; }

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute_without_checking_exception_message(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const override;

public:
    // How this modification reaches storage. Set when the statement is prepared
    // and never null afterwards; see cql3::statements::modification_executor.
    const modification_executor& executor() const { return *_executor; }
    void set_executor(const modification_executor& e) { _executor = &e; }

    // True if this modification commits through Raft rather than storage_proxy.
    // The native protocol handler asks, because a batch may not mix the two.
    bool is_strongly_consistent() const;

    virtual json_cache_opt maybe_prepare_json_cache(const query_options& options) const;

    db::timeout_clock::duration get_timeout(const service::client_state& state, const query_options& options) const;

protected:
    /**
     * If there are conditions on the statement, this is called after the where clause and conditions have been
     * processed to check that they are compatible.  A conditional statement cannot
     * use IN on a key column: it addresses one row.
     * @throws InvalidRequestException
     */
    void reject_in_relations_with_conditions(bool key_is_in_relation, bool clustering_key_has_IN) const;

private:
    const modification_executor* _executor;

    friend class raw::modification_statement;
};

}

}
