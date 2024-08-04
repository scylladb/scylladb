/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */
#pragma once
#include "utils/assert.hh"
#include "service/paxos/cas_request.hh"
#include "cql3/statements/modification_statement.hh"

namespace cql3::statements {

using namespace std::chrono;

/**
 * Due to some operation on lists, we can't generate the update that a given Modification statement does before
 * we get the values read by the initial read of Paxos. A RowUpdate thus just store the relevant information
 * (include the statement itself) to generate those updates. We'll have multiple RowUpdate for a Batch, otherwise
 * we'll have only one.
 */
struct cas_row_update {
    modification_statement const& statement;
    std::vector<query::clustering_range> ranges;
    modification_statement::json_cache_opt json_cache;
    // This statement query options. Different from cas_request::query_options,
    // which may stand for BATCH statement, not individual modification_statement,
    // in case of BATCH
    const query_options& options;
};

/**
 * Processed CAS conditions and update on potentially multiple rows of the same partition.
 */
class cas_request: public service::cas_request {
private:
    std::vector<cas_row_update> _updates;
    schema_ptr _schema;
    // A single partition key. Represented as a vector of partition ranges
    // since this is the conventional format for storage_proxy.
    std::vector<dht::partition_range> _key;
    update_parameters::prefetch_data _rows;

public:
    cas_request(schema_ptr schema_arg, std::vector<dht::partition_range> key_arg)
          : _schema(schema_arg)
          , _key(std::move(key_arg))
          , _rows(schema_arg)
    {
        SCYLLA_ASSERT(_key.size() == 1 && query::is_single_partition(_key.front()));
    }

    dht::partition_range_vector key() const {
        return dht::partition_range_vector(_key);
    }

    const update_parameters::prefetch_data& rows() const {
        return _rows;
    }

    lw_shared_ptr<query::read_command> read_command(query_processor& qp) const;

    void add_row_update(const modification_statement& stmt_arg, std::vector<query::clustering_range> ranges_arg,
        modification_statement::json_cache_opt json_cache_arg, const query_options& options_arg);

    virtual std::optional<mutation> apply(foreign_ptr<lw_shared_ptr<query::result>> qr,
            const query::partition_slice& slice, api::timestamp_type ts) override;

    /// Build a result set with prefetched rows, but return only
    /// the columns required by CAS.
    ///
    /// Each cas_row_update provides a row in the result set.
    /// Rows are ordered the same way as the individual statements appear
    /// in case of batch statement.
    seastar::shared_ptr<cql_transport::messages::result_message>
    build_cas_result_set(seastar::shared_ptr<cql3::metadata> metadata,
            const column_set& mask, bool is_applied) const;

private:
    bool applies_to() const;
    std::optional<mutation> apply_updates(api::timestamp_type t) const;
    /// Find a row in prefetch_data which matches primary key identifying a given `cas_row_update`
    struct old_row {
        const clustering_key* ckey;
        const update_parameters::prefetch_data::row* row;
    };
    old_row find_old_row(const cas_row_update& op) const;
};

} // end of namespace "cql3::statements"
