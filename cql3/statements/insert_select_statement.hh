/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "cql3/attributes.hh"
#include "cql3/statements/select_statement.hh"
#include "schema/schema_fwd.hh"
#include "dht/i_partitioner.hh"
#include "mutation/timestamp.hh"
#include "gc_clock.hh"

#include <seastar/core/shared_ptr.hh>

#include <memory>
#include <vector>

namespace cql3 {

class query_processor;

namespace statements {

/**
 * A prepared, distributed <code>INSERT INTO target SELECT ... FROM source</code>.
 *
 * Execution model (coordinator-driven, token-range fan-out):
 *
 *   1. The source token ring [min, max] is split into N contiguous subranges,
 *      N scaled to the cluster size (vnodes/tablets * shards) so the work is
 *      spread across owning replicas rather than funneled.
 *   2. Up to `max_concurrent_ranges` subranges are processed concurrently; a
 *      semaphore bounds coordinator memory and in-flight load (backpressure).
 *   3. For each subrange, the inner SELECT is executed with internal paging
 *      restricted to that token range. Each page's rows are converted into
 *      mutations for the target table and applied via the normal write path,
 *      which fans each mutation out to its owning replicas cluster-wide.
 *   4. The next page is only fetched once the previous page's writes have been
 *      acknowledged, bounding per-range memory to one page.
 *
 * The operation is NOT atomic and NOT idempotent across coordinator failure:
 * partially-copied data may remain if the job aborts. This mirrors the
 * semantics of an externally-driven COPY and is documented for users.
 *
 * Phase 2 (not in this change): push each subrange to the node that owns it so
 * reads are node-local; the per-range pipeline below is unchanged by that move.
 */
class insert_select_statement : public cql_statement_no_metadata {
public:
    // Maps one selected result-set column onto a target column.
    struct column_mapping_entry {
        // Index into the SELECT result row.
        uint32_t source_index;
        // Target column being written.
        const column_definition* target_column;
    };

private:
    // Target table schema.
    schema_ptr _target_schema;
    // Prepared inner SELECT. Drives the read side.
    ::shared_ptr<cql3::statements::select_statement> _source;
    // Source -> target column mapping, validated for type compatibility.
    std::vector<column_mapping_entry> _mapping;
    // Target primary-key columns must all be covered by the mapping; these are
    // their indices into _mapping, used to build partition/clustering keys.
    std::vector<uint32_t> _partition_key_mapping_indices;
    std::vector<uint32_t> _clustering_key_mapping_indices;
    // Source result-set column name for each _mapping entry, resolved from the
    // inner SELECT's result metadata. Source rows are keyed by these names;
    // positional remapping means they need not match the target column names.
    std::vector<sstring> _source_names;
    // USING TIMESTAMP / TTL applied uniformly to every produced row.
    std::unique_ptr<attributes> _attrs;
    uint32_t _bound_terms;

public:
    insert_select_statement(schema_ptr target_schema,
            ::shared_ptr<cql3::statements::select_statement> source,
            std::vector<column_mapping_entry> mapping,
            std::vector<uint32_t> partition_key_mapping_indices,
            std::vector<uint32_t> clustering_key_mapping_indices,
            std::unique_ptr<attributes> attrs,
            uint32_t bound_terms,
            cql_stats& stats);

    virtual uint32_t get_bound_terms() const override { return _bound_terms; }

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    virtual bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const override;

private:
    // Number of token subranges to split the source ring into, scaled to the
    // shard count so reads/writes spread across replicas and shards.
    size_t compute_range_split() const;

    // Split the whole token ring [min, max] into `n` disjoint, contiguous
    // half-open sub-ranges whose union is the entire ring.
    static dht::partition_range_vector split_token_ring(size_t n);

    // Process a single token subrange end-to-end: paged read of the source
    // (restricted to `range` when `restrict_to_range` is set, else the full
    // ring), converting each page to target mutations and applying them, with
    // one page in flight at a time. `timestamp`/`ttl` are computed once by the
    // caller so every copied row shares them across all ranges.
    future<uint64_t> process_token_range(query_processor& qp, service::query_state& state,
            const query_options& options, const dht::partition_range& range, bool restrict_to_range,
            api::timestamp_type timestamp, std::optional<gc_clock::duration> ttl) const;
};

}

}
