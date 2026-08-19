/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/selection/selection.hh"
#include "keys/keys.hh"
#include "vector_search/vector_store_client.hh"

#include <optional>

class schema;
class column_definition;

namespace cql3::statements {

/// What a full-text search needs in order to be asked for a fragment of each fetched row's text.
///
/// Everything here is borrowed from the statement's execution frame, which outlives the base-table
/// read the provider is prepared after.
struct highlight_source {
    vector_search::vector_store_client& client;
    sstring keyspace;
    sstring index;
    sstring query;
    seastar::abort_source& as;
    /// The selection the fetched rows are shaped by, and the column within it whose text is sent.
    const selection::selection& selection;
    const column_definition& column;
};

/// The per-row values one external search's response injects into a result row.
///
/// A search answers with one entry per candidate row.  A value that entry carries - the score - can
/// be handed out straight away; a value generated from the row's own contents - a highlight - only
/// exists once the base-table rows have been read, and is asked for in prepare().  Both land in
/// temporary slots of the same row, so one provider owns them: they are matched to a row
/// differently - the score against the response by primary key, the highlight by position - and
/// when the score's cursor passes a row over, so that the row is dropped, the highlight's position
/// has to move with it.
///
/// The score's cursor only moves forward: base-table results are merged in external search
/// primary-key order, so a row can only ever match at or after the current position.  Entries it
/// steps over are keys the index still knows about but that are no longer in the base table.
///
/// A provider instance is therefore single-use and tied to one response - it cannot be rewound or
/// replayed, which is worth keeping in mind when paging arrives.
class external_search_provider : public cql3::selection::external_values_provider {
    const vector_search::vector_store_client::primary_keys& _results;
    mutable size_t _next_result;                    // cursor into _results: which entry to match next
    const std::optional<size_t> _score_slot;        // temporary slot the score is written to
    const std::optional<size_t> _highlight_slot;    // temporary slot the fragment is written to
    const schema& _schema;

    std::optional<highlight_source> _highlight_source;   // what prepare() asks, and of whom
    /// prepare()'s answer, one entry per fetched row, and the key each was collected for.  Empty
    /// unless a fragment is wanted.
    std::vector<std::optional<sstring>> _highlights;
    std::vector<std::pair<partition_key, clustering_key_prefix>> _highlighted_rows;
    mutable size_t _next_row;                       // index into _highlights: which row is being filled

public:
    external_search_provider(const vector_search::vector_store_client::primary_keys& results, std::optional<size_t> score_slot,
            std::optional<size_t> highlight_slot, const schema& schema, std::optional<highlight_source> highlight_source = std::nullopt);

    future<> prepare(const query::result& result, const query::partition_slice& slice) override;

    bool try_fill(std::vector<cql3::raw_value>& temporaries, std::span<const bytes> partition_key, std::span<const bytes> clustering_key,
            const query::result_row_view& static_row, const query::result_row_view* row) const override;
};

} // namespace cql3::statements
