/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "external_search_provider.hh"

#include <cmath>

#include "cql3/expr/expr-utils.hh"
#include "cql3/values.hh"
#include "exceptions/exceptions.hh"
#include "keys/keys.hh"
#include "query/query-request.hh"
#include "query/query-result-reader.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "utils/log.hh"

#include <seastar/coroutine/exception.hh>

namespace cql3::statements {

namespace {

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
logging::logger hlogger("external_search_provider");

/// Collects the text a fragment is to be generated from, one document per fetched row.
///
/// The documents are matched to the fragments by position, so this has to emit exactly the rows
/// result_set_builder::visitor emits, in the same order - which is why it walks the same merged
/// result with the same slice, and why it repeats that visitor's rule for a partition holding
/// nothing but a static row.  The key of each row is collected alongside, so that a drift between
/// the two walks is caught rather than silently misattributing every fragment.
class document_collector {
    const schema& _schema;
    const selection::selection& _selection;
    const column_definition& _column;
    size_t _column_index;
    std::vector<sstring>& _documents;
    std::vector<std::pair<partition_key, clustering_key_prefix>>& _rows;

    std::optional<partition_key> _partition_key;
    clustering_key_prefix _clustering_key = clustering_key_prefix::make_empty();
    uint64_t _row_count = 0;

    void collect(const query::result_row_view& static_row, const query::result_row_view* row) {
        _documents.push_back(read_text(static_row, row));
        _rows.emplace_back(_partition_key.value_or(partition_key::make_empty()), _clustering_key);
    }

    sstring read_text(const query::result_row_view& static_row, const query::result_row_view* row) const {
        // A row whose text is null or absent still needs a document, or every fragment after it
        // would belong to the wrong row.  The index answers an empty document with no fragment.
        auto value = _column.is_clustering_key() ? clustering_key_component() : expr::get_non_pk_values(_selection, static_row, row)[_column_index];
        if (!value) {
            return {};
        }
        return value_cast<sstring>(_column.type->deserialize(managed_bytes_view(*value)));
    }

    std::optional<managed_bytes> clustering_key_component() const {
        auto components = _clustering_key.explode(_schema);
        if (components.size() <= _column.component_index()) {
            return std::nullopt;
        }
        return managed_bytes(components[_column.component_index()]);
    }

public:
    document_collector(const schema& schema, const selection::selection& selection, const column_definition& column, size_t column_index,
            std::vector<sstring>& documents, std::vector<std::pair<partition_key, clustering_key_prefix>>& rows)
        : _schema(schema)
        , _selection(selection)
        , _column(column)
        , _column_index(column_index)
        , _documents(documents)
        , _rows(rows) {
    }

    void accept_new_partition(const partition_key& key, uint64_t row_count) {
        _partition_key = key;
        _row_count = row_count;
    }

    void accept_new_partition(uint64_t row_count) {
        _partition_key = std::nullopt;
        _row_count = row_count;
    }

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row, const query::result_row_view& row) {
        _clustering_key = key;
        accept_new_row(static_row, row);
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
        collect(static_row, &row);
    }

    void accept_partition_end(const query::result_row_view& static_row) {
        if (_row_count == 0) {
            // The builder emits one row for a partition that holds only a static row, so there is a
            // document to collect for it too.
            _clustering_key = clustering_key_prefix::make_empty();
            collect(static_row, nullptr);
        }
    }
};

/// Where in the values get_non_pk_values() hands back the highlighted column sits.  That vector is
/// aligned with the selection's columns, which is also the order the slice asks the replicas for
/// them in, so the three agree by construction.
size_t column_index_in(const selection::selection& selection, const column_definition& column) {
    const auto& columns = selection.get_columns();
    auto it = std::ranges::find(columns, &column);
    if (it == columns.end()) {
        on_internal_error(hlogger, seastar::format("column {} is highlighted but was not asked for", column.name_as_text()));
    }
    return std::distance(columns.begin(), it);
}

} // anonymous namespace

external_search_provider::external_search_provider(const vector_search::vector_store_client::primary_keys& results, std::optional<size_t> score_slot,
        std::optional<size_t> highlight_slot, const schema& schema, std::optional<highlight_source> highlight_source)
    : _results(results)
    , _next_result(0)
    , _score_slot(score_slot)
    , _highlight_slot(highlight_slot)
    , _schema(schema)
    , _highlight_source(std::move(highlight_source))
    , _next_row(0) {
}

future<> external_search_provider::prepare(const query::result& result, const query::partition_slice& slice) {
    if (!_highlight_slot) {
        co_return;
    }

    auto& source = *_highlight_source;
    auto documents = std::vector<sstring>{};
    query::result_view::consume(result, slice,
            document_collector(_schema, source.selection, source.column, column_index_in(source.selection, source.column), documents, _highlighted_rows));

    if (documents.empty()) {
        // Nothing was fetched, so there is nothing to generate a fragment from.
        co_return;
    }

    auto fragments = co_await source.client.highlight(source.keyspace, source.index, source.query, std::move(documents), source.as);
    if (!fragments) {
        co_await coroutine::return_exception(
                exceptions::invalid_request_exception(std::visit(vector_search::vector_store_client::fts_error_visitor{}, fragments.error())));
    }
    // The client has already checked the reply against the number of documents it sent.
    _highlights = std::move(fragments.value());
}

bool external_search_provider::try_fill(std::vector<cql3::raw_value>& temporaries, std::span<const bytes> partition_key,
        std::span<const bytes> clustering_key, const query::result_row_view&, const query::result_row_view* row) const {
    const auto row_pk = ::partition_key::from_range(partition_key);
    const auto row_ck = (_schema.clustering_key_size() > 0) ? ::clustering_key_prefix::from_range(clustering_key) : ::clustering_key_prefix{};

    if (_highlight_slot) {
        // Advanced for every row the builder offers, whatever the score below decides: a row the
        // score drops still consumed the fragment collected for it.
        const auto row_index = _next_row++;
        if (row_index >= _highlighted_rows.size()) {
            on_internal_error(hlogger, seastar::format("row {} was not seen when the documents to highlight were collected", row_index));
        }
        const auto& [collected_pk, collected_ck] = _highlighted_rows[row_index];
        // A partition holding nothing but a static row is offered with the clustering key of
        // whichever row came before it - the builder never clears it - so there is nothing to
        // compare there, and the partition key is the whole of a static-only row's identity anyway.
        const bool same_row = collected_pk.equal(_schema, row_pk)
                && (row == nullptr || _schema.clustering_key_size() == 0 || collected_ck.equal(_schema, row_ck));
        if (!same_row) {
            // The two walks over the result disagree about which row is which, so every fragment
            // from here on would belong to the wrong row.
            on_internal_error(hlogger, seastar::format("row {} is not the row its fragment was collected for", row_index));
        }
        // A row the index found no fragment in is kept, with the value left absent.  So is a row
        // reached before prepare() had anything to hand out, which is only the empty result.
        temporaries[*_highlight_slot] = row_index < _highlights.size() && _highlights[row_index]
                                               ? cql3::raw_value::make_value(utf8_type->decompose(*_highlights[row_index]))
                                               : cql3::raw_value::make_null();
    }

    if (!_score_slot) {
        return true;
    }

    // Base-table results are merged in Vector Store primary-key order by
    // external_index_select_statement. Consume the matching score in that order,
    // passing over results with no matching row - the index may be stale and
    // return keys of rows that are no longer in the base table.
    while (_next_result < _results.size()) {
        const auto& vs_result = _results[_next_result];

        if (!vs_result.partition.key().equal(_schema, row_pk)) {
            ++_next_result;
            continue;
        }

        if (_schema.clustering_key_size() > 0) {
            if (!vs_result.clustering.equal(_schema, row_ck)) {
                ++_next_result;
                continue;
            }
        }

        float score = vs_result.similarity;
        ++_next_result;

        // Vector store can't return Inf over JSON API.
        // It also shouldn't return NaN (null in JSON),
        // but if it does, we treat it as an error and skip the row.
        if (!std::isfinite(score)) {
            return false;
        }

        temporaries[*_score_slot] = cql3::raw_value::make_value(float_type->decompose(score));
        return true;
    }

    return false;
}

} // namespace cql3::statements
