/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "utils/assert.hh"
#include "bytes.hh"
#include "schema/schema_fwd.hh"
#include "query-result-reader.hh"
#include "selector.hh"
#include "cql3/column_specification.hh"
#include "cql3/functions/function.hh"
#include "exceptions/exceptions.hh"
#include "unimplemented.hh"
#include <seastar/core/thread.hh>

namespace cql3 {

class result_set;
class metadata;
class query_options;

namespace restrictions {
class statement_restrictions;
}

namespace selection {

class raw_selector;
class result_set_builder;

class selectors {
public:
    virtual ~selectors() {}

    virtual bool requires_thread() const = 0;

    virtual bool is_aggregate() const = 0;

    /**
    * Adds the current row of the specified <code>ResultSetBuilder</code>.
    *
    * @param rs the <code>ResultSetBuilder</code>
    * @throws InvalidRequestException
    */
    virtual void add_input_row(result_set_builder& rs) = 0;

    virtual std::vector<managed_bytes_opt> get_output_row() = 0;

    // When not aggregating, each input row becomes one output row.
    virtual std::vector<managed_bytes_opt> transform_input_row(result_set_builder& rs) = 0;

    virtual void reset() = 0;
};

class selection {
private:
    schema_ptr _schema;
    std::vector<const column_definition*> _columns;
    ::shared_ptr<metadata> _metadata;
    const bool _collect_timestamps;
    const bool _collect_TTLs;
    const bool _contains_static_columns;
    bool _is_trivial;
protected:
    using trivial = bool_class<class trivial_tag>;

    selection(schema_ptr schema,
        std::vector<const column_definition*> columns,
        std::vector<lw_shared_ptr<column_specification>> metadata_,
        bool collect_timestamps,
        bool collect_TTLs, trivial is_trivial = trivial::no);

    virtual ~selection() {}
public:
    // Overridden by SimpleSelection when appropriate.
    virtual bool is_wildcard() const {
        return false;
    }

    /**
     * Checks if this selection contains static columns.
     * @return <code>true</code> if this selection contains static columns, <code>false</code> otherwise;
     */
    bool contains_static_columns() const {
        return _contains_static_columns;
    }

    /**
     * Checks if this selection contains only static columns.
     * @return <code>true</code> if this selection contains only static columns, <code>false</code> otherwise;
     */
    bool contains_only_static_columns() const;

    /**
     * Returns the index of the specified column, in the un-processed domain (before applying
     * transformations to the input columns and aggregations)
     *
     * @param def the column definition
     * @return the index of the specified column
     */
    int32_t index_of(const column_definition& def) const;

    bool has_column(const column_definition& def) const;

    ::shared_ptr<const metadata> get_result_metadata() const {
        return _metadata;
    }

    ::shared_ptr<metadata> get_result_metadata() {
        return _metadata;
    }

    static ::shared_ptr<selection> wildcard(schema_ptr schema);
    static std::vector<const column_definition*> wildcard_columns(schema_ptr schema);
    static ::shared_ptr<selection> for_columns(schema_ptr schema, std::vector<const column_definition*> columns);

    // Adds a column to the selection and result set. Returns an index within the result set row.
    virtual uint32_t add_column_for_post_processing(const column_definition& c);

    virtual std::vector<shared_ptr<functions::function>> used_functions() const { return {}; }

    query::partition_slice::option_set get_query_options();
private:
    static bool processes_selection(const std::vector<prepared_selector>& prepared_selectors);

    static std::vector<lw_shared_ptr<column_specification>> collect_metadata(const schema& schema,
        const std::vector<prepared_selector>& prepared_selectors);
public:
    static ::shared_ptr<selection> from_selectors(data_dictionary::database db, schema_ptr schema, const sstring& ks, const std::vector<prepared_selector>& raw_selectors);

    virtual std::unique_ptr<selectors> new_selectors() const = 0;

    /**
     * Returns a range of CQL3 columns this selection needs.
     */
    auto const& get_columns() const {
        return _columns;
    }

    uint32_t get_column_count() const {
        return _columns.size();
    }

    virtual bool is_aggregate() const = 0;

    virtual bool is_count() const {return false;}

    virtual bool is_reducible() const {return false;}

    virtual query::mapreduce_request::reductions_info get_reductions() const {return {{}, {}};}

    /**
     * Returns true if the selection is trivial, i.e. there are no function
     * selectors (including casts or aggregates).
     */
    bool is_trivial() const { return _is_trivial; }

    friend class result_set_builder;
};

shared_ptr<selection> selection_from_partition_slice(schema_ptr schema, const query::partition_slice& slice);

class result_set_builder {
private:
    std::unique_ptr<result_set> _result_set;
    std::unique_ptr<selectors> _selectors;
    const std::vector<size_t> _group_by_cell_indices; ///< Indices in \c current of cells holding GROUP BY values.
    const uint64_t _limit; ///< Maximum number of rows to return.
    std::vector<managed_bytes_opt> _last_group; ///< Previous row's group: all of GROUP BY column values.
    bool _group_began; ///< Whether a group began being formed.
public:
    std::vector<managed_bytes_opt> current;
    std::vector<bytes> current_partition_key;
    std::vector<bytes> current_clustering_key;
    std::vector<api::timestamp_type> _timestamps;
    std::vector<int32_t> _ttls;
private:
    const gc_clock::time_point _now;
public:
    template<typename Func>
    auto with_thread_if_needed(Func&& func) {
        if (_selectors->requires_thread()) {
            return async(std::move(func));
        } else {
            return futurize_invoke(std::move(func));
        }
    }

    class nop_filter {
    public:
        inline bool operator()(const selection&, const std::vector<bytes>&, const std::vector<bytes>&, const query::result_row_view&, const query::result_row_view*) const {
            return true;
        }
        void reset(const partition_key* = nullptr) {
        }
        uint64_t get_rows_dropped() const {
            return 0;
        }
    };
    class restrictions_filter {
        const ::shared_ptr<const restrictions::statement_restrictions> _restrictions;
        const query_options& _options;
        const bool _skip_pk_restrictions;
        const bool _skip_ck_restrictions;
        mutable bool _current_partition_key_does_not_match = false;
        mutable bool _current_static_row_does_not_match = false;
        mutable uint64_t _rows_dropped = 0;
        mutable uint64_t _remaining;
        schema_ptr _schema;
        mutable uint64_t _per_partition_limit;
        mutable uint64_t _per_partition_remaining;
        mutable uint64_t _rows_fetched_for_last_partition;
        mutable std::optional<partition_key> _last_pkey;
        mutable bool _is_first_partition_on_page = true;
    public:
        explicit restrictions_filter(::shared_ptr<const restrictions::statement_restrictions> restrictions,
                const query_options& options,
                uint64_t remaining,
                schema_ptr schema,
                uint64_t per_partition_limit,
                std::optional<partition_key> last_pkey = {},
                uint64_t rows_fetched_for_last_partition = 0);
        bool operator()(const selection& selection, const std::vector<bytes>& pk, const std::vector<bytes>& ck, const query::result_row_view& static_row, const query::result_row_view* row) const;
        void reset(const partition_key* key = nullptr);
        uint64_t get_rows_dropped() const {
            return _rows_dropped;
        }
    private:
        bool do_filter(const selection& selection, const std::vector<bytes>& pk, const std::vector<bytes>& ck, const query::result_row_view& static_row, const query::result_row_view* row) const;
    };

    result_set_builder(const selection& s, gc_clock::time_point now,
                       std::vector<size_t> group_by_cell_indices = {},
                       uint64_t limit = std::numeric_limits<uint64_t>::max());
    void add_empty();
    void add(bytes_opt value);
    void add(const column_definition& def, const query::result_atomic_cell_view& c);
    void add_collection(const column_definition& def, bytes_view c);
    void start_new_row();
    void complete_row();
    std::unique_ptr<result_set> build();
    api::timestamp_type timestamp_of(size_t idx);
    int32_t ttl_of(size_t idx);
    size_t result_set_size() const;

    // Implements ResultVisitor concept from query.hh
    template<typename Filter = nop_filter>
    class visitor {
    protected:
        result_set_builder& _builder;
        const schema& _schema;
        const selection& _selection;
        uint64_t _row_count;
        std::vector<bytes>& _partition_key;
        std::vector<bytes>& _clustering_key;
        Filter _filter;
    public:
        visitor(cql3::selection::result_set_builder& builder, const schema& s,
                const selection& selection, Filter filter = Filter())
            : _builder(builder)
            , _schema(s)
            , _selection(selection)
            , _row_count(0)
            , _partition_key(_builder.current_partition_key)
            , _clustering_key(_builder.current_clustering_key)
            , _filter(filter)
        {}
        visitor(visitor&&) = default;

        void add_value(const column_definition& def, query::result_row_view::iterator_type& i) {
            if (def.type->is_multi_cell()) {
                auto cell = i.next_collection_cell();
                if (!cell) {
                    _builder.add_empty();
                    return;
                }
                _builder.add_collection(def, cell->linearize());
            } else {
                auto cell = i.next_atomic_cell();
                if (!cell) {
                    _builder.add_empty();
                    return;
                }
                _builder.add(def, *cell);
            }
        }

        void accept_new_partition(const partition_key& key, uint64_t row_count) {
            _partition_key = key.explode(_schema);
            _row_count = row_count;
            _filter.reset(&key);
        }

        void accept_new_partition(uint64_t row_count) {
            _row_count = row_count;
            _filter.reset();
        }

        void accept_new_row(const clustering_key& key, const query::result_row_view& static_row, const query::result_row_view& row) {
            _clustering_key = key.explode(_schema);
            accept_new_row(static_row, row);
        }

        void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
            auto static_row_iterator = static_row.iterator();
            auto row_iterator = row.iterator();
            if (!_filter(_selection, _partition_key, _clustering_key, static_row, &row)) {
                return;
            }
            _builder.start_new_row();
            for (auto&& def : _selection.get_columns()) {
                switch (def->kind) {
                case column_kind::partition_key:
                    _builder.add(_partition_key[def->component_index()]);
                    break;
                case column_kind::clustering_key:
                    if (_clustering_key.size() > def->component_index()) {
                        _builder.add(_clustering_key[def->component_index()]);
                    } else {
                        _builder.add({});
                    }
                    break;
                case column_kind::regular_column:
                    add_value(*def, row_iterator);
                    break;
                case column_kind::static_column:
                    add_value(*def, static_row_iterator);
                    break;
                default:
                    SCYLLA_ASSERT(0);
                }
            }
            _builder.complete_row();
        }

        uint64_t accept_partition_end(const query::result_row_view& static_row) {
            if (_row_count == 0) {
                if (!_filter(_selection, _partition_key, _clustering_key, static_row, nullptr)) {
                    return _filter.get_rows_dropped();
                }
                _builder.start_new_row();
                auto static_row_iterator = static_row.iterator();
                for (auto&& def : _selection.get_columns()) {
                    if (def->is_partition_key()) {
                        _builder.add(_partition_key[def->component_index()]);
                    } else if (def->is_static()) {
                        add_value(*def, static_row_iterator);
                    } else {
                        _builder.add_empty();
                    }
                }
                _builder.complete_row();
            }
            return _filter.get_rows_dropped();
        }
    };

private:
    bytes_opt get_value(data_type t, query::result_atomic_cell_view c);

    /// True iff the \c current row ends a previously started group, either according to
    /// _group_by_cell_indices or aggregation.
    bool last_group_ended() const;

    /// Gets output row from _selectors and resets them.
    void flush_selectors();

    /// Updates _last_group from the \c current row.
    void update_last_group();
};

}

}
