/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm/equal.hpp>
#include <boost/range/algorithm/transform.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>

#include "cql3/selection/selection.hh"
#include "cql3/selection/raw_selector.hh"
#include "cql3/result_set.hh"
#include "cql3/query_options.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/first_function.hh"
#include "cql3/functions/aggregate_fcts.hh"

namespace cql3 {

logger cql_logger("cql_logger");

namespace selection {

selection::selection(schema_ptr schema,
    std::vector<const column_definition*> columns,
    std::vector<lw_shared_ptr<column_specification>> metadata_,
    bool collect_timestamps,
    bool collect_TTLs,
    trivial is_trivial)
        : _schema(std::move(schema))
        , _columns(std::move(columns))
        , _metadata(::make_shared<metadata>(std::move(metadata_)))
        , _collect_timestamps(collect_timestamps)
        , _collect_TTLs(collect_TTLs)
        , _contains_static_columns(std::any_of(_columns.begin(), _columns.end(), std::mem_fn(&column_definition::is_static)))
        , _is_trivial(is_trivial)
{ }

query::partition_slice::option_set selection::get_query_options() {
    query::partition_slice::option_set opts;

    opts.set_if<query::partition_slice::option::send_timestamp>(_collect_timestamps);
    opts.set_if<query::partition_slice::option::send_expiry>(_collect_TTLs);

    opts.set_if<query::partition_slice::option::send_partition_key>(
        std::any_of(_columns.begin(), _columns.end(),
            std::mem_fn(&column_definition::is_partition_key)));

    opts.set_if<query::partition_slice::option::send_clustering_key>(
        std::any_of(_columns.begin(), _columns.end(),
            std::mem_fn(&column_definition::is_clustering_key)));

    return opts;
}

bool selection::contains_only_static_columns() const {
    if (!contains_static_columns()) {
        return false;
    }

    if (is_wildcard()) {
        return false;
    }

    for (auto&& def : _columns) {
        if (!def->is_partition_key() && !def->is_static()) {
            return false;
        }
    }

    return true;
}

int32_t selection::index_of(const column_definition& def) const {
    auto i = std::find(_columns.begin(), _columns.end(), &def);
    if (i == _columns.end()) {
        return -1;
    }
    return std::distance(_columns.begin(), i);
}

bool selection::has_column(const column_definition& def) const {
    return std::find(_columns.begin(), _columns.end(), &def) != _columns.end();
}

bool selection::processes_selection(const std::vector<prepared_selector>& prepared_selectors) {
    return std::any_of(prepared_selectors.begin(), prepared_selectors.end(),
        [] (auto&& s) { return cql3::selection::processes_selection(s); });
}

// Special cased selection for when no function is used (this save some allocations).
class simple_selection : public selection {
private:
    const bool _is_wildcard;
public:
    static ::shared_ptr<simple_selection> make(schema_ptr schema, std::vector<const column_definition*> columns, bool is_wildcard) {
        std::vector<lw_shared_ptr<column_specification>> metadata;
        metadata.reserve(columns.size());
        for (auto&& col : columns) {
            metadata.emplace_back(col->column_specification);
        }
        return ::make_shared<simple_selection>(schema, std::move(columns), std::move(metadata), is_wildcard);
    }

    /*
     * In theory, even a simple selection could have multiple time the same column, so we
     * could filter those duplicate out of columns. But since we're very unlikely to
     * get much duplicate in practice, it's more efficient not to bother.
     */
    simple_selection(schema_ptr schema, std::vector<const column_definition*> columns,
        std::vector<lw_shared_ptr<column_specification>> metadata, bool is_wildcard)
            : selection(schema, std::move(columns), std::move(metadata), false, false, trivial::yes)
            , _is_wildcard(is_wildcard)
    { }

    virtual bool is_wildcard() const override { return _is_wildcard; }
    virtual bool is_aggregate() const override { return false; }
protected:
    class simple_selectors : public selectors {
    public:
        virtual void reset() override {
            on_internal_error(cql_logger, "simple_selectors::reset() called, but we don't support aggregation");
        }

        virtual bool requires_thread() const override { return false; }

        // Should not be reached, since this is called when aggregating
        virtual std::vector<managed_bytes_opt> get_output_row() override {
            on_internal_error(cql_logger, "simple_selectors::get_output_row() called, but we don't support aggregation");
        }

        // Should not be reached, since this is called when aggregating
        virtual void add_input_row(result_set_builder& rs) override {
            on_internal_error(cql_logger, "simple_selectors::add_input_row() called, but we don't support aggregation");
        }

        virtual std::vector<managed_bytes_opt> transform_input_row(result_set_builder& rs) override {
            return std::move(rs.current);
        }

        virtual bool is_aggregate() const override {
            return false;
        }
    };

    std::unique_ptr<selectors> new_selectors() const override {
        return std::make_unique<simple_selectors>();
    }
};

shared_ptr<selection>
selection_from_partition_slice(schema_ptr schema, const query::partition_slice& slice) {
    std::vector<const column_definition*> cdefs;
    cdefs.reserve(slice.static_columns.size() + slice.regular_columns.size());
    for (auto static_col : slice.static_columns) {
        cdefs.push_back(&schema->static_column_at(static_col));
    }
    for (auto regular_col : slice.regular_columns) {
        cdefs.push_back(&schema->regular_column_at(regular_col));
    }
    return simple_selection::make(std::move(schema), std::move(cdefs), false);
}

static
bool
contains_column_mutation_attribute(expr::column_mutation_attribute::attribute_kind kind, const expr::expression& e) {
    return expr::find_in_expression<expr::column_mutation_attribute>(e, [kind] (const expr::column_mutation_attribute& cma) {
        return cma.kind == kind;
    });
}

static
bool
contains_writetime(const expr::expression& e) {
    return contains_column_mutation_attribute(expr::column_mutation_attribute::attribute_kind::writetime, e);
}

static
bool
contains_ttl(const expr::expression& e) {
    return contains_column_mutation_attribute(expr::column_mutation_attribute::attribute_kind::ttl, e);
}

class selection_with_processing : public selection {
private:
    std::vector<expr::expression> _selectors;
    std::vector<expr::expression> _inner_loop;
    std::vector<expr::expression> _outer_loop;
    std::vector<raw_value> _initial_values_for_temporaries;
public:
    selection_with_processing(schema_ptr schema, std::vector<const column_definition*> columns,
            std::vector<lw_shared_ptr<column_specification>> metadata,
            std::vector<expr::expression> selectors)
        : selection(schema, std::move(columns), std::move(metadata),
            contains_writetime(expr::tuple_constructor{selectors}),
            contains_ttl(expr::tuple_constructor{selectors}))
        , _selectors(std::move(selectors))
    {
        auto agg_split = expr::split_aggregation(_selectors);
        _outer_loop = std::move(agg_split.outer_loop);
        _inner_loop = std::move(agg_split.inner_loop);
        _initial_values_for_temporaries = std::move(agg_split.initial_values_for_temporaries);
    }

    virtual uint32_t add_column_for_post_processing(const column_definition& c) override {
        uint32_t index = selection::add_column_for_post_processing(c);
        _selectors.push_back(expr::column_value(&c));
        if (_inner_loop.empty()) {
            // Simple case: no aggregation
            return index;
        } else {
            // Complex case: aggregation, must pass through temporary
            auto first_func = cql3::functions::aggregate_fcts::make_first_function(c.type);
            auto& agg = first_func->get_aggregate();
            auto temp_index = _initial_values_for_temporaries.size();
            auto temp = expr::temporary{
                .index = temp_index,
                .type = agg.argument_types[0],
            };
            _inner_loop.push_back(
                expr::function_call{
                    .func = agg.aggregation_function,
                    .args = {temp, expr::column_value(&c)},
                });
            _initial_values_for_temporaries.push_back(raw_value::make_value(agg.initial_state));
            _outer_loop.push_back(
                expr::function_call{
                    .func = agg.state_to_result_function,
                    .args = {temp},
                });
            return _outer_loop.size() - 1;
        }
    }

    virtual bool is_aggregate() const override {
        return !_inner_loop.empty();
    }

    virtual bool is_count() const override {
        return _selectors.size() == 1
            && expr::find_in_expression<expr::function_call>(_selectors[0], [] (const expr::function_call& fc) {
                auto& func = std::get<shared_ptr<cql3::functions::function>>(fc.func);
                return func->name() == functions::function_name::native_function(functions::aggregate_fcts::COUNT_ROWS_FUNCTION_NAME);
            });
    }

    virtual bool is_reducible() const override {
        return boost::algorithm::all_of(
                _selectors,
               [] (const expr::expression& e) {
                    auto fc = expr::as_if<expr::function_call>(&e);
                    if (!fc) {
                        return false;
                    }
                    auto func = std::get<shared_ptr<cql3::functions::function>>(fc->func);
                    if (!func->is_aggregate()) {
                        return false;
                    }
                    auto agg_func = dynamic_pointer_cast<functions::aggregate_function>(std::move(func));
                    if (!agg_func->get_aggregate().state_reduction_function) {
                        return false;
                    }
                    // We only support transforming columns directly for parallel queries
                    if (!boost::algorithm::all_of(fc->args, expr::is<expr::column_value>)) {
                        return false;
                    }
                    return true;
                }
        );
    }

    virtual query::mapreduce_request::reductions_info get_reductions() const override {
        std::vector<query::mapreduce_request::reduction_type> types;
        std::vector<query::mapreduce_request::aggregation_info> infos;
        auto bad = [] {
            throw std::runtime_error("Selection doesn't have a reduction");
        };
        for (const auto& e : _selectors) {
            auto fc = expr::as_if<expr::function_call>(&e);
            if (!fc) {
                bad();
            }
            auto func = std::get<shared_ptr<cql3::functions::function>>(fc->func);
            if (!func->is_aggregate()) {
                bad();
            }
            auto agg_func = dynamic_pointer_cast<functions::aggregate_function>(std::move(func));

            auto type = (agg_func->name().name == "countRows") ? query::mapreduce_request::reduction_type::count : query::mapreduce_request::reduction_type::aggregate;

            std::vector<sstring> column_names;
            for (auto& arg : fc->args) {
                auto col = expr::as_if<expr::column_value>(&arg);
                if (!col) {
                    bad();
                }
                column_names.push_back(col->col->name_as_text());
            }

            auto info = query::mapreduce_request::aggregation_info {
                .name = agg_func->name(),
                .column_names = std::move(column_names),
            };

            types.push_back(type);
            infos.push_back(std::move(info));
        }
        return {types, infos};
    }

    virtual std::vector<shared_ptr<functions::function>> used_functions() const override {
        auto ret = std::vector<shared_ptr<functions::function>>();
        expr::recurse_until(expr::tuple_constructor{_selectors}, [&] (const expr::expression& e) {
            if (auto fc = expr::as_if<expr::function_call>(&e)) {
                auto func = std::get<shared_ptr<functions::function>>(fc->func);
                ret.push_back(func);
                if (auto agg_func = dynamic_pointer_cast<functions::aggregate_function>(std::move(func))) {
                    auto& agg = agg_func->get_aggregate();
                    if (agg.aggregation_function) {
                        ret.push_back(agg.aggregation_function);
                    }
                    if (agg.state_to_result_function) {
                        ret.push_back(agg.state_to_result_function);
                    }
                }
            }
            return false;
        });
        return ret;
    }

protected:
    class selectors_with_processing : public selectors {
    private:
        const selection_with_processing& _sel;
        std::vector<raw_value> _temporaries;
        bool _requires_thread;
    public:
        explicit selectors_with_processing(const selection_with_processing& sel)
            : _sel(sel)
            , _temporaries(_sel._initial_values_for_temporaries)
            , _requires_thread(boost::algorithm::any_of(sel._selectors, [] (const expr::expression& e) {
                return expr::find_in_expression<expr::function_call>(e, [] (const expr::function_call& fc) {
                    return std::get<shared_ptr<functions::function>>(fc.func)->requires_thread();
                });
             }))
        { }

        virtual bool requires_thread() const override {
            return _requires_thread;
        }

        virtual void reset() override {
            _temporaries = _sel._initial_values_for_temporaries;
        }

        virtual bool is_aggregate() const override {
            return !_sel._inner_loop.empty();
        }

        virtual std::vector<managed_bytes_opt> transform_input_row(result_set_builder& rs) override {
            std::vector<managed_bytes_opt> output_row;
            output_row.reserve(_sel._selectors.size());
            auto inputs = expr::evaluation_inputs{
                    .partition_key = rs.current_partition_key,
                    .clustering_key = rs.current_clustering_key,
                    .static_and_regular_columns = rs.current,
                    .selection = &_sel,
                    .options = nullptr,
                    .static_and_regular_timestamps = rs._timestamps,
                    .static_and_regular_ttls = rs._ttls,
                    .temporaries = {},
            };
            for (auto&& e : _sel._selectors) {
                auto out = expr::evaluate(e, inputs);
                output_row.emplace_back(std::move(out).to_managed_bytes_opt());
            }
            return output_row;
        }

        virtual std::vector<managed_bytes_opt> get_output_row() override {
            std::vector<managed_bytes_opt> output_row;
            output_row.reserve(_sel._outer_loop.size());
            auto inputs = expr::evaluation_inputs{
                    .partition_key = {},
                    .clustering_key = {},
                    .static_and_regular_columns = {},
                    .selection = &_sel,
                    .options = nullptr,
                    .static_and_regular_timestamps = {},
                    .static_and_regular_ttls = {},
                    .temporaries = _temporaries,
            };
            for (auto&& e : _sel._outer_loop) {
                auto out = expr::evaluate(e, inputs);
                output_row.emplace_back(std::move(out).to_managed_bytes_opt());
            }
            return output_row;
        }

        virtual void add_input_row(result_set_builder& rs) override {
            auto inputs = expr::evaluation_inputs{
                    .partition_key = rs.current_partition_key,
                    .clustering_key = rs.current_clustering_key,
                    .static_and_regular_columns = rs.current,
                    .selection = &_sel,
                    .options = nullptr,
                    .static_and_regular_timestamps = rs._timestamps,
                    .static_and_regular_ttls = rs._ttls,
                    .temporaries = _temporaries,
            };
            for (size_t i = 0; i != _sel._inner_loop.size(); ++i) {
                _temporaries[i] = expr::evaluate(_sel._inner_loop[i], inputs);
            }
        }

        std::vector<shared_ptr<functions::function>> used_functions() const {
            return _sel.used_functions();
        }
    };

    std::unique_ptr<selectors> new_selectors() const override  {
        return std::make_unique<selectors_with_processing>(*this);
    }
};

// Return a list of columns that "SELECT *" should show - these are all
// columns except potentially some that are is_hidden_from_cql() (currently,
// those can be the "virtual columns" used in materialized views).
// The list points to column_definition objects in the given schema_ptr,
// which can be used only as long as the caller keeps the schema_ptr alive.
std::vector<const column_definition*> selection::wildcard_columns(schema_ptr schema) {
    auto columns = schema->all_columns_in_select_order();
    // filter out hidden columns, which should not be seen by the
    // user when doing "SELECT *". We also disallow selecting them
    // individually (see column_identifier::new_selector_factory()).
    return boost::copy_range<std::vector<const column_definition*>>(
        columns |
        boost::adaptors::filtered([](const column_definition& c) {
            return !c.is_hidden_from_cql();
        }) |
        boost::adaptors::transformed([](const column_definition& c) {
            return &c;
        }));
}

::shared_ptr<selection> selection::wildcard(schema_ptr schema) {
    return simple_selection::make(schema, wildcard_columns(schema), true);
}

::shared_ptr<selection> selection::for_columns(schema_ptr schema, std::vector<const column_definition*> columns) {
    return simple_selection::make(schema, std::move(columns), false);
}

uint32_t selection::add_column_for_post_processing(const column_definition& c) {
    _columns.push_back(&c);
    _metadata->add_non_serialized_column(c.column_specification);
    return _columns.size() - 1;
}

::shared_ptr<selection> selection::from_selectors(data_dictionary::database db, schema_ptr schema, const sstring& ks, const std::vector<prepared_selector>& prepared_selectors) {
    std::vector<const column_definition*> defs;

    for (auto&& [sel, alias] : prepared_selectors) {
        expr::for_each_expression<expr::column_value>(sel, [&] (const expr::column_value& cv) {
            if (std::find(defs.begin(), defs.end(), cv.col) == defs.end()) {
                defs.push_back(cv.col);
            }
        });
    }

    auto metadata = collect_metadata(*schema, prepared_selectors);
    if (processes_selection(prepared_selectors) || prepared_selectors.size() != defs.size()) {
        return ::make_shared<selection_with_processing>(schema, std::move(defs), std::move(metadata),
                boost::copy_range<std::vector<expr::expression>>(prepared_selectors | boost::adaptors::transformed(std::mem_fn(&prepared_selector::expr))));
    } else {
        return ::make_shared<simple_selection>(schema, std::move(defs), std::move(metadata), false);
    }
}

std::vector<lw_shared_ptr<column_specification>>
selection::collect_metadata(const schema& schema, const std::vector<prepared_selector>& prepared_selectors) {
    std::vector<lw_shared_ptr<column_specification>> r;
    r.reserve(prepared_selectors.size());
    for (auto&& selector : prepared_selectors) {
        auto name = fmt::format("{:result_set_metadata}", selector.expr);
        auto col_id = ::make_shared<column_identifier>(name, /* keep_case */ true);
        lw_shared_ptr<column_specification> col_spec = make_lw_shared<column_specification>(
                schema.ks_name(), schema.cf_name(), std::move(col_id), expr::type_of(selector.expr));
        ::shared_ptr<column_identifier> alias = selector.alias;
        r.push_back(alias ? col_spec->with_alias(alias) : col_spec);
    }
    return r;
}

result_set_builder::result_set_builder(const selection& s, gc_clock::time_point now,
                                       std::vector<size_t> group_by_cell_indices,
                                       uint64_t limit)
    : _result_set(std::make_unique<result_set>(::make_shared<metadata>(*(s.get_result_metadata()))))
    , _selectors(s.new_selectors())
    , _group_by_cell_indices(std::move(group_by_cell_indices))
    , _limit(limit)
    , _last_group(_group_by_cell_indices.size())
    , _group_began(false)
    , _now(now)
{
    if (s._collect_timestamps) {
        _timestamps.resize(s._columns.size(), 0);
    }
    if (s._collect_TTLs) {
        _ttls.resize(s._columns.size(), 0);
    }
}

void result_set_builder::add_empty() {
    current.emplace_back();
    if (!_timestamps.empty()) {
        _timestamps[current.size() - 1] = api::missing_timestamp;
    }
    if (!_ttls.empty()) {
        _ttls[current.size() - 1] = -1;
    }
}

void result_set_builder::add(bytes_opt value) {
    current.emplace_back(std::move(value));
}

void result_set_builder::add(const column_definition& def, const query::result_atomic_cell_view& c) {
    current.emplace_back(get_value(def.type, c));
    if (!_timestamps.empty()) {
        _timestamps[current.size() - 1] = c.timestamp();
    }
    if (!_ttls.empty()) {
        gc_clock::duration ttl_left(-1);
        expiry_opt e = c.expiry();
        if (e) {
            ttl_left = *e - _now;
        }
        _ttls[current.size() - 1] = ttl_left.count();
    }
}

void result_set_builder::add_collection(const column_definition& def, bytes_view c) {
    current.emplace_back(to_bytes(c));
    // timestamps, ttls meaningless for collections
}

void result_set_builder::update_last_group() {
    _group_began = true;
    boost::transform(_group_by_cell_indices, _last_group.begin(), [this](size_t i) { return current[i]; });
}

bool result_set_builder::last_group_ended() const {
    if (!_group_began) {
        return false;
    }
    if (_last_group.empty()) {
        return !_selectors->is_aggregate();
    }
    using boost::adaptors::reversed;
    using boost::adaptors::transformed;
    return !boost::equal(
            _last_group | reversed,
            _group_by_cell_indices | reversed | transformed([this](size_t i) { return current[i]; }));
}

void result_set_builder::flush_selectors() {
    if (!_selectors->is_aggregate()) {
        // handled by process_current_row
        return;
    }
    if (_result_set->size() < _limit) {
        _result_set->add_row(_selectors->get_output_row());
        _selectors->reset();
    }
}

void result_set_builder::complete_row() {
    if (!_selectors->is_aggregate()) {
        // Fast path when not aggregating
        _result_set->add_row(_selectors->transform_input_row(*this));
        return;
    }
    if (last_group_ended()) {
        flush_selectors();
    }
    update_last_group();
    _selectors->add_input_row(*this);
}

void result_set_builder::start_new_row() {
    current.clear();
}

std::unique_ptr<result_set> result_set_builder::build() {
    if (_group_began && _selectors->is_aggregate()) {
        flush_selectors();
    }
    if (_result_set->empty() && _selectors->is_aggregate() && _group_by_cell_indices.empty()) {
        _result_set->add_row(_selectors->get_output_row());
    }
    return std::move(_result_set);
}

result_set_builder::restrictions_filter::restrictions_filter(::shared_ptr<const restrictions::statement_restrictions> restrictions,
        const query_options& options,
        uint64_t remaining,
        schema_ptr schema,
        uint64_t per_partition_limit,
        std::optional<partition_key> last_pkey,
        uint64_t rows_fetched_for_last_partition)
    : _restrictions(restrictions)
    , _options(options)
    , _skip_pk_restrictions(!_restrictions->pk_restrictions_need_filtering())
    , _skip_ck_restrictions(!_restrictions->ck_restrictions_need_filtering())
    , _remaining(remaining)
    , _schema(schema)
    , _per_partition_limit(per_partition_limit)
    , _per_partition_remaining(_per_partition_limit)
    , _rows_fetched_for_last_partition(rows_fetched_for_last_partition)
    , _last_pkey(std::move(last_pkey))
{ }

bool result_set_builder::restrictions_filter::do_filter(const selection& selection,
                                                         const std::vector<bytes>& partition_key,
                                                         const std::vector<bytes>& clustering_key,
                                                         const query::result_row_view& static_row,
                                                         const query::result_row_view* row) const {
    static logging::logger rlogger("restrictions_filter");

    if (_current_partition_key_does_not_match || _current_static_row_does_not_match || _remaining == 0 || _per_partition_remaining == 0) {
        return false;
    }

    const expr::expression& clustering_columns_restrictions = _restrictions->get_clustering_columns_restrictions();
    if (expr::contains_multi_column_restriction(clustering_columns_restrictions)) {
        clustering_key_prefix ckey = clustering_key_prefix::from_exploded(clustering_key);
        // FIXME: push to upper layer so it happens once per row
        auto static_and_regular_columns = expr::get_non_pk_values(selection, static_row, row);
        bool multi_col_clustering_satisfied = expr::is_satisfied_by(
                clustering_columns_restrictions,
                expr::evaluation_inputs{
                    .partition_key = partition_key,
                    .clustering_key = clustering_key,
                    .static_and_regular_columns = static_and_regular_columns,
                    .selection = &selection,
                    .options = &_options,
                });
        if (!multi_col_clustering_satisfied) {
            return false;
        }
    }

    auto static_row_iterator = static_row.iterator();
    auto row_iterator = row ? std::optional<query::result_row_view::iterator_type>(row->iterator()) : std::nullopt;
    const expr::single_column_restrictions_map& non_pk_restrictions_map = _restrictions->get_non_pk_restriction();
    for (auto&& cdef : selection.get_columns()) {
        switch (cdef->kind) {
        case column_kind::static_column:
            // fallthrough
        case column_kind::regular_column: {
            if (cdef->kind == column_kind::regular_column && !row_iterator) {
                continue;
            }
            auto restr_it = non_pk_restrictions_map.find(cdef);
            if (restr_it == non_pk_restrictions_map.end()) {
                continue;
            }
            const expr::expression& single_col_restriction = restr_it->second;
            // FIXME: push to upper layer so it happens once per row
            auto static_and_regular_columns = expr::get_non_pk_values(selection, static_row, row);
            bool regular_restriction_matches = expr::is_satisfied_by(
                    single_col_restriction,
                    expr::evaluation_inputs{
                        .partition_key = partition_key,
                        .clustering_key = clustering_key,
                        .static_and_regular_columns = static_and_regular_columns,
                        .selection = &selection,
                        .options = &_options,
                    });
            if (!regular_restriction_matches) {
                _current_static_row_does_not_match = (cdef->kind == column_kind::static_column);
                return false;
            }
            }
            break;
        case column_kind::partition_key: {
            if (_skip_pk_restrictions) {
                continue;
            }
            auto partition_key_restrictions_map = _restrictions->get_single_column_partition_key_restrictions();
            auto restr_it = partition_key_restrictions_map.find(cdef);
            if (restr_it == partition_key_restrictions_map.end()) {
                continue;
            }
            const expr::expression& single_col_restriction = restr_it->second;
            if (!expr::is_satisfied_by(
                        single_col_restriction,
                        expr::evaluation_inputs{
                            .partition_key = partition_key,
                            .clustering_key = clustering_key,
                            .static_and_regular_columns = {}, // partition key filtering only
                            .selection = &selection,
                            .options = &_options,
                        })) {
                _current_partition_key_does_not_match = true;
                return false;
            }
            }
            break;
        case column_kind::clustering_key: {
            if (_skip_ck_restrictions) {
                continue;
            }
            const expr::single_column_restrictions_map& clustering_key_restrictions_map =
                _restrictions->get_single_column_clustering_key_restrictions();
            auto restr_it = clustering_key_restrictions_map.find(cdef);
            if (restr_it == clustering_key_restrictions_map.end()) {
                continue;
            }
            if (clustering_key.empty()) {
                return false;
            }
            const expr::expression& single_col_restriction = restr_it->second;
            if (!expr::is_satisfied_by(
                        single_col_restriction,
                        expr::evaluation_inputs{
                            .partition_key = partition_key,
                            .clustering_key = clustering_key,
                            .static_and_regular_columns = {}, // clustering key checks only
                            .selection = &selection,
                            .options = &_options,
                        })) {
                return false;
            }
            }
            break;
        default:
            break;
        }
    }
    return true;
}

bool result_set_builder::restrictions_filter::operator()(const selection& selection,
                                                         const std::vector<bytes>& partition_key,
                                                         const std::vector<bytes>& clustering_key,
                                                         const query::result_row_view& static_row,
                                                         const query::result_row_view* row) const {
    const bool accepted = do_filter(selection, partition_key, clustering_key, static_row, row);
    if (!accepted) {
        ++_rows_dropped;
    } else {
        if (_remaining > 0) {
            --_remaining;
        }
        if (_per_partition_remaining > 0) {
            --_per_partition_remaining;
        }
    }
    return accepted;
}

void result_set_builder::restrictions_filter::reset(const partition_key* key) {
    _current_partition_key_does_not_match = false;
    _current_static_row_does_not_match = false;
    _rows_dropped = 0;
    _per_partition_remaining = _per_partition_limit;
    if (_is_first_partition_on_page && _per_partition_limit < std::numeric_limits<decltype(_per_partition_limit)>::max()) {
        // If any rows related to this key were also present in the previous query,
        // we need to take it into account as well.
        if (key && _last_pkey && _last_pkey->equal(*_schema, *key)) {
            _per_partition_remaining -= _rows_fetched_for_last_partition;
        }
        _is_first_partition_on_page = false;
    }
}

api::timestamp_type result_set_builder::timestamp_of(size_t idx) {
    return _timestamps[idx];
}

int32_t result_set_builder::ttl_of(size_t idx) {
    return _ttls[idx];
}

size_t result_set_builder::result_set_size() const {
    return _result_set->size();
}

bytes_opt result_set_builder::get_value(data_type t, query::result_atomic_cell_view c) {
    return {c.value().linearize()};
}

}

}
