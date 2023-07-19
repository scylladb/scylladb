/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/statements/raw/cf_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/attributes.hh"
#include "db/config.hh"
#include <seastar/core/shared_ptr.hh>

namespace cql3 {

namespace selection {
    class selection;
    class raw_selector;
    class prepared_selector;
} // namespace selection

namespace statements {

namespace raw {

/**
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 *
 */
class select_statement : public cf_statement
{
public:
    // Ordering of selected values as defined by the basic comparison order.
    // Even for a column that by default has ordering 4, 3, 2, 1 ordering it in ascending order will result in 1, 2, 3, 4.
    enum class ordering {
        ascending,
        descending
    };
    class parameters final {
    public:
        using orderings_type = std::vector<std::pair<shared_ptr<column_identifier::raw>, ordering>>;
        enum class statement_subtype { REGULAR, JSON, PRUNE_MATERIALIZED_VIEW, MUTATION_FRAGMENTS };
    private:
        const orderings_type _orderings;
        const bool _is_distinct;
        const bool _allow_filtering;
        const statement_subtype _statement_subtype;
        bool _bypass_cache = false;
    public:
        parameters();
        parameters(orderings_type orderings,
            bool is_distinct,
            bool allow_filtering);
        parameters(orderings_type orderings,
            bool is_distinct,
            bool allow_filtering,
            statement_subtype statement_subtype,
            bool bypass_cache);
        bool is_distinct() const;
        bool allow_filtering() const;
        bool is_json() const;
        bool is_mutation_fragments() const;
        bool bypass_cache() const;
        bool is_prune_materialized_view() const;
        orderings_type const& orderings() const;
    };
    template<typename T>
    using compare_fn = std::function<bool(const T&, const T&)>;

    using result_row_type = std::vector<managed_bytes_opt>;
    using ordering_comparator_type = compare_fn<result_row_type>;
private:
    using prepared_orderings_type = std::vector<std::pair<const column_definition*, ordering>>;
private:
    lw_shared_ptr<const parameters> _parameters;
    std::vector<::shared_ptr<selection::raw_selector>> _select_clause;
    expr::expression _where_clause;
    std::optional<expr::expression> _limit;
    std::optional<expr::expression> _per_partition_limit;
    std::vector<::shared_ptr<cql3::column_identifier::raw>> _group_by_columns;
    std::unique_ptr<cql3::attributes::raw> _attrs;
public:
    select_statement(cf_name cf_name,
            lw_shared_ptr<const parameters> parameters,
            std::vector<::shared_ptr<selection::raw_selector>> select_clause,
            expr::expression where_clause,
            std::optional<expr::expression> limit,
            std::optional<expr::expression> per_partition_limit,
            std::vector<::shared_ptr<cql3::column_identifier::raw>> group_by_columns,
            std::unique_ptr<cql3::attributes::raw> attrs);

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override {
        return prepare(db, stats, false);
    }
    std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats, bool for_view);
private:
    std::vector<selection::prepared_selector> maybe_jsonize_select_clause(std::vector<selection::prepared_selector> select, data_dictionary::database db, schema_ptr schema);
    ::shared_ptr<restrictions::statement_restrictions> prepare_restrictions(
        data_dictionary::database db,
        schema_ptr schema,
        prepare_context& ctx,
        ::shared_ptr<selection::selection> selection,
        bool for_view = false,
        bool allow_filtering = false,
        restrictions::check_indexes do_check_indexes = restrictions::check_indexes::yes);

    /** Returns an expression for the limit or nullopt if no limit is set */
    std::optional<expr::expression> prepare_limit(data_dictionary::database db, prepare_context& ctx, const std::optional<expr::expression>& limit);

    // Checks whether it is legal to have ORDER BY in this statement
    static void verify_ordering_is_allowed(const parameters& params, const restrictions::statement_restrictions& restrictions);

    void handle_unrecognized_ordering_column(const column_identifier& column) const;

    // Processes ORDER BY column orderings, converts column_identifiers to column_defintions
    prepared_orderings_type prepare_orderings(const schema& schema) const;

    void verify_ordering_is_valid(const prepared_orderings_type&, const schema&, const restrictions::statement_restrictions& restrictions) const;

    // Checks whether this ordering reverses all results.
    // We only allow leaving select results unchanged or reversing them.
    bool is_ordering_reversed(const prepared_orderings_type&) const;

    select_statement::ordering_comparator_type get_ordering_comparator(
        const prepared_orderings_type&,
        selection::selection& selection,
        const restrictions::statement_restrictions& restrictions);

    static void validate_distinct_selection(const schema& schema,
        const selection::selection& selection,
        const restrictions::statement_restrictions& restrictions);

    /** If ALLOW FILTERING was not specified, this verifies that it is not needed */
    void check_needs_filtering(
            const restrictions::statement_restrictions& restrictions,
            db::tri_mode_restriction_t::mode strict_allow_filtering,
            std::vector<sstring>& warnings);

    void ensure_filtering_columns_retrieval(data_dictionary::database db,
                                            selection::selection& selection,
                                            const restrictions::statement_restrictions& restrictions);

    /// Returns indices of GROUP BY cells in fetched rows.
    std::vector<size_t> prepare_group_by(const schema& schema, selection::selection& selection) const;

    bool contains_alias(const column_identifier& name) const;

    lw_shared_ptr<column_specification> limit_receiver(bool per_partition = false);

#if 0
    public:
        virtual sstring to_string() override {
            return sstring("raw_statement(")
                + "name=" + cf_name->to_string()
                + ", selectClause=" + to_string(_select_clause)
                + ", whereClause=" + to_string(_where_clause)
                + ", isDistinct=" + to_string(_parameters->is_distinct())
                + ", isJson=" + to_string(_parameters->is_json())
                + ")";
        }
    };
#endif
};

}

}

}
