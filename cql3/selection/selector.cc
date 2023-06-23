/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "selector.hh"
#include "raw_selector.hh"
#include "cql3/column_identifier.hh"
#include "cql3/expr/expr-utils.hh"

namespace cql3 {

namespace selection {

lw_shared_ptr<column_specification>
selector::factory::get_column_specification(const schema& schema) const {
    return make_lw_shared<column_specification>(schema.ks_name(),
        schema.cf_name(),
        ::make_shared<column_identifier>(column_name(), true),
        get_return_type());
}

bool selector::requires_thread() const { return false; }

std::vector<prepared_selector>
raw_selector::to_prepared_selectors(const std::vector<::shared_ptr<raw_selector>>& raws,
        const schema& schema, data_dictionary::database db, const sstring& ks) {
    std::vector<prepared_selector> r;
    r.reserve(raws.size());
    for (auto&& raw : raws) {
        r.emplace_back(prepared_selector{
            .expr = expr::prepare_expression(raw->selectable_, db, ks, &schema, nullptr),
            .alias = raw->alias,
        });
    }
    return r;
}

std::vector<shared_ptr<selectable>>
to_selectables(std::span<const prepared_selector> selectors,
        const schema& schema, data_dictionary::database db, const sstring& ks) {
    std::vector<::shared_ptr<selectable>> r;
    r.reserve(selectors.size());
    for (auto&& ps : selectors) {
        r.emplace_back(prepare_selectable(schema, ps.expr, db, ks));
    }
    return r;
}

bool
processes_selection(const prepared_selector& ps) {
    return selectable_processes_selection(ps.expr);
}

}

}
