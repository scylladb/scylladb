/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "selector.hh"
#include "raw_selector.hh"
#include "selectable-expr.hh"
#include "cql3/expr/expr-utils.hh"

namespace cql3 {

namespace selection {

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

bool
processes_selection(const prepared_selector& ps) {
    return selectable_processes_selection(ps.expr);
}

}

}
