/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "selector.hh"
#include "raw_selector.hh"
#include "cql3/column_identifier.hh"

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

std::vector<::shared_ptr<selectable>>
raw_selector::to_selectables(const std::vector<::shared_ptr<raw_selector>>& raws,
        const schema& schema, data_dictionary::database db, const sstring& ks) {
    std::vector<::shared_ptr<selectable>> r;
    r.reserve(raws.size());
    for (auto&& raw : raws) {
        r.emplace_back(prepare_selectable(schema, raw->selectable_, db, ks));
    }
    return r;
}

bool
raw_selector::processes_selection() const {
    return selectable_processes_selection(selectable_);
}

}

}
