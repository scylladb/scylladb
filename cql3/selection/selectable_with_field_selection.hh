/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */


#pragma once

#include "selectable.hh"
#include "cql3/column_identifier.hh"

namespace cql3 {

namespace selection {

class selectable::with_field_selection : public selectable {
public:
    shared_ptr<selectable> _selected;
    shared_ptr<column_identifier> _field;
    size_t _field_idx;
public:
    with_field_selection(shared_ptr<selectable> selected, shared_ptr<column_identifier> field, size_t field_idx)
            : _selected(std::move(selected)), _field(std::move(field)), _field_idx(field_idx) {
    }

    virtual sstring to_string() const override;

    virtual shared_ptr<selector::factory> new_selector_factory(data_dictionary::database db, schema_ptr s, std::vector<const column_definition*>& defs) override;
};

}

}
