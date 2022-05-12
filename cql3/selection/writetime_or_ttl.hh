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

class selectable::writetime_or_ttl : public selectable {
public:
    shared_ptr<column_identifier> _id;
    bool _is_writetime;

    writetime_or_ttl(shared_ptr<column_identifier> id, bool is_writetime)
            : _id(std::move(id)), _is_writetime(is_writetime) {
    }

    virtual sstring to_string() const override;

    virtual shared_ptr<selector::factory> new_selector_factory(data_dictionary::database db, schema_ptr s, std::vector<const column_definition*>& defs) override;
};

}

}
