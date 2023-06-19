/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "schema/schema_fwd.hh"
#include <seastar/core/shared_ptr.hh>
#include "cql3/selection/selector.hh"
#include "cql3/cql3_type.hh"
#include "cql3/functions/function.hh"
#include "cql3/functions/function_name.hh"

namespace cql3 {

namespace selection {

class selectable;

class selectable {
public:
    virtual ~selectable() {}
    virtual ::shared_ptr<selector::factory> new_selector_factory(data_dictionary::database db, schema_ptr schema, std::vector<const column_definition*>& defs) = 0;
    virtual sstring to_string() const = 0;
protected:
    static size_t add_and_get_index(const column_definition& def, std::vector<const column_definition*>& defs) {
        auto i = std::find(defs.begin(), defs.end(), &def);
        if (i != defs.end()) {
            return std::distance(defs.begin(), i);
        }
        defs.push_back(&def);
        return defs.size() - 1;
    }
public:
    class writetime_or_ttl;

    class with_anonymous_function;

    class with_field_selection;
};

std::ostream & operator<<(std::ostream &os, const selectable& s);

class selectable::with_anonymous_function : public selectable {
    shared_ptr<functions::function> _function;
    std::vector<shared_ptr<selectable>> _args;
public:
    with_anonymous_function(::shared_ptr<functions::function> f, std::vector<shared_ptr<selectable>> args)
        : _function(f), _args(std::move(args)) {
    }

    virtual sstring to_string() const override;

    virtual shared_ptr<selector::factory> new_selector_factory(data_dictionary::database db, schema_ptr s, std::vector<const column_definition*>& defs) override;
};

}

}
