/*
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/abstract_marker.hh"
#include "maps.hh"
#include "column_specification.hh"
#include "column_identifier.hh"
#include "to_string.hh"
#include <unordered_set>

namespace cql3 {

/**
 * Static helper methods and classes for sets.
 */
class sets {
    sets() = delete;
public:
    static lw_shared_ptr<column_specification> value_spec_of(const column_specification& column);

    class setter : public operation {
    public:
        setter(const column_definition& column, expr::expression e)
                : operation(column, std::move(e)) {
        }
        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
        static void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params, const column_definition& column, const expr::constant& value);
    };

    class adder : public operation {
    public:
        adder(const column_definition& column, expr::expression e)
            : operation(column, std::move(e)) {
        }
        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
        static void do_add(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params,
                const expr::constant& value, const column_definition& column);
    };

    // Note that this is reused for Map subtraction too (we subtract a set from a map)
    class discarder : public operation {
    public:
        discarder(const column_definition& column, expr::expression e)
            : operation(column, std::move(e)) {
        }
        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
    };

    class element_discarder : public operation {
    public:
        element_discarder(const column_definition& column, expr::expression e)
            : operation(column, std::move(e)) { }
        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
    };
};

}
