/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <concepts>

#include <seastar/core/sstring.hh>

#include "schema/schema_fwd.hh"

namespace cql3 {

// Identified by table_id; names are kept because drop notifications only carry names.
struct dependent_table {
    table_id id;
    sstring ks_name;
    sstring cf_name;

    // Template so schema.hh isn't needed here: instantiated only where schema is complete.
    template <std::same_as<schema> S>
    explicit dependent_table(const S& s) : id(s.id()), ks_name(s.ks_name()), cf_name(s.cf_name()) {}
};

}
