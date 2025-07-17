/*
 * Copyright 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "schema/schema_fwd.hh"
#include "mutation/frozen_mutation.hh"
#include "bytes_ostream.hh"
#include "db/view/base_info.hh"

namespace db {
class schema_ctxt;
}

// Transport for schema_ptr across shards/nodes.
// It's safe to access from another shard by const&.
class frozen_schema {
    bytes_ostream _data;
public:
    explicit frozen_schema(bytes_ostream);
    frozen_schema(const schema_ptr&);
    frozen_schema(frozen_schema&&) = default;
    frozen_schema(const frozen_schema&) = default;
    frozen_schema& operator=(const frozen_schema&) = default;
    frozen_schema& operator=(frozen_schema&&) = default;
    schema_ptr unfreeze(const db::schema_ctxt&, std::optional<db::view::base_dependent_view_info> base_info = {}) const;
    const bytes_ostream& representation() const;
};

// A frozen schema with additional information that is needed to be transported
// with it to be used for unfreezing it.
struct extended_frozen_schema {
    extended_frozen_schema(const schema_ptr& c);
    extended_frozen_schema(frozen_schema fs, std::optional<db::view::base_dependent_view_info> base_info);
    schema_ptr unfreeze(const db::schema_ctxt& ctxt) const;

    frozen_schema fs;
    std::optional<db::view::base_dependent_view_info> base_info; // Set only for views.
};
