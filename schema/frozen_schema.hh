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

// To unfreeze view without base table added to schema registry
// we need base_info.
class frozen_schema_with_base_info : public frozen_schema {
public:
    frozen_schema_with_base_info(const schema_ptr& c);
    schema_ptr unfreeze(const db::schema_ctxt& ctxt) const;
private:
    // Set only for views.
    std::optional<db::view::base_dependent_view_info> base_info;
};
