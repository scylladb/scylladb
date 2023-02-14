/*
 * Copyright 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "schema/schema_fwd.hh"
#include "mutation/frozen_mutation.hh"
#include "bytes_ostream.hh"

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
    schema_ptr unfreeze(const db::schema_ctxt&) const;
    const bytes_ostream& representation() const;
};
