/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "atomic_cell.hh"
#include "keys.hh"
#include "mutation.hh"
#include "mutation_partition_view.hh"


// Immutable, compact form of mutation.
//
// This form is primarily destined to be sent over the network channel.
// Regular mutation can't be deserialized because its complex data structures
// need schema reference at the time object is constructed. We can't lookup
// schema before we deserialize column family ID. Another problem is that even
// if we had the ID somehow, low level RPC layer doesn't know how to lookup
// the schema. Data can be wrapped in frozen_mutation without schema
// information, the schema is only needed to access some of the fields.
//
class frozen_mutation final {
private:
    bytes _bytes;
public:
    frozen_mutation(const mutation& m);
    explicit frozen_mutation(bytes&& b);
    frozen_mutation(frozen_mutation&& m) = default;

    bytes_view representation() const { return _bytes; }
    utils::UUID column_family_id() const;
    partition_key_view key(const schema& s) const;
    mutation_partition_view partition() const;
    mutation unfreeze(schema_ptr s) const;
};

frozen_mutation freeze(const mutation& m);
