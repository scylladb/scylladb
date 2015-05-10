/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "database_fwd.hh"
#include "mutation_partition_visitor.hh"

// View on serialized mutation partition. See mutation_partition_serializer.
class mutation_partition_view {
    bytes_view _bytes;
private:
    mutation_partition_view(bytes_view v)
        : _bytes(v)
    { }
public:
    static mutation_partition_view from_bytes(bytes_view v) { return { v }; }
    void accept(const schema& schema, mutation_partition_visitor& visitor) const;
};
