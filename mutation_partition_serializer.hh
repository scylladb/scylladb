/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "database_fwd.hh"
#include "utils/data_output.hh"
#include "mutation_partition_view.hh"

class mutation_partition_serializer {
public:
    using count_type = uint32_t;

    static size_t size(const schema& schema, const mutation_partition& p);
    static void write(data_output& out, const schema& schema, const mutation_partition& p);
    static mutation_partition_view view(bytes_view v) { return mutation_partition_view::from_bytes(v); }
};
