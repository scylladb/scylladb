/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "query.hh"
#include "to_string.hh"

namespace query {

std::ostream& operator<<(std::ostream& out, const partition_slice& ps) {
    return out << "{"
        << "regular_cols=[" << join(", ", ps.regular_columns) << "]"
        << ", static_cols=[" << join(", ", ps.static_columns) << "]"
        << ", rows=[" << join(", ", ps.row_ranges) << "]"
        << "}";
}

std::ostream& operator<<(std::ostream& out, const read_command& r) {
    return out << "read_command{"
        << "ks=" << r.keyspace
        << ", cf=" << r.column_family
        << ", pks=[" << join(", ", r.partition_ranges) << "]"
        << ", slice=" << r.slice << ""
        << ", limit=" << r.row_limit << "}";
}

}
