/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "query.hh"
#include "to_string.hh"
#include "bytes.hh"

namespace query {

std::ostream& operator<<(std::ostream& out, const partition_slice& ps) {
    return out << "{"
        << "regular_cols=[" << join(", ", ps.regular_columns) << "]"
        << ", static_cols=[" << join(", ", ps.static_columns) << "]"
        << ", rows=[" << join(", ", ps.row_ranges) << "]"
        << ", options=" << sprint("%x", ps.options.mask()) // FIXME: pretty print options
        << "}";
}

std::ostream& operator<<(std::ostream& out, const read_command& r) {
    return out << "read_command{"
        << "cf_id=" << r.cf_id
        << ", pks=[" << join(", ", r.partition_ranges) << "]"
        << ", slice=" << r.slice << ""
        << ", limit=" << r.row_limit << "}";
}

}
