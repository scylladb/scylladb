/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "query-request.hh"
#include "to_string.hh"
#include "bytes.hh"

namespace query {

const partition_range full_partition_range = partition_range::make_open_ended_both_sides();

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
        << ", slice=" << r.slice << ""
        << ", limit=" << r.row_limit << "}";
}

std::ostream& operator<<(std::ostream& out, const ring_position& pos) {
    out << "{" << pos.token();
    if (pos.has_key()) {
        out << ", " << *pos.key();
    }
    return out << "}";
}

}
