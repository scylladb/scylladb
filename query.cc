/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "query.hh"

namespace query {

template <typename T>
sstring join(const std::vector<T>& v, sstring sep) {
    std::ostringstream oss;
    size_t left = v.size();
    for (auto&& item : v) {
        oss << item;
        if (--left) {
            oss << ", ";
        }
    }
    return oss.str();
}

std::ostream& operator<<(std::ostream& out, const partition_slice& ps) {
    return out << "{"
        << "regular_cols=[" << join(ps.regular_columns, ", ") << "]"
        << ", static_cols=[" << join(ps.static_columns, ", ") << "]"
        << ", rows=[" << join(ps.row_ranges, ", ") << "]"
        << "}";
}

std::ostream& operator<<(std::ostream& out, const read_command& r) {
    return out << "read_command{"
        << "ks=" << r.keyspace
        << ", cf=" << r.column_family
        << ", pks=[" << join(r.partition_ranges, ", ") << "]"
        << ", slice=" << r.slice << ""
        << ", limit=" << r.row_limit << "}";
}

}
