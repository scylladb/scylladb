/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "types/types.hh"
#include "utils/rjson.hh"

struct value_and_type {
    sstring value;
    rjson::type type; // type of the top-level value
};

bytes from_json_object(const abstract_type &t, const rjson::value& value);

value_and_type to_json_value(const abstract_type &t, bytes_view bv);
value_and_type to_json_value(const abstract_type &t, const managed_bytes_view& bv);

inline sstring to_json_string(const abstract_type &t, bytes_view bv) {
    return to_json_value(t, bv).value;
}

inline sstring to_json_string(const abstract_type &t, const managed_bytes_view& bv) {
    return to_json_value(t, bv).value;
}

inline sstring to_json_string(const abstract_type &t, const bytes& b) {
    return to_json_string(t, bytes_view(b));
}

inline sstring to_json_string(const abstract_type& t, const bytes_opt& b) {
    return b ? to_json_string(t, *b) : "null";
}
