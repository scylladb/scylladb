/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "types/types.hh"
#include "utils/rjson.hh"

bytes from_json_object(const abstract_type &t, const rjson::value& value);
sstring to_json_string(const abstract_type &t, bytes_view bv);
sstring to_json_string(const abstract_type &t, const managed_bytes_view& bv);

inline sstring to_json_string(const abstract_type &t, const bytes& b) {
    return to_json_string(t, bytes_view(b));
}

inline sstring to_json_string(const abstract_type& t, const bytes_opt& b) {
    return b ? to_json_string(t, *b) : "null";
}
