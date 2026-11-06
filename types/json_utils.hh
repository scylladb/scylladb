/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "types/types.hh"
#include "utils/rjson.hh"

bytes from_json_object(const abstract_type &t, const rjson::value& value);
sstring to_json_string(const abstract_type &t, bytes_view bv);
sstring to_json_string(const abstract_type &t, const managed_bytes_view& bv);

// Returns the JSON string (with the surrounding quotes, and escaping) which
// represents the given column or user-defined-type field name as the key of
// a JSON object, as SELECT JSON prints and INSERT JSON reads it: A name with
// uppercase letters is case-sensitive, so it is wrapped in an additional pair
// of quotes (e.g., "\"Foo\""). So is a name which itself begins and ends with
// a quote, because INSERT JSON removes one pair of quotes from such a name.
sstring json_key_for_name(std::string_view name);

inline sstring to_json_string(const abstract_type &t, const bytes& b) {
    return to_json_string(t, bytes_view(b));
}

inline sstring to_json_string(const abstract_type& t, const bytes_opt& b) {
    return b ? to_json_string(t, *b) : "null";
}

rjson::type to_json_type(const abstract_type &t, managed_bytes_view bv);
