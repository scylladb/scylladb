/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <string>
#include <string_view>
#include <optional>
#include "types/types.hh"
#include "schema/schema_fwd.hh"
#include "keys.hh"
#include "utils/rjson.hh"
#include "utils/big_decimal.hh"

class position_in_partition;

namespace alternator {

enum class alternator_type : int8_t {
    S, B, BOOL, N, NOT_SUPPORTED_YET
};

struct type_info {
    alternator_type atype;
    data_type dtype;
};

struct type_representation {
    std::string ident;
    data_type dtype;
};

inline constexpr std::string_view scylla_paging_region(":scylla:paging:region");
inline constexpr std::string_view scylla_paging_weight(":scylla:paging:weight");

type_info type_info_from_string(std::string_view type);
type_representation represent_type(alternator_type atype);

bytes serialize_item(const rjson::value& item);
rjson::value deserialize_item(bytes_view bv);

std::string type_to_string(data_type type);

bytes get_key_column_value(const rjson::value& item, const column_definition& column);
bytes get_key_from_typed_value(const rjson::value& key_typed_value, const column_definition& column);
rjson::value json_key_column_value(bytes_view cell, const column_definition& column);

partition_key pk_from_json(const rjson::value& item, schema_ptr schema);
clustering_key ck_from_json(const rjson::value& item, schema_ptr schema);
position_in_partition pos_from_json(const rjson::value& item, schema_ptr schema);

// If v encodes a number (i.e., it is a {"N": [...]}, returns an object representing it.  Otherwise,
// raises ValidationException with diagnostic.
big_decimal unwrap_number(const rjson::value& v, std::string_view diagnostic);

// try_unwrap_number is like unwrap_number, but returns an unset optional
// when the given v does not encode a number.
std::optional<big_decimal> try_unwrap_number(const rjson::value& v);

// unwrap_bytes decodes byte value, on decoding failure it either raises api_error::serialization
// iff from_query is true or returns unset optional iff from_query is false.
// Therefore it's safe to dereference returned optional when called with from_query equal true.
std::optional<bytes> unwrap_bytes(const rjson::value& value, bool from_query);

// Check if a given JSON object encodes a set (i.e., it is a {"SS": [...]}, or "NS", "BS"
// and returns set's type and a pointer to that set. If the object does not encode a set,
// returned value is {"", nullptr}
const std::pair<std::string, const rjson::value*> unwrap_set(const rjson::value& v);

// Check if a given JSON object encodes a list (i.e., it is a {"L": [...]}
// and returns a pointer to that list.
const rjson::value* unwrap_list(const rjson::value& v);

// Take two JSON-encoded numeric values ({"N": "thenumber"}) and return the
// sum, again as a JSON-encoded number.
rjson::value number_add(const rjson::value& v1, const rjson::value& v2);
rjson::value number_subtract(const rjson::value& v1, const rjson::value& v2);
// Take two JSON-encoded set values (e.g. {"SS": [...the actual set]}) and
// return the sum of both sets, again as a set value.
rjson::value set_sum(const rjson::value& v1, const rjson::value& v2);
// Take two JSON-encoded set values (e.g. {"SS": [...the actual list]}) and
// return the difference of s1 - s2, again as a set value.
// DynamoDB does not allow empty sets, so if resulting set is empty, return
// an unset optional instead.
std::optional<rjson::value> set_diff(const rjson::value& v1, const rjson::value& v2);
// Take two JSON-encoded list values (remember that a list value is
// {"L": [...the actual list]}) and return the concatenation, again as
// a list value.
// Returns a null value if one of the arguments is not actually a list.
rjson::value list_concatenate(const rjson::value& v1, const rjson::value& v2);

namespace internal {
struct magnitude_and_precision {
    int magnitude;
    int precision;
};
magnitude_and_precision get_magnitude_and_precision(std::string_view);
}

}
