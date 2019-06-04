/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <string>
#include "types.hh"
#include "schema.hh"
#include "keys.hh"
#include "json.hh"

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

type_info type_info_from_string(std::string type);
type_representation represent_type(alternator_type atype);

bytes serialize_item(const Json::Value& item);
Json::Value deserialize_item(bytes_view bv);

std::string type_to_string(data_type type);

bytes get_key_column_value(const Json::Value& item, const column_definition& column);
Json::Value json_key_column_value(bytes_view cell, const column_definition& column);

partition_key pk_from_json(const Json::Value& item, schema_ptr schema);
clustering_key ck_from_json(const Json::Value& item, schema_ptr schema);

}
