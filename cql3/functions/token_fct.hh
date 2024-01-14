/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "native_scalar_function.hh"
#include "dht/i_partitioner.hh"
#include "unimplemented.hh"

namespace cql3 {
namespace functions {

class token_fct: public native_scalar_function {
private:
    schema_ptr _schema;

public:
    token_fct(schema_ptr s)
            : native_scalar_function("token",
                    dht::token::get_token_validator(),
                    s->partition_key_type()->types())
                    , _schema(s) {
    }

    bytes_opt execute(std::span<const bytes_opt> parameters) override {
        if (std::any_of(parameters.begin(), parameters.end(), [](const auto& param){ return !param; })) {
            return std::nullopt;
        }
        auto key = partition_key::from_optional_exploded(*_schema, parameters);
        auto tok = dht::get_token(*_schema, key);
        warn(unimplemented::cause::VALIDATION);
        return tok.data();
    }
};

}
}
