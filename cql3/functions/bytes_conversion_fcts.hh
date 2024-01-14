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
#include "exceptions/exceptions.hh"
#include <seastar/core/print.hh>
#include <seastar/util/log.hh>
#include "cql3/cql3_type.hh"

namespace cql3 {

namespace functions {

// Most of the XAsBlob and blobAsX functions are basically no-op since everything is
// bytes internally. They only "trick" the type system.

inline
shared_ptr<function>
make_to_blob_function(data_type from_type) {
    auto name = from_type->as_cql3_type().to_string() + "asblob";
    return make_native_scalar_function<true>(name, bytes_type, { from_type },
            [] (std::span<const bytes_opt> parameters) {
        return parameters[0];
    });
}

inline
shared_ptr<function>
make_from_blob_function(data_type to_type) {
    sstring name = sstring("blobas") + to_type->as_cql3_type().to_string();
    return make_native_scalar_function<true>(name, to_type, { bytes_type },
            [name, to_type] (std::span<const bytes_opt> parameters) -> bytes_opt {
        auto&& val = parameters[0];
        if (!val) {
            return val;
        }
        try {
            to_type->validate(*val);
            return val;
        } catch (marshal_exception& e) {
            using namespace exceptions;
            throw invalid_request_exception(format("In call to function {}, value 0x{} is not a valid binary representation for type {}",
                    name, to_hex(val), to_type->as_cql3_type().to_string()));
        }
    });
}

inline
shared_ptr<function>
make_varchar_as_blob_fct() {
    return make_native_scalar_function<true>("varcharasblob", bytes_type, { utf8_type },
            [] (std::span<const bytes_opt> parameters) -> bytes_opt {
        return parameters[0];
    });
}

inline
shared_ptr<function>
make_blob_as_varchar_fct() {
    return make_native_scalar_function<true>("blobasvarchar", utf8_type, { bytes_type },
            [] (std::span<const bytes_opt> parameters) -> bytes_opt {
        return parameters[0];
    });
}

}
}
