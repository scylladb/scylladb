/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "seastarx.hh"

namespace db {
namespace index {

/**
 * Abstract base class for different types of secondary indexes.
 *
 * Do not extend this directly, please pick from PerColumnSecondaryIndex or PerRowSecondaryIndex
 */
class secondary_index {
public:
    static const sstring custom_class_option_name;
    static const sstring index_version_option_name;

};

}
}
