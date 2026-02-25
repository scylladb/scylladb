/*
 * Copyright 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils/tagged_integer.hh"

namespace utils {

template<typename Tag, typename ValueType>
struct tagged_integer final {
    ValueType value();
};

} // namespace utils
