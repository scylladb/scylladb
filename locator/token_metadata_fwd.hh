/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/core/shared_ptr.hh>
#include "seastarx.hh"

namespace locator {

template <typename NodeId>
class generic_token_metadata;
using token_metadata = generic_token_metadata<gms::inet_address>;
using token_metadata_ptr = lw_shared_ptr<const token_metadata>;
using mutable_token_metadata_ptr = lw_shared_ptr<token_metadata>;
using token_metadata2 = generic_token_metadata<host_id>;
using token_metadata2_ptr = lw_shared_ptr<const token_metadata2>;
using mutable_token_metadata2_ptr = lw_shared_ptr<token_metadata2>;

} // namespace locator
