/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "serializer_impl.hh"

namespace ser {

logging::logger serlog("serializer");

} // namespace ser

namespace utils {

managed_bytes_view
buffer_view_to_managed_bytes_view(ser::buffer_view<bytes_ostream::fragment_iterator> bv) {
    auto impl = bv.extract_implementation();
    return build_managed_bytes_view_from_internals(
            impl.current,
            impl.next.extract_implementation().current_chunk,
            impl.size
    );
}

managed_bytes_view_opt
buffer_view_to_managed_bytes_view(std::optional<ser::buffer_view<bytes_ostream::fragment_iterator>> bvo) {
    if (!bvo) {
        return std::nullopt;
    }
    return buffer_view_to_managed_bytes_view(*bvo);
}


} // namespace utils
