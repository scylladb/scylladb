/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "vint-serialization.hh"
#include "sstables/m_format_read_helpers.hh"
#include "sstables/exceptions.hh"
#include "sstables/random_access_reader.hh"
#include "sstables/mx/types.hh"
#include <fmt/format.h>

namespace sstables {

static void check_buf_size(temporary_buffer<char>& buf, size_t expected) {
    if (buf.size() < expected) {
        throw bufsize_mismatch_exception(buf.size(), expected);
    }
}

template <typename T>
inline future<T> read_vint_impl(random_access_reader& in) {
    using vint_type = std::conditional_t<std::is_unsigned_v<T>, unsigned_vint, signed_vint>;
    return in.read_exactly(1).then([&in] (auto&& buf) {
        check_buf_size(buf, 1);
        vint_size_type len = vint_type::serialized_size_from_first_byte(*buf.begin());
        temporary_buffer<char> bytes(len);
        std::copy_n(buf.begin(), 1, bytes.get_write());
        return in.read_exactly(len - 1).then([len, bytes = std::move(bytes)] (temporary_buffer<char> buf) mutable {
            check_buf_size(buf, len - 1);
            std::copy_n(buf.begin(), len - 1, bytes.get_write() + 1);
            return vint_type::deserialize(
                bytes_view(reinterpret_cast<bytes::value_type*>(bytes.get_write()), len));
        });
    });
}

future<uint64_t> read_unsigned_vint(random_access_reader& in) {
    return read_vint_impl<uint64_t>(in);
}

future<int64_t> read_signed_vint(random_access_reader& in) {
    return read_vint_impl<int64_t>(in);
}

}  // namespace sstables

auto fmt::formatter<sstables::bound_kind_m>::format(sstables::bound_kind_m kind, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    std::string_view name;
    switch (kind) {
    using enum sstables::bound_kind_m;
    case excl_end:
        name = "excl_end";
        break;
    case incl_start:
        name = "incl_start";
        break;
    case excl_end_incl_start:
        name = "excl_end_incl_start";
        break;
    case static_clustering:
        name = "static_clustering";
        break;
    case clustering:
        name = "clustering";
        break;
    case incl_end_excl_start:
        name = "incl_end_excl_start";
        break;
    case incl_end:
        name = "incl_end";
        break;
    case excl_start:
        name = "excl_start";
        break;
    default:
        return fmt::format_to(ctx.out(), "{}", underlying(kind));
    }
    return fmt::format_to(ctx.out(), "{}", name);
}
