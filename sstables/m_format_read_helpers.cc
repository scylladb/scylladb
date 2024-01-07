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

std::ostream& operator<<(std::ostream& out, sstables::bound_kind_m kind) {
    switch (kind) {
    case sstables::bound_kind_m::excl_end:
        out << "excl_end";
        break;
    case sstables::bound_kind_m::incl_start:
        out << "incl_start";
        break;
    case sstables::bound_kind_m::excl_end_incl_start:
        out << "excl_end_incl_start";
        break;
    case sstables::bound_kind_m::static_clustering:
        out << "static_clustering";
        break;
    case sstables::bound_kind_m::clustering:
        out << "clustering";
        break;
    case sstables::bound_kind_m::incl_end_excl_start:
        out << "incl_end_excl_start";
        break;
    case sstables::bound_kind_m::incl_end:
        out << "incl_end";
        break;
    case sstables::bound_kind_m::excl_start:
        out << "excl_start";
        break;
    default:
        out << static_cast<unsigned>(kind);
    }
    return out;
}

}  // namespace sstables
