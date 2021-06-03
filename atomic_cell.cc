/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "atomic_cell.hh"
#include "atomic_cell_or_collection.hh"
#include "counters.hh"
#include "types.hh"

atomic_cell atomic_cell::make_dead(api::timestamp_type timestamp, gc_clock::time_point deletion_time) {
    return atomic_cell_type::make_dead(timestamp, deletion_time);
}

atomic_cell atomic_cell::make_live(const abstract_type& type, api::timestamp_type timestamp, bytes_view value, atomic_cell::collection_member cm) {
    return atomic_cell_type::make_live(timestamp, single_fragment_range(value));
}

atomic_cell atomic_cell::make_live(const abstract_type& type, api::timestamp_type timestamp, managed_bytes_view value, atomic_cell::collection_member cm) {
    return atomic_cell_type::make_live(timestamp, fragment_range(value));
}

atomic_cell atomic_cell::make_live(const abstract_type& type, api::timestamp_type timestamp, ser::buffer_view<bytes_ostream::fragment_iterator> value, atomic_cell::collection_member cm) {
    return atomic_cell_type::make_live(timestamp, value);
}

atomic_cell atomic_cell::make_live(const abstract_type& type, api::timestamp_type timestamp, const fragmented_temporary_buffer::view& value, collection_member cm)
{
    return atomic_cell_type::make_live(timestamp, value);
}

atomic_cell atomic_cell::make_live(const abstract_type& type, api::timestamp_type timestamp, bytes_view value,
                             gc_clock::time_point expiry, gc_clock::duration ttl, atomic_cell::collection_member cm) {
    return atomic_cell_type::make_live(timestamp, single_fragment_range(value), expiry, ttl);
}

atomic_cell atomic_cell::make_live(const abstract_type& type, api::timestamp_type timestamp, managed_bytes_view value,
                             gc_clock::time_point expiry, gc_clock::duration ttl, atomic_cell::collection_member cm) {
    return atomic_cell_type::make_live(timestamp, fragment_range(value), expiry, ttl);
}

atomic_cell atomic_cell::make_live(const abstract_type& type, api::timestamp_type timestamp, ser::buffer_view<bytes_ostream::fragment_iterator> value,
                             gc_clock::time_point expiry, gc_clock::duration ttl, atomic_cell::collection_member cm) {
    return atomic_cell_type::make_live(timestamp, value, expiry, ttl);
}

atomic_cell atomic_cell::make_live(const abstract_type& type, api::timestamp_type timestamp, const fragmented_temporary_buffer::view& value,
                                   gc_clock::time_point expiry, gc_clock::duration ttl, collection_member cm)
{
    return atomic_cell_type::make_live(timestamp, value, expiry, ttl);
}

atomic_cell atomic_cell::make_live_counter_update(api::timestamp_type timestamp, int64_t value) {
    return atomic_cell_type::make_live_counter_update(timestamp, value);
}

atomic_cell atomic_cell::make_live_uninitialized(const abstract_type& type, api::timestamp_type timestamp, size_t size) {
    return atomic_cell_type::make_live_uninitialized(timestamp, size);
}

atomic_cell::atomic_cell(const abstract_type& type, atomic_cell_view other)
    : _data(other._view) {
    set_view(_data);
}

atomic_cell_or_collection atomic_cell_or_collection::copy(const abstract_type& type) const {
    if (_data.empty()) {
        return atomic_cell_or_collection();
    }
    return atomic_cell_or_collection(managed_bytes(_data));
}

atomic_cell_or_collection::atomic_cell_or_collection(const abstract_type& type, atomic_cell_view acv)
    : _data(acv._view)
{
}

bool atomic_cell_or_collection::equals(const abstract_type& type, const atomic_cell_or_collection& other) const
{
    if (_data.empty() || other._data.empty()) {
        return _data.empty() && other._data.empty();
    }

    if (type.is_atomic()) {
        auto a = atomic_cell_view::from_bytes(type, _data);
        auto b = atomic_cell_view::from_bytes(type, other._data);
        if (a.timestamp() != b.timestamp()) {
            return false;
        }
        if (a.is_live() != b.is_live()) {
            return false;
        }
        if (a.is_live()) {
            if (a.is_counter_update() != b.is_counter_update()) {
                return false;
            }
            if (a.is_counter_update()) {
                return a.counter_update_value() == b.counter_update_value();
            }
            if (a.is_live_and_has_ttl() != b.is_live_and_has_ttl()) {
                return false;
            }
            if (a.is_live_and_has_ttl()) {
                if (a.ttl() != b.ttl() || a.expiry() != b.expiry()) {
                    return false;
                }
            }
            return a.value() == b.value();
        }
        return a.deletion_time() == b.deletion_time();
    } else {
        return as_collection_mutation().data == other.as_collection_mutation().data;
    }
}

size_t atomic_cell_or_collection::external_memory_usage(const abstract_type& t) const
{
    return _data.external_memory_usage();
}

std::ostream&
operator<<(std::ostream& os, const atomic_cell_view& acv) {
    if (acv.is_live()) {
        return fmt_print(os, "atomic_cell{{{},ts={:d},expiry={:d},ttl={:d}}}",
            acv.is_counter_update()
                    ? "counter_update_value=" + to_sstring(acv.counter_update_value())
                    : to_hex(to_bytes(acv.value())),
            acv.timestamp(),
            acv.is_live_and_has_ttl() ? acv.expiry().time_since_epoch().count() : -1,
            acv.is_live_and_has_ttl() ? acv.ttl().count() : 0);
    } else {
        return fmt_print(os, "atomic_cell{{DEAD,ts={:d},deletion_time={:d}}}",
            acv.timestamp(), acv.deletion_time().time_since_epoch().count());
    }
}

std::ostream&
operator<<(std::ostream& os, const atomic_cell& ac) {
    return os << atomic_cell_view(ac);
}

std::ostream&
operator<<(std::ostream& os, const atomic_cell_view::printer& acvp) {
    auto& type = acvp._type;
    auto& acv = acvp._cell;
    if (acv.is_live()) {
        std::ostringstream cell_value_string_builder;
        if (type.is_counter()) {
            if (acv.is_counter_update()) {
                cell_value_string_builder << "counter_update_value=" << acv.counter_update_value();
            } else {
                cell_value_string_builder << "shards: ";
                auto ccv = counter_cell_view(acv);
                cell_value_string_builder << ::join(", ", ccv.shards());
            }
        } else {
            cell_value_string_builder << type.to_string(to_bytes(acv.value()));
        }
        return fmt_print(os, "atomic_cell{{{},ts={:d},expiry={:d},ttl={:d}}}",
            cell_value_string_builder.str(),
            acv.timestamp(),
            acv.is_live_and_has_ttl() ? acv.expiry().time_since_epoch().count() : -1,
            acv.is_live_and_has_ttl() ? acv.ttl().count() : 0);
    } else {
        return fmt_print(os, "atomic_cell{{DEAD,ts={:d},deletion_time={:d}}}",
            acv.timestamp(), acv.deletion_time().time_since_epoch().count());
    }
}

std::ostream&
operator<<(std::ostream& os, const atomic_cell::printer& acp) {
    return operator<<(os, static_cast<const atomic_cell_view::printer&>(acp));
}

std::ostream& operator<<(std::ostream& os, const atomic_cell_or_collection::printer& p) {
    if (p._cell._data.empty()) {
        return os << "{ null atomic_cell_or_collection }";
    }
    os << "{ ";
    if (p._cdef.type->is_multi_cell()) {
        os << "collection ";
        auto cmv = p._cell.as_collection_mutation();
        os << collection_mutation_view::printer(*p._cdef.type, cmv);
    } else {
        os << atomic_cell_view::printer(*p._cdef.type, p._cell.as_atomic_cell(p._cdef));
    }
    return os << " }";
}
