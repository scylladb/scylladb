/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "atomic_cell.hh"
#include "atomic_cell_or_collection.hh"
#include "counters.hh"
#include "types/types.hh"

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

// Based on Cassandra's resolveRegular function:
//  - https://github.com/apache/cassandra/blob/e4f31b73c21b04966269c5ac2d3bd2562e5f6c63/src/java/org/apache/cassandra/db/rows/Cells.java#L79-L119
//
// Note: the ordering algorithm for cell is the same as for rows,
// except that the cell value is used to break a tie in case all other attributes are equal.
// See compare_row_marker_for_merge.
std::strong_ordering
compare_atomic_cell_for_merge(atomic_cell_view left, atomic_cell_view right) {
    // Largest write timestamp wins.
    if (left.timestamp() != right.timestamp()) {
        return left.timestamp() <=> right.timestamp();
    }
    // Tombstones always win reconciliation with live cells of the same timestamp
    if (left.is_live() != right.is_live()) {
        return left.is_live() ? std::strong_ordering::less : std::strong_ordering::greater;
    }
    if (left.is_live()) {
        // Prefer expiring cells (which will become tombstones at some future date) over live cells.
        // See https://issues.apache.org/jira/browse/CASSANDRA-14592
        if (left.is_live_and_has_ttl() != right.is_live_and_has_ttl()) {
            return left.is_live_and_has_ttl() ? std::strong_ordering::greater : std::strong_ordering::less;
        }
        // If both are expiring, choose the cell with the latest expiry or derived write time.
        if (left.is_live_and_has_ttl()) {
            // Prefer cell with latest expiry
            if (left.expiry() != right.expiry()) {
                return left.expiry() <=> right.expiry();
            } else if (right.ttl() != left.ttl()) {
                // The cell write time is derived by (expiry - ttl).
                // Prefer the cell that was written later,
                // so it survives longer after it expires, until purged,
                // as it become purgeable gc_grace_seconds after it was written.
                //
                // Note that this is an extension to Cassandra's algorithm
                // which stops at the expiration time, and if equal,
                // move forward to compare the cell values.
                return right.ttl() <=> left.ttl();
            }
        }
        // The cell with the largest value wins, if all other attributes of the cells are identical.
        // This is quite arbitrary, but still required to break the tie in a deterministic way.
        return compare_unsigned(left.value(), right.value());
    } else {
        // Both are deleted

        // Origin compares big-endian serialized deletion time. That's because it
        // delegates to AbstractCell.reconcile() which compares values after
        // comparing timestamps, which in case of deleted cells will hold
        // serialized expiry.
        return (uint64_t) left.deletion_time().time_since_epoch().count()
                <=> (uint64_t) right.deletion_time().time_since_epoch().count();
    }
    return std::strong_ordering::equal;
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

auto fmt::formatter<atomic_cell_view>::format(const atomic_cell_view& acv, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    if (acv.is_live()) {
        return fmt::format_to(ctx.out(), "atomic_cell{{{},ts={:d},expiry={:d},ttl={:d}}}",
            acv.is_counter_update()
                    ? "counter_update_value=" + to_sstring(acv.counter_update_value())
                    : to_hex(to_bytes(acv.value())),
            acv.timestamp(),
            acv.is_live_and_has_ttl() ? acv.expiry().time_since_epoch().count() : -1,
            acv.is_live_and_has_ttl() ? acv.ttl().count() : 0);
    } else {
        return fmt::format_to(ctx.out(), "atomic_cell{{DEAD,ts={:d},deletion_time={:d}}}",
            acv.timestamp(), acv.deletion_time().time_since_epoch().count());
    }
}

auto fmt::formatter<atomic_cell_view::printer>::format(const atomic_cell_view::printer& acvp,
                                                       fmt::format_context& ctx) const
    ->decltype(ctx.out()) {
    auto& type = acvp._type;
    auto& acv = acvp._cell;
    if (acv.is_live()) {
        std::ostringstream cell_value_string_builder;
        if (type.is_counter()) {
            if (acv.is_counter_update()) {
                fmt::print(cell_value_string_builder, "counter_update_value={}", acv.counter_update_value());
            } else {
                auto ccv = counter_cell_view(acv);
                fmt::print(cell_value_string_builder, "shards: {}", fmt::join(ccv.shards(), ", "));
            }
        } else {
            fmt::print(cell_value_string_builder, "{}", type.to_string(to_bytes(acv.value())));
        }
        return fmt::format_to(ctx.out(), "atomic_cell{{{},ts={:d},expiry={:d},ttl={:d}}}",
            cell_value_string_builder.str(),
            acv.timestamp(),
            acv.is_live_and_has_ttl() ? acv.expiry().time_since_epoch().count() : -1,
            acv.is_live_and_has_ttl() ? acv.ttl().count() : 0);
    } else {
        return fmt::format_to(ctx.out(), "atomic_cell{{DEAD,ts={:d},deletion_time={:d}}}",
            acv.timestamp(), acv.deletion_time().time_since_epoch().count());
    }
}

auto fmt::formatter<atomic_cell_or_collection::printer>::format(const atomic_cell_or_collection::printer& p, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto out = ctx.out();
    if (p._cell._data.empty()) {
        return fmt::format_to(out, "{{ null atomic_cell_or_collection }}");
    }
    out = fmt::format_to(out, "{{");
    if (p._cdef.type->is_multi_cell()) {
        out = fmt::format_to(out, "collection ");
        auto cmv = p._cell.as_collection_mutation();
        out = fmt::format_to(out, "{}", collection_mutation_view::printer(*p._cdef.type, cmv));
    } else {
        out = fmt::format_to(out, "{}", atomic_cell_view::printer(*p._cdef.type, p._cell.as_atomic_cell(p._cdef)));
    }
    return fmt::format_to(out, "}}");
}
